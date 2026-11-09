/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.http.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.http.LookupCoordinatorResource;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LookupResourceFilterTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final LookupExtractorFactoryMapContainer SPEC = new LookupExtractorFactoryMapContainer(
      "v1", ImmutableMap.of("type", "map", "map", ImmutableMap.of("key", "value"))
  );
  private static final Map<String, Map<String, LookupExtractorFactoryMapContainer>> LOOKUPS = ImmutableMap.of(
      "prod", ImmutableMap.of("orders", SPEC, "users", SPEC),
      "dev", ImmutableMap.of("orders", SPEC),
      "empty", ImmutableMap.of()
  );

  static Stream<Arguments> authorizationCases()
  {
    return Arrays.stream(Endpoint.values()).flatMap(endpoint ->
        Arrays.stream(Grants.values()).flatMap(grants ->
            Stream.of(true, false).map(enabled -> Arguments.of(endpoint, grants, enabled))));
  }

  /**
   * Exercises the path filter and resource method together for each supported configuration route.
   * Stateful request attributes detect accidental double authorization as well as missing checks.
   */
  @ParameterizedTest(name = "{0}, {1}, lookup authorization={2}")
  @MethodSource("authorizationCases")
  public void testConfigurationAuthorization(final Endpoint endpoint, final Grants grants, final boolean enabled) throws IOException
  {
    final HttpServletRequest request = mock(HttpServletRequest.class);
    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, new AuthenticationResult("user", "auth", null, null));
    when(request.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
    doAnswer(call -> {
      attributes.put(call.getArgument(0), call.getArgument(1));
      return null;
    }).when(request).setAttribute(anyString(), any());
    when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON);

    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final List<String> checkedTypes = new ArrayList<>();
    when(mapper.getAuthorizer("auth")).thenReturn((authentication, resource, action) -> {
      checkedTypes.add(resource.getType());
      return grants.allows(resource.getType(), resource.getName(), action) ? Access.OK : Access.DENIED;
    });
    final AuthConfig config = AuthConfig.newBuilder().setEnableLookupAuthorization(enabled).build();
    final AtomicInteger mutations = new AtomicInteger();
    final LookupCoordinatorManager manager = manager(mutations);
    final LookupCoordinatorResource resource = new LookupCoordinatorResource(manager, MAPPER, MAPPER, mapper, config);

    final LookupResourceFilter filter = new LookupResourceFilter(mapper, config);
    filter.setReq(request);
    final MultivaluedMapImpl params = new MultivaluedMapImpl();
    if (endpoint.tier != null) {
      params.add("tier", endpoint.tier);
    }
    if (endpoint.lookup != null) {
      params.add("lookup", endpoint.lookup);
    }
    final UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPathParameters()).thenReturn(params);
    filter.uriInfo = uriInfo;
    final ContainerRequest containerRequest = mock(ContainerRequest.class);
    when(containerRequest.getMethod()).thenReturn(endpoint.method);

    final Response response;
    try {
      filter.filter(containerRequest);
      response = invoke(endpoint, resource, request);
    }
    catch (ForbiddenException e) {
      assertEquals(0, mutations.get());
      assertTrue(isDenied(endpoint, grants, enabled), "Unexpected filter denial");
      assertEquals(false, attributes.get(AuthConfig.DRUID_AUTHORIZATION_CHECKED));
      return;
    }

    final boolean denied = isDenied(endpoint, grants, enabled);
    assertEquals(denied ? 403 : endpoint.action() == Action.READ ? 200 : 202, response.getStatus());
    assertEquals(!denied, attributes.get(AuthConfig.DRUID_AUTHORIZATION_CHECKED));
    assertEquals(!denied && endpoint.action() == Action.WRITE ? 1 : 0, mutations.get());
    assertTrue(!checkedTypes.isEmpty(), "Every route must perform an authorization check");
    assertEquals(ImmutableSet.of(enabled ? ResourceType.LOOKUP : ResourceType.CONFIG), ImmutableSet.copyOf(checkedTypes));
    if (!denied && endpoint.action() == Action.READ) {
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> visible = enabled
                                                                                 ? grants.readableLookups()
                                                                                 : LOOKUPS;
      final Map<String, LookupExtractorFactoryMapContainer> tier = visible.getOrDefault("prod", ImmutableMap.of());
      final Object expected = switch (endpoint) {
        case GET_TIERS, GET_DISCOVERED_TIERS -> visible.keySet();
        case GET_ALL -> visible;
        case GET_TIER -> tier.keySet();
        case GET_TIER_DETAILED -> tier;
        case GET_EMPTY_TIER -> ImmutableMap.of();
        case GET_LOOKUP, GET_OTHER_LOOKUP -> SPEC;
        default -> throw new AssertionError(endpoint);
      };
      assertEquals(expected, response.getEntity());
    }
  }

  private static boolean isDenied(final Endpoint endpoint, final Grants grants, final boolean enabled)
  {
    if (!enabled) {
      return !grants.allows(ResourceType.CONFIG, "CONFIG", endpoint.action());
    }
    // Bulk reads filter their response instead of rejecting requests without READ grants.
    if (endpoint.action() == Action.READ && endpoint.lookup == null) {
      return false;
    }
    if (endpoint.lookup != null) {
      return !grants.allows(ResourceType.LOOKUP, "prod/" + endpoint.lookup, endpoint.action());
    }
    if (endpoint == Endpoint.DELETE_EMPTY_TIER || endpoint == Endpoint.POST_EMPTY) {
      return !grants.allows(ResourceType.LOOKUP, endpoint == Endpoint.POST_EMPTY ? "LOOKUP" : "empty", Action.WRITE);
    }
    // Both the coordinator-wide update and tier deletion affect orders and users.
    return !grants.allows(ResourceType.LOOKUP, "prod/orders", Action.WRITE)
           || !grants.allows(ResourceType.LOOKUP, "prod/users", Action.WRITE);
  }

  private static LookupCoordinatorManager manager(final AtomicInteger mutations)
  {
    final LookupCoordinatorManager manager = mock(LookupCoordinatorManager.class);
    when(manager.getKnownLookups()).thenReturn(LOOKUPS);
    when(manager.discoverTiers()).thenReturn(LOOKUPS.keySet());
    when(manager.getLookup(anyString(), anyString())).thenReturn(SPEC);
    when(manager.updateLookups(any(), any())).thenAnswer(call -> {
      mutations.incrementAndGet();
      return true;
    });
    when(manager.updateLookup(anyString(), anyString(), any(), any())).thenAnswer(call -> {
      mutations.incrementAndGet();
      return true;
    });
    when(manager.deleteLookup(anyString(), anyString(), any())).thenAnswer(call -> {
      mutations.incrementAndGet();
      return true;
    });
    when(manager.deleteTier(anyString(), any(), any())).thenAnswer(call -> {
      final Consumer<Map<String, LookupExtractorFactoryMapContainer>> checkLookups = call.getArgument(2);
      checkLookups.accept(LOOKUPS.get(call.getArgument(0)));
      mutations.incrementAndGet();
      return true;
    });
    return manager;
  }

  private static Response invoke(
      final Endpoint endpoint,
      final LookupCoordinatorResource resource,
      final HttpServletRequest request
  ) throws IOException
  {
    return switch (endpoint) {
      case GET_TIERS -> resource.getTiers(false, request);
      case GET_DISCOVERED_TIERS -> resource.getTiers(true, request);
      case GET_ALL -> resource.getAllLookupSpecs(request);
      case GET_TIER -> resource.getSpecificTier("prod", false, request);
      case GET_TIER_DETAILED -> resource.getSpecificTier("prod", true, request);
      case GET_EMPTY_TIER -> resource.getSpecificTier("empty", true, request);
      case GET_LOOKUP, GET_OTHER_LOOKUP -> resource.getSpecificLookup("prod", endpoint.lookup);
      case POST_ALL -> resource.updateAllLookups(body(ImmutableMap.of("prod", LOOKUPS.get("prod"))), request);
      case POST_EMPTY -> resource.updateAllLookups(body(ImmutableMap.of()), request);
      case POST_LOOKUP, POST_OTHER_LOOKUP -> resource.createOrUpdateLookup("prod", endpoint.lookup, body(SPEC), request);
      case DELETE_TIER -> resource.deleteTier("prod", request);
      case DELETE_EMPTY_TIER -> resource.deleteTier("empty", request);
      case DELETE_LOOKUP, DELETE_OTHER_LOOKUP -> resource.deleteLookup("prod", endpoint.lookup, request);
    };
  }

  private static ByteArrayInputStream body(final Object value) throws IOException
  {
    return new ByteArrayInputStream(MAPPER.writeValueAsBytes(value));
  }

  private enum Endpoint
  {
    GET_TIERS("GET", null, null),
    GET_DISCOVERED_TIERS("GET", null, null),
    GET_ALL("GET", null, null),
    GET_TIER("GET", "prod", null),
    GET_TIER_DETAILED("GET", "prod", null),
    GET_EMPTY_TIER("GET", "empty", null),
    GET_LOOKUP("GET", "prod", "orders"),
    GET_OTHER_LOOKUP("GET", "prod", "users"),
    POST_ALL("POST", null, null),
    POST_EMPTY("POST", null, null),
    POST_LOOKUP("POST", "prod", "orders"),
    POST_OTHER_LOOKUP("POST", "prod", "users"),
    DELETE_TIER("DELETE", "prod", null),
    DELETE_EMPTY_TIER("DELETE", "empty", null),
    DELETE_LOOKUP("DELETE", "prod", "orders"),
    DELETE_OTHER_LOOKUP("DELETE", "prod", "users");

    private final String method;
    private final String tier;
    private final String lookup;

    Endpoint(final String method, final String tier, final String lookup)
    {
      this.method = method;
      this.tier = tier;
      this.lookup = lookup;
    }

    private Action action()
    {
      return "GET".equals(method) ? Action.READ : Action.WRITE;
    }
  }

  private enum Grants
  {
    ALL, NONE, PARTIAL, READ_ONLY, WRITE_ONLY, CONFIG_ONLY;

    private boolean allows(final String type, final String name, final Action action)
    {
      return switch (this) {
        case ALL -> true;
        case NONE -> false;
        case PARTIAL -> ResourceType.LOOKUP.equals(type) && "prod/orders".equals(name);
        case READ_ONLY -> action == Action.READ;
        case WRITE_ONLY -> action == Action.WRITE;
        case CONFIG_ONLY -> ResourceType.CONFIG.equals(type);
      };
    }

    private Map<String, Map<String, LookupExtractorFactoryMapContainer>> readableLookups()
    {
      return switch (this) {
        case ALL, READ_ONLY -> LOOKUPS;
        case PARTIAL -> ImmutableMap.of("prod", ImmutableMap.of("orders", SPEC));
        default -> ImmutableMap.of();
      };
    }
  }

  @Test
  public void testSingleRoutes()
  {
    assertRoute("tier", "lookup", "name", "GET", "LOOKUP:tier/name:READ");
    assertRoute("tier", "lookup", "name", "HEAD", "LOOKUP:tier/name:READ");
    assertRoute("tier", "lookup", "name", "POST", "LOOKUP:tier/name:WRITE");
    assertRoute(null, "lookupId", "name", "GET", "LOOKUP:node/name:READ");
    assertRoute(null, "id", "name", "DELETE", "LOOKUP:node/name:WRITE");
    assertRoute("a/x", "lookup", "y", "GET", "LOOKUP:a%2Fx/y:READ");
    assertRoute("a", "lookup", "x/y", "GET", "LOOKUP:a/x%2Fy:READ");
    assertRoute("a%", "lookup", "x", "GET", "LOOKUP:a%25/x:READ");
  }

  @Test
  public void testBulkRouteIsAuthorizedByResourceMethod()
  {
    final Fixture fixture = new Fixture(true, true);
    fixture.filter(null, null, null).filter(fixture.request("GET"));
    assertEquals(ImmutableList.of(), fixture.checked);
  }

  @Test
  public void testDeniedSingleRoute()
  {
    final Fixture fixture = new Fixture(true, false);
    assertThrows(
        ForbiddenException.class,
        () -> fixture.filter("tier", "lookup", "name").filter(fixture.request("GET"))
    );
  }

  @Test
  public void testDisabledUsesConfig()
  {
    final Fixture fixture = new Fixture(false, true);
    fixture.filter(null, null, null).filter(fixture.request("GET"));
    assertEquals(ImmutableList.of("CONFIG:CONFIG:READ"), fixture.checked);
    verify(fixture.servletRequest).setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
  }

  private static void assertRoute(String tier, String param, String lookup, String method, String expected)
  {
    final Fixture fixture = new Fixture(true, true);
    fixture.filter(tier, param, lookup).filter(fixture.request(method));
    assertEquals(ImmutableList.of(expected), fixture.checked);
  }

  private static class Fixture
  {
    private final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    private final LookupNodeService lookupNodeService = new LookupNodeService("node");
    private final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    private final List<String> checked = new ArrayList<>();
    private final boolean enabled;

    private Fixture(boolean enabled, boolean allowed)
    {
      this.enabled = enabled;
      when(servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
          .thenReturn(new AuthenticationResult("user", "authorizer", null, null));
      when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) -> {
        checked.add(resource.getType() + ":" + resource.getName() + ":" + action);
        return allowed ? Access.OK : Access.DENIED;
      });
    }

    private LookupResourceFilter filter(String tier, String param, String lookup)
    {
      final LookupResourceFilter filter = new LookupResourceFilter(
          mapper,
          AuthConfig.newBuilder().setEnableLookupAuthorization(enabled).build()
      );
      filter.lookupNodeService = lookupNodeService;
      final UriInfo uriInfo = mock(UriInfo.class);
      final MultivaluedMapImpl params = new MultivaluedMapImpl();
      if (tier != null) {
        params.add("tier", tier);
      }
      if (param != null) {
        params.add(param, lookup);
      }
      when(uriInfo.getPathParameters()).thenReturn(params);
      filter.uriInfo = uriInfo;
      filter.setReq(servletRequest);
      return filter;
    }

    private ContainerRequest request(String method)
    {
      final ContainerRequest request = mock(ContainerRequest.class);
      when(request.getMethod()).thenReturn(method);
      return request;
    }
  }
}
