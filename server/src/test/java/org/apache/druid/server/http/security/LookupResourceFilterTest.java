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
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LookupResourceFilterTest
{
  @Test
  public void testBulkReadReturnsOnlyAuthorizedLookups()
  {
    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final Injector injector = mock(Injector.class);
    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    final ContainerRequest request = mock(ContainerRequest.class);
    final LookupCoordinatorManager coordinator = mock(LookupCoordinatorManager.class);
    final Map<String, Object> attributes = new HashMap<>();
    final List<PathSegment> segments = Arrays.stream("druid/coordinator/v1/lookups/config/tier".split("/"))
                                             .map(segment -> {
                                               final PathSegment pathSegment = mock(PathSegment.class);
                                               when(pathSegment.getPath()).thenReturn(segment);
                                               return pathSegment;
                                             })
                                             .collect(Collectors.toList());
    when(request.getPathSegments()).thenReturn(segments);
    when(request.getMethod()).thenReturn("GET");
    when(servletRequest.getMethod()).thenReturn("GET");
    when(servletRequest.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
    doAnswer(call -> {
      attributes.put(call.getArgument(0), call.getArgument(1));
      return null;
    }).when(servletRequest).setAttribute(anyString(), any());
    attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT,
                   new AuthenticationResult("user", "authorizer", null, null));
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) ->
        "tier/allowed".equals(resource.getName()) ? Access.OK : Access.DENIED);
    when(injector.getInstance(LookupCoordinatorManager.class)).thenReturn(coordinator);
    when(coordinator.getKnownLookups()).thenReturn(ImmutableMap.of(
        "tier", ImmutableMap.of(
            "allowed", mock(LookupExtractorFactoryMapContainer.class),
            "denied", mock(LookupExtractorFactoryMapContainer.class)
        )
    ));

    final LookupResourceFilter filter = new LookupResourceFilter(
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build(),
        injector
    );
    final UriInfo uriInfo = mock(UriInfo.class);
    final MultivaluedMapImpl pathParams = new MultivaluedMapImpl();
    pathParams.add("tier", "tier");
    when(uriInfo.getPathParameters()).thenReturn(pathParams);
    filter.uriInfo = uriInfo;
    filter.setReq(servletRequest);
    filter.filter(request);
    final Map<String, LookupExtractorFactoryMapContainer> readable = LookupResourceFilter.filterBulk(
        servletRequest,
        "tier",
        coordinator.getKnownLookups().get("tier")
    );

    assertEquals(ImmutableSet.of("allowed"), readable.keySet());
    verify(servletRequest).setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
  }

  @Test
  public void testBulkTierReadDoesNotModifyInput()
  {
    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    final ContainerRequest request = mock(ContainerRequest.class);
    final UriInfo uriInfo = mock(UriInfo.class);
    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT,
                   new AuthenticationResult("user", "authorizer", null, null));
    when(servletRequest.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
    doAnswer(call -> {
      attributes.put(call.getArgument(0), call.getArgument(1));
      return null;
    }).when(servletRequest).setAttribute(anyString(), any());
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) ->
        "tier/allowed".equals(resource.getName()) ? Access.OK : Access.DENIED);
    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedMapImpl());

    final LookupResourceFilter filter = new LookupResourceFilter(
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build(),
        mock(Injector.class)
    );
    filter.uriInfo = uriInfo;
    filter.setReq(servletRequest);
    filter.filter(request);

    final ImmutableSet<String> tiers = ImmutableSet.of("tier", "empty");
    final Set<String> filtered = LookupResourceFilter.filterBulk(
        servletRequest,
        tiers,
        ImmutableMap.of("tier", ImmutableMap.of("allowed", 1))
    );
    assertEquals(ImmutableSet.of("tier"), filtered);
    assertEquals(ImmutableSet.of("tier", "empty"), tiers);
  }

  @Test
  public void testReadableStateWithUnknownNodeState()
  {
    final LookupsState<Object> readable = LookupResourceFilter.readable(
        ImmutableSet.of(),
        "tier",
        new LookupsState<>(null, null, null)
    );
    assertEquals(ImmutableMap.of(), readable.getCurrent());
    assertEquals(ImmutableMap.of(), readable.getToLoad());
    assertEquals(ImmutableSet.of(), readable.getToDrop());
  }

  @Test
  public void testNodeStatusDoesNotExposeHostsWithoutReadableLookups()
  {
    final HostAndPort host = HostAndPort.fromString("example:8080");
    final Map<HostAndPort, LookupsState<Integer>> states = ImmutableMap.of(
        host,
        new LookupsState<>(ImmutableMap.of("denied", 1), ImmutableMap.of(), ImmutableSet.of())
    );

    assertEquals(ImmutableMap.of(), LookupResourceFilter.readable(ImmutableSet.of(), "tier", states));
  }

  @Test
  public void testNamedLookups()
  {
    assertLookupResources("druid/coordinator/v1/lookups/config/tier/name", "GET", null, "LOOKUP:tier/name:READ");
    assertLookupResources("druid/coordinator/v1/lookups/status/tier/name", "GET", null, "LOOKUP:tier/name:READ");
    assertLookupResources("druid/coordinator/v1/lookups/config/tier/name", "POST", null, "LOOKUP:tier/name:WRITE");
    assertLookupResources("druid/coordinator/v1/lookups/config/tier/name", "PUT", null, "LOOKUP:tier/name:WRITE");
    assertLookupResources("druid/coordinator/v1/lookups/config/tier/name", "DELETE", null, "LOOKUP:tier/name:WRITE");
    assertLookupResources("druid/v1/lookups/introspect/name/keys", "GET", null, "LOOKUP:tier/name:READ");
    assertLookupResources("druid/listen/v1/lookups/name", "GET", null, "LOOKUP:tier/name:READ");
    assertLookupResources("druid/listen/v1/lookups/introspect", "GET", null, "LOOKUP:tier/introspect:READ");
    assertLookupResources("druid/listen/v1/lookups/updates", "GET", null, "LOOKUP:tier/updates:READ");
    assertLookupResources("druid/listen/v1/lookups/name", "POST", null, "LOOKUP:tier/name:WRITE");
    assertLookupResources("druid/listen/v1/lookups/updates", "DELETE", null, "LOOKUP:tier/updates:WRITE");
  }

  @Test
  public void testLookupInTierNamedAllRequiresReadPermission()
  {
    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    final ContainerRequest request = mock(ContainerRequest.class);
    final Injector injector = mock(Injector.class);
    final List<PathSegment> segments = Arrays.stream("druid/coordinator/v1/lookups/config/all/name".split("/"))
                                             .map(segment -> {
                                               final PathSegment pathSegment = mock(PathSegment.class);
                                               when(pathSegment.getPath()).thenReturn(segment);
                                               return pathSegment;
                                             })
                                             .collect(Collectors.toList());
    when(request.getPathSegments()).thenReturn(segments);
    when(request.getMethod()).thenReturn("GET");
    when(servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .thenReturn(new AuthenticationResult("user", "authorizer", null, null));
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) -> Access.DENIED);

    final LookupResourceFilter filter = new LookupResourceFilter(
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build(),
        injector
    );
    final UriInfo uriInfo = mock(UriInfo.class);
    final MultivaluedMapImpl pathParams = new MultivaluedMapImpl();
    pathParams.add("tier", "all");
    pathParams.add("lookup", "name");
    when(uriInfo.getPathParameters()).thenReturn(pathParams);
    filter.uriInfo = uriInfo;
    filter.setReq(servletRequest);

    assertThrows(ForbiddenException.class, () -> filter.filter(request));
  }

  @Test
  public void testBulkLookups()
  {
    assertBulkResources(null, ImmutableMap.of("tier", ImmutableMap.of("name", 1)), Action.WRITE,
                        "LOOKUP:tier/name:WRITE");
    assertBulkResources(null, ImmutableMap.of("empty", ImmutableMap.of()), Action.WRITE,
                        "LOOKUP:empty:WRITE");
    assertBulkResources(null, ImmutableMap.of(), Action.WRITE, "LOOKUP:LOOKUP:WRITE");
    assertBulkResources("tier", ImmutableMap.of("name", 1), Action.WRITE, "LOOKUP:tier/name:WRITE");
    assertBulkResources("tier", ImmutableMap.of(), Action.WRITE, "LOOKUP:tier:WRITE");
    assertBulkResources("tier", new LookupsState<>(
        ImmutableMap.of(), ImmutableMap.of("name", 1), ImmutableSet.of("other")
    ), Action.WRITE, "LOOKUP:tier/name:WRITE", "LOOKUP:tier/other:WRITE");
    assertBulkResources(null, ImmutableMap.of("tier", ImmutableMap.of(
        HostAndPort.fromString("example:8080"),
        new LookupsState<>(ImmutableMap.of("name", 1), ImmutableMap.of(), ImmutableSet.of())
    )), Action.READ, "LOOKUP:tier/name:READ");
  }

  @Test
  public void testDisabledUsesConfig()
  {
    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    final ContainerRequest request = mock(ContainerRequest.class);
    final List<String> checked = new ArrayList<>();
    when(servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .thenReturn(new AuthenticationResult("user", "authorizer", null, null));
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) -> {
      checked.add(resource.getType() + ":" + resource.getName() + ":" + action);
      return Access.OK;
    });
    when(request.getMethod()).thenReturn("GET");

    final LookupResourceFilter filter = new LookupResourceFilter(mapper, new AuthConfig(), mock(Injector.class));
    filter.setReq(servletRequest);
    filter.filter(request);

    assertEquals(Arrays.asList("CONFIG:CONFIG:READ"), checked);
    verify(servletRequest).setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
  }

  private static void assertLookupResources(String path, String method, String body, String... expected)
  {
    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final Injector injector = mock(Injector.class);
    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    final ContainerRequest request = mock(ContainerRequest.class);
    final LookupCoordinatorManager coordinator = mock(LookupCoordinatorManager.class);
    final List<String> checked = new ArrayList<>();
    final String route = path.split("\\?", 2)[0];
    final List<PathSegment> segments = Arrays.stream(route.split("/"))
                                             .map(segment -> {
                                               final PathSegment pathSegment = mock(PathSegment.class);
                                               when(pathSegment.getPath()).thenReturn(segment);
                                               return pathSegment;
                                             })
                                             .collect(Collectors.toList());
    when(request.getPathSegments()).thenReturn(segments);
    when(request.getMethod()).thenReturn(method);
    if (path.endsWith("?discover=true")) {
      final MultivaluedMapImpl queryParameters = new MultivaluedMapImpl();
      queryParameters.add("discover", "true");
      when(request.getQueryParameters()).thenReturn(queryParameters);
    }
    when(servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .thenReturn(new AuthenticationResult("user", "authorizer", null, null));
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) -> {
      checked.add(resource.getType() + ":" + resource.getName() + ":" + action);
      return Access.OK;
    });
    when(injector.getInstance(LookupNodeService.class)).thenReturn(new LookupNodeService("tier"));
    when(injector.getInstance(LookupCoordinatorManager.class)).thenReturn(coordinator);
    when(coordinator.getKnownLookups()).thenReturn(ImmutableMap.of(
        "tier", ImmutableMap.of("name", mock(LookupExtractorFactoryMapContainer.class)),
        "empty", ImmutableMap.of()
    ));
    final HostAndPort host = HostAndPort.fromString("example:8080");
    when(coordinator.discoverTiers()).thenReturn(ImmutableSet.of("tier"));
    when(coordinator.discoverNodesInTier("tier")).thenReturn(ImmutableList.of(host));
    when(coordinator.discoverNodesInTier("empty")).thenReturn(ImmutableList.of());
    when(coordinator.discoverNodesInTier("future")).thenReturn(ImmutableList.of());
    when(coordinator.getLastKnownLookupsStateOnNodes()).thenReturn(ImmutableMap.of(
        host,
        new LookupsState<>(
            ImmutableMap.of("name", mock(LookupExtractorFactoryMapContainer.class)),
            ImmutableMap.of(),
            ImmutableSet.of()
        )
    ));
    if (body != null) {
      when(request.getEntityInputStream()).thenReturn(
          new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8))
      );
      when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(new ObjectMapper());
    }

    final LookupResourceFilter filter = new LookupResourceFilter(
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build(),
        injector
    );
    final UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPathParameters()).thenReturn(matchedPathParams(route, method));
    filter.uriInfo = uriInfo;
    filter.setReq(servletRequest);
    filter.filter(request);

    assertEquals(Arrays.asList(expected), checked);
    verify(servletRequest).setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
  }

  private static MultivaluedMapImpl matchedPathParams(String route, String method)
  {
    final MultivaluedMapImpl params = new MultivaluedMapImpl();
    final String[] segments = route.split("/");
    if (route.startsWith("druid/coordinator/v1/lookups/")) {
      if (segments.length > 5
          && !(segments.length == 6 && "config".equals(segments[4]) && "all".equals(segments[5]))) {
        params.add("tier", segments[5]);
      }
      if (segments.length > 6 && !"nodeStatus".equals(segments[4])) {
        params.add("lookup", segments[6]);
      } else if (segments.length > 6) {
        params.add("hostAndPort", segments[6]);
      }
    } else if (route.startsWith("druid/v1/lookups/introspect/")) {
      params.add("lookupId", segments[4]);
    } else if (segments.length > 4 && !("POST".equals(method) && "updates".equals(segments[4]))) {
      params.add("id", segments[4]);
    }
    return params;
  }

  private static void assertBulkResources(String tier, Object value, Action action, String... expected)
  {
    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    final ContainerRequest request = mock(ContainerRequest.class);
    final Injector injector = mock(Injector.class);
    final UriInfo uriInfo = mock(UriInfo.class);
    final Map<String, Object> attributes = new HashMap<>();
    final List<String> checked = new ArrayList<>();
    final String method = action == Action.READ ? "GET" : "POST";
    when(request.getMethod()).thenReturn(method);
    when(servletRequest.getMethod()).thenReturn(method);
    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedMapImpl());
    when(servletRequest.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
    doAnswer(call -> {
      attributes.put(call.getArgument(0), call.getArgument(1));
      return null;
    }).when(servletRequest).setAttribute(anyString(), any());
    attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT,
                   new AuthenticationResult("user", "authorizer", null, null));
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, checkedAction) -> {
      checked.add(resource.getType() + ":" + resource.getName() + ":" + checkedAction);
      return Access.OK;
    });
    when(injector.getInstance(LookupNodeService.class)).thenReturn(new LookupNodeService("tier"));

    final LookupResourceFilter filter = new LookupResourceFilter(
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build(),
        injector
    );
    filter.uriInfo = uriInfo;
    filter.setReq(servletRequest);
    filter.filter(request);
    LookupResourceFilter.filterBulk(servletRequest, tier, value);

    assertEquals(Arrays.asList(expected), checked);
    verify(servletRequest).setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
  }
}
