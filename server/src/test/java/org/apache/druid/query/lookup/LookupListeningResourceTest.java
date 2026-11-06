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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LookupListeningResourceTest
{
  @Test
  public void testUpdateResponseFiltersUnreadableLookups()
  {
    final Fixture fixture = new Fixture(true);
    final LookupReferencesManager manager = mock(LookupReferencesManager.class);
    final LookupExtractorFactoryContainer allowed = mock(LookupExtractorFactoryContainer.class);
    final LookupExtractorFactoryContainer denied = mock(LookupExtractorFactoryContainer.class);
    when(manager.getAllLookupsState()).thenReturn(new LookupsState<>(
        ImmutableMap.of("allowed", allowed, "denied", denied),
        ImmutableMap.of(),
        ImmutableSet.of()
    ));
    when(fixture.request.getContentType()).thenReturn(MediaType.APPLICATION_JSON);
    final LookupListeningResource resource = fixture.resource(manager);

    final Response response = resource.serviceAnnouncementHandleUpdates(
        new ByteArrayInputStream(StringUtils.toUtf8("{\"toLoad\":{},\"toDrop\":[]}")),
        fixture.request
    );

    assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    final LookupsState<?> state = (LookupsState<?>) response.getEntity();
    assertEquals(ImmutableMap.of("allowed", allowed), state.getCurrent());
    assertEquals(ImmutableSet.of("tier:WRITE", "tier/allowed:READ", "tier/denied:READ"),
                 ImmutableSet.copyOf(fixture.checked));
  }

  @Test
  public void testUpdateDenialReachesForbiddenMapper()
  {
    final Fixture fixture = new Fixture();
    final LookupListeningResource resource = fixture.resource(mock(LookupReferencesManager.class));
    when(fixture.request.getContentType()).thenReturn(MediaType.APPLICATION_JSON);

    assertThrows(
        ForbiddenException.class,
        () -> resource.serviceAnnouncementHandleUpdates(
            new ByteArrayInputStream(StringUtils.toUtf8("{\"toLoad\":{},\"toDrop\":[]}")),
            fixture.request
        )
    );
    assertEquals(ImmutableSet.of("tier:WRITE"), ImmutableSet.copyOf(fixture.checked));
  }

  @Test
  public void testBulkUpdateChecksAllNames()
  {
    final Fixture fixture = new Fixture();
    assertThrows(
        ForbiddenException.class,
        () -> LookupListeningResource.authorizeChanges(
            fixture.request,
            "tier",
            ImmutableSet.of("allowed", "denied"),
            fixture.mapper,
            fixture.config
        )
    );
    assertEquals(ImmutableSet.of("tier/allowed:WRITE", "tier/denied:WRITE"), ImmutableSet.copyOf(fixture.checked));
  }

  @Test
  public void testBulkReadFiltersEveryPartOfState()
  {
    final Fixture fixture = new Fixture();
    final LookupsState<Integer> state = new LookupsState<>(
        ImmutableMap.of("allowed", 1, "denied", 2),
        ImmutableMap.of("denied", 3),
        ImmutableSet.of("allowed", "denied")
    );
    final LookupsState<Integer> filtered = LookupListeningResource.readableState(
        fixture.request,
        "tier",
        state,
        fixture.mapper,
        fixture.config,
        false
    );
    assertEquals(ImmutableMap.of("allowed", 1), filtered.getCurrent());
    assertEquals(ImmutableMap.of(), filtered.getToLoad());
    assertEquals(ImmutableSet.of("allowed"), filtered.getToDrop());
  }

  @Test
  public void testEmptyUpdateChecksTier()
  {
    final Fixture fixture = new Fixture();
    assertThrows(
        ForbiddenException.class,
        () -> LookupListeningResource.authorizeChanges(
            fixture.request,
            "tier",
            ImmutableSet.of(),
            fixture.mapper,
            fixture.config
        )
    );
    assertEquals(ImmutableSet.of("tier:WRITE"), ImmutableSet.copyOf(fixture.checked));
  }

  private static class Fixture
  {
    private final HttpServletRequest request = mock(HttpServletRequest.class);
    private final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    private final AuthConfig config = AuthConfig.newBuilder().setEnableLookupAuthorization(true).build();
    private final List<String> checked = new ArrayList<>();

    private Fixture()
    {
      this(false);
    }

    private Fixture(boolean allowTierWrite)
    {
      when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
          .thenReturn(new AuthenticationResult("user", "authorizer", null, null));
      when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) -> {
        checked.add(resource.getName() + ":" + action);
        return "tier/allowed".equals(resource.getName())
               || allowTierWrite && "tier".equals(resource.getName()) && action == Action.WRITE
               ? Access.OK : Access.DENIED;
      });
    }

    private LookupListeningResource resource(LookupReferencesManager manager)
    {
      final ObjectMapper jsonMapper = new DefaultObjectMapper();
      return new LookupListeningResource(
          jsonMapper,
          jsonMapper,
          manager,
          () -> request,
          mapper,
          config,
          new LookupNodeService("tier")
      );
    }
  }
}
