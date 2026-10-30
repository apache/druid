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

import com.google.common.collect.ImmutableList;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LookupResourceFilterTest
{
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
