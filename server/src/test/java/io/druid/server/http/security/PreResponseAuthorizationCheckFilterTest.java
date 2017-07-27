/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.http.security;

import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.Authenticator;
import io.druid.server.security.NoopAuthenticator;
import io.druid.server.security.PreResponseAuthorizationCheckFilter;
import org.easymock.EasyMock;
import org.junit.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

public class PreResponseAuthorizationCheckFilterTest
{
  private static List<Authenticator> authenticators = Lists.newArrayList(new NoopAuthenticator());
  private static AuthConfig authConfig = new AuthConfig(true, null, null, null);

  @Test
  public void testValidRequest() throws Exception
  {
    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    HttpServletResponse resp = EasyMock.createStrictMock(HttpServletResponse.class);
    FilterChain filterChain = EasyMock.createNiceMock(FilterChain.class);
    ServletOutputStream outputStream = EasyMock.createNiceMock(ServletOutputStream.class);

    EasyMock.expect(resp.getOutputStream()).andReturn(outputStream).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN)).andReturn("so-very-valid").once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED)).andReturn(true).once();
    EasyMock.replay(req, resp, filterChain, outputStream);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authConfig,
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, filterChain);
    EasyMock.verify(req, resp, filterChain, outputStream);
  }

  @Test
  public void testAuthenticationFailedRequest() throws Exception
  {
      HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
      HttpServletResponse resp = EasyMock.createStrictMock(HttpServletResponse.class);
      FilterChain filterChain = EasyMock.createNiceMock(FilterChain.class);
      ServletOutputStream outputStream = EasyMock.createNiceMock(ServletOutputStream.class);

      EasyMock.expect(resp.getOutputStream()).andReturn(outputStream).once();
      EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN)).andReturn(null).once();
      resp.addHeader("WWW-Authenticate", "noop");
      EasyMock.expectLastCall().once();
      resp.setStatus(401);
      EasyMock.expectLastCall().once();
      resp.setContentType("application/json");
      EasyMock.expectLastCall().once();
      resp.setCharacterEncoding("UTF-8");
      EasyMock.expectLastCall().once();
      EasyMock.replay(req, resp, filterChain, outputStream);

      PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
          authConfig,
          authenticators,
          new DefaultObjectMapper()
      );
      filter.doFilter(req, resp, filterChain);
      EasyMock.verify(req, resp, filterChain, outputStream);
  }

  @Test
  public void testMissingAuthorizationCheck() throws Exception
  {

    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    HttpServletResponse resp = EasyMock.createStrictMock(HttpServletResponse.class);
    FilterChain filterChain = EasyMock.createNiceMock(FilterChain.class);
    ServletOutputStream outputStream = EasyMock.createNiceMock(ServletOutputStream.class);

    EasyMock.expect(resp.getOutputStream()).andReturn(outputStream).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN)).andReturn("so-very-valid").once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED)).andReturn(null).once();
    EasyMock.expect(resp.getStatus()).andReturn(200).once();
    EasyMock.expect(req.getRequestURI()).andReturn("uri").once();
    resp.setStatus(403);
    EasyMock.expectLastCall().once();
    resp.setContentType("application/json");
    EasyMock.expectLastCall().once();
    resp.setCharacterEncoding("UTF-8");
    EasyMock.expectLastCall().once();
    EasyMock.replay(req, resp, filterChain, outputStream);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authConfig,
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, filterChain);
    EasyMock.verify(req, resp, filterChain, outputStream);
  }
}
