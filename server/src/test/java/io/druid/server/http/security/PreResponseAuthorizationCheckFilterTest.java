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
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.server.security.AllowAllAuthenticator;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authenticator;
import io.druid.server.security.PreResponseAuthorizationCheckFilter;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.FilterChain;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

public class PreResponseAuthorizationCheckFilterTest
{
  private static List<Authenticator> authenticators = Lists.newArrayList(new AllowAllAuthenticator());

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testValidRequest() throws Exception
  {
    AuthenticationResult authenticationResult = new AuthenticationResult("so-very-valid", "so-very-valid");

    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    HttpServletResponse resp = EasyMock.createStrictMock(HttpServletResponse.class);
    FilterChain filterChain = EasyMock.createNiceMock(FilterChain.class);
    ServletOutputStream outputStream = EasyMock.createNiceMock(ServletOutputStream.class);

    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(authenticationResult).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(true).once();
    EasyMock.replay(req, resp, filterChain, outputStream);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
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
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).once();
    resp.setStatus(401);
    EasyMock.expectLastCall().once();
    resp.setContentType("application/json");
    EasyMock.expectLastCall().once();
    resp.setCharacterEncoding("UTF-8");
    EasyMock.expectLastCall().once();
    EasyMock.replay(req, resp, filterChain, outputStream);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, filterChain);
    EasyMock.verify(req, resp, filterChain, outputStream);
  }

  @Test
  public void testMissingAuthorizationCheck() throws Exception
  {
    EmittingLogger.registerEmitter(EasyMock.createNiceMock(ServiceEmitter.class));

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Request did not have an authorization check performed.");

    AuthenticationResult authenticationResult = new AuthenticationResult("so-very-valid", "so-very-valid");

    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    HttpServletResponse resp = EasyMock.createStrictMock(HttpServletResponse.class);
    FilterChain filterChain = EasyMock.createNiceMock(FilterChain.class);
    ServletOutputStream outputStream = EasyMock.createNiceMock(ServletOutputStream.class);

    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(authenticationResult).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(resp.getStatus()).andReturn(200).once();
    EasyMock.expect(req.getRequestURI()).andReturn("uri").once();
    EasyMock.expect(req.getMethod()).andReturn("GET").once();
    EasyMock.expect(req.getRemoteAddr()).andReturn("1.2.3.4").once();
    EasyMock.expect(req.getRemoteHost()).andReturn("ahostname").once();
    EasyMock.expect(resp.isCommitted()).andReturn(true).once();
    resp.setStatus(403);
    EasyMock.expectLastCall().once();
    resp.setContentType("application/json");
    EasyMock.expectLastCall().once();
    resp.setCharacterEncoding("UTF-8");
    EasyMock.expectLastCall().once();
    EasyMock.replay(req, resp, filterChain, outputStream);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, filterChain);
    EasyMock.verify(req, resp, filterChain, outputStream);
  }

  @Test
  public void testMissingAuthorizationCheckWithError() throws Exception
  {
    EmittingLogger.registerEmitter(EasyMock.createNiceMock(ServiceEmitter.class));
    AuthenticationResult authenticationResult = new AuthenticationResult("so-very-valid", "so-very-valid");

    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    HttpServletResponse resp = EasyMock.createStrictMock(HttpServletResponse.class);
    FilterChain filterChain = EasyMock.createNiceMock(FilterChain.class);
    ServletOutputStream outputStream = EasyMock.createNiceMock(ServletOutputStream.class);

    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(authenticationResult).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(resp.getStatus()).andReturn(404).once();
    EasyMock.replay(req, resp, filterChain, outputStream);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, filterChain);
    EasyMock.verify(req, resp, filterChain, outputStream);
  }
}
