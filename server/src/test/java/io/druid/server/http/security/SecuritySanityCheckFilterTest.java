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

import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.SecuritySanityCheckFilter;
import org.easymock.EasyMock;
import org.junit.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SecuritySanityCheckFilterTest
{
  @Test
  public void testValidRequest() throws Exception
  {
    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    HttpServletResponse resp = EasyMock.createStrictMock(HttpServletResponse.class);
    FilterChain filterChain = EasyMock.createStrictMock(FilterChain.class);

    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).once();
    filterChain.doFilter(req, resp);
    EasyMock.expectLastCall().once();
    EasyMock.replay(req, filterChain);
    SecuritySanityCheckFilter filter = new SecuritySanityCheckFilter(new DefaultObjectMapper());
    filter.doFilter(req, resp, filterChain);
    EasyMock.verify(req, filterChain);
  }

  @Test
  public void testInvalidRequest() throws Exception
  {
    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    HttpServletResponse resp = EasyMock.createStrictMock(HttpServletResponse.class);
    FilterChain filterChain = EasyMock.createStrictMock(FilterChain.class);
    ServletOutputStream outputStream = EasyMock.createNiceMock(ServletOutputStream.class);

    AuthenticationResult authenticationResult = new AuthenticationResult("does-not-belong", "does-not-belong", null);

    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(true).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(authenticationResult).once();
    EasyMock.expect(resp.getOutputStream()).andReturn(outputStream).once();
    resp.setStatus(403);
    EasyMock.expectLastCall().once();
    resp.setContentType("application/json");
    EasyMock.expectLastCall().once();
    resp.setCharacterEncoding("UTF-8");
    EasyMock.expectLastCall().once();

    EasyMock.replay(req, resp, filterChain, outputStream);
    SecuritySanityCheckFilter filter = new SecuritySanityCheckFilter(new DefaultObjectMapper());
    filter.doFilter(req, resp, filterChain);
    EasyMock.verify(req, resp, filterChain, outputStream);
  }
}
