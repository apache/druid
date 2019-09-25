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

package org.apache.druid.server.security;

import org.easymock.EasyMock;
import org.junit.Test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class TrustedDomainAuthenticatorTest
{

  @Test
  public void testTrustedHost() throws IOException, ServletException
  {
    Authenticator authenticator = new TrustedDomainAuthenticator(
        "test-authenticator",
        "test.com",
        false,
        "my-auth",
        "myUser"
    );
    HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getRemoteAddr()).andReturn("serverA.test.com");
    req.setAttribute(
        AuthConfig.DRUID_AUTHENTICATION_RESULT,
        new AuthenticationResult("myUser", "my-auth", "test-authenticator", null)
    );
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(req);

    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    EasyMock.replay(resp);

    FilterChain filterChain = EasyMock.createMock(FilterChain.class);
    filterChain.doFilter(req, resp);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(filterChain);

    Filter authenticatorFilter = authenticator.getFilter();
    authenticatorFilter.doFilter(req, resp, filterChain);

    EasyMock.verify(req, resp, filterChain);
  }

  @Test
  public void testNonTrustedHost() throws IOException, ServletException
  {
    Authenticator authenticator = new TrustedDomainAuthenticator(
        "test-authenticator",
        "test.com",
        false,
        "my-auth",
        "myUser"
    );
    HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getRemoteAddr()).andReturn("serverA.test2.com");
    EasyMock.replay(req);

    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    EasyMock.replay(resp);

    FilterChain filterChain = EasyMock.createMock(FilterChain.class);
    filterChain.doFilter(req, resp);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(filterChain);

    Filter authenticatorFilter = authenticator.getFilter();
    authenticatorFilter.doFilter(req, resp, filterChain);

    EasyMock.verify(req, resp, filterChain);
  }

  @Test
  public void testForwardedForTrustedHost() throws IOException, ServletException
  {
    Authenticator authenticator = new TrustedDomainAuthenticator(
        "test-authenticator",
        "test.com",
        true,
        "my-auth",
        "myUser"
    );
    HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getRemoteAddr()).andReturn("serverA.test2.com");
    EasyMock.expect(req.getHeader("X-Forwarded-For")).andReturn("serverA.test.com");
    req.setAttribute(
        AuthConfig.DRUID_AUTHENTICATION_RESULT,
        new AuthenticationResult("myUser", "my-auth", "test-authenticator", null)
    );
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(req);

    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    EasyMock.replay(resp);

    FilterChain filterChain = EasyMock.createMock(FilterChain.class);
    filterChain.doFilter(req, resp);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(filterChain);

    Filter authenticatorFilter = authenticator.getFilter();
    authenticatorFilter.doFilter(req, resp, filterChain);

    EasyMock.verify(req, resp, filterChain);
  }

  @Test
  public void testForwardedForTrustedHostMultiProxy() throws IOException, ServletException
  {
    Authenticator authenticator = new TrustedDomainAuthenticator(
        "test-authenticator",
        "test.com",
        true,
        "my-auth",
        "myUser"
    );
    HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getRemoteAddr()).andReturn("serverA.test2.com");
    EasyMock.expect(req.getHeader("X-Forwarded-For")).andReturn("serverA.test.com,proxy-1,proxy-2");
    req.setAttribute(
        AuthConfig.DRUID_AUTHENTICATION_RESULT,
        new AuthenticationResult("myUser", "my-auth", "test-authenticator", null)
    );
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(req);

    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    EasyMock.replay(resp);

    FilterChain filterChain = EasyMock.createMock(FilterChain.class);
    filterChain.doFilter(req, resp);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(filterChain);

    Filter authenticatorFilter = authenticator.getFilter();
    authenticatorFilter.doFilter(req, resp, filterChain);

    EasyMock.verify(req, resp, filterChain);
  }

}
