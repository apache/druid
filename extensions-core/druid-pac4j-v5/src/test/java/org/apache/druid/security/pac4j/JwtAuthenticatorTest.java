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

package org.apache.druid.security.pac4j;

import com.google.common.collect.ImmutableMap;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.Subject;
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.pac4j.oidc.profile.creator.TokenValidator;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;

public class JwtAuthenticatorTest
{
  private static final String DUMMY_BEARER_TOKEN_HEADER = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";

  @Test
  public void testBearerToken()
      throws IOException, ServletException
  {
    OIDCConfig configuration = EasyMock.createMock(OIDCConfig.class);
    TokenValidator tokenValidator = EasyMock.createMock(TokenValidator.class);

    HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null);
    EasyMock.expect(req.getHeader("Authorization")).andReturn("Nobearer");

    EasyMock.replay(req);

    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    EasyMock.replay(resp);

    FilterChain filterChain = EasyMock.createMock(FilterChain.class);
    filterChain.doFilter(req, resp);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(filterChain);


    JwtAuthenticator jwtAuthenticator = new JwtAuthenticator("jwt", "allowAll", configuration);
    JwtAuthFilter authFilter = new JwtAuthFilter("allowAll", "jwt", configuration, tokenValidator);
    authFilter.doFilter(req, resp, filterChain);

    EasyMock.verify(req, resp, filterChain);
    Assert.assertEquals(jwtAuthenticator.getFilterClass(), JwtAuthFilter.class);
    Assert.assertNull(jwtAuthenticator.getInitParameters());
    Assert.assertNull(jwtAuthenticator.authenticateJDBCContext(ImmutableMap.of()));
  }

  @Test
  public void testAuthenticatedRequest() throws ServletException, IOException
  {
    HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn("AlreadyAuthenticated");

    EasyMock.replay(req);

    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    EasyMock.replay(resp);

    FilterChain filterChain = EasyMock.createMock(FilterChain.class);
    filterChain.doFilter(req, resp);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(filterChain);

    JwtAuthFilter authFilter = new JwtAuthFilter("allowAll", "jwt", null, null);
    authFilter.doFilter(req, resp, filterChain);

    EasyMock.verify(req, resp, filterChain);
  }

  @Test
  public void testValidClaim()
      throws IOException, ServletException, BadJOSEException, JOSEException
  {
    HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null);
    EasyMock.expect(req.getHeader("Authorization")).andReturn(DUMMY_BEARER_TOKEN_HEADER);
    req.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, new AuthenticationResult("foo", "allowAll", "jwt", null));
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(req);

    OIDCConfig configuration = EasyMock.createMock(OIDCConfig.class);
    EasyMock.expect(configuration.getOidcClaim()).andReturn("iss");
    EasyMock.expect(configuration.getClientID()).andReturn("testClient");
    EasyMock.replay(configuration);

    TokenValidator tokenValidator = EasyMock.createMock(TokenValidator.class);
    EasyMock.expect(tokenValidator.validate(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new IDTokenClaimsSet(new Issuer("foo"),
                                            new Subject("testsub"),
                                            Collections.emptyList(),
                                            null,
                                            null
            ));
    EasyMock.replay(tokenValidator);

    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    EasyMock.replay(resp);

    FilterChain filterChain = EasyMock.createMock(FilterChain.class);
    filterChain.doFilter(req, resp);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(filterChain);


    JwtAuthenticator jwtAuthenticator = new JwtAuthenticator("jwt", "allowAll", configuration);
    JwtAuthFilter authFilter = new JwtAuthFilter("allowAll", "jwt", configuration, tokenValidator);
    authFilter.doFilter(req, resp, filterChain);

    EasyMock.verify(req, resp, filterChain);
    Assert.assertEquals(jwtAuthenticator.getFilterClass(), JwtAuthFilter.class);
    Assert.assertNull(jwtAuthenticator.getInitParameters());
    Assert.assertNull(jwtAuthenticator.authenticateJDBCContext(ImmutableMap.of()));
  }

  @Test
  public void testEmptyClaim()
      throws IOException, ServletException, BadJOSEException, JOSEException
  {
    OIDCConfig configuration = EasyMock.createMock(OIDCConfig.class);

    HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null);
    EasyMock.expect(req.getHeader("Authorization")).andReturn(DUMMY_BEARER_TOKEN_HEADER);

    EasyMock.replay(req);

    TokenValidator tokenValidator = EasyMock.createMock(TokenValidator.class);
    // This doesn't return any claims for the default scope
    EasyMock.expect(tokenValidator.validate(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new IDTokenClaimsSet(new Issuer("test"),
                                            new Subject("testsub"),
                                            Collections.emptyList(),
                                            null,
                                            null
            ));
    EasyMock.replay(tokenValidator);

    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    resp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(resp);

    FilterChain filterChain = EasyMock.createMock(FilterChain.class);
    EasyMock.replay(filterChain);


    JwtAuthenticator jwtAuthenticator = new JwtAuthenticator("jwt", "allowAll", configuration);
    JwtAuthFilter authFilter = new JwtAuthFilter("allowAll", "jwt", configuration, tokenValidator);
    authFilter.doFilter(req, resp, filterChain);

    EasyMock.verify(req, resp, filterChain);
    Assert.assertEquals(jwtAuthenticator.getFilterClass(), JwtAuthFilter.class);
    Assert.assertNull(jwtAuthenticator.getInitParameters());
    Assert.assertNull(jwtAuthenticator.authenticateJDBCContext(ImmutableMap.of()));
  }
}
