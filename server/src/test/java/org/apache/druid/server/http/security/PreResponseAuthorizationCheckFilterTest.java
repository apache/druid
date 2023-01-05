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

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.PreResponseAuthorizationCheckFilter;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class PreResponseAuthorizationCheckFilterTest
{
  private static final List<Authenticator> authenticators = Collections.singletonList(new AllowAllAuthenticator());

  @Test
  public void testValidRequest() throws Exception
  {
    AuthenticationResult authenticationResult = new AuthenticationResult("so-very-valid", "so-very-valid", null, null);

    MockHttpServletRequest req = new MockHttpServletRequest();
    MockHttpServletResponse resp = new MockHttpServletResponse();

    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
    req.attributes.put(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, (request, response) -> {
    });
  }

  @Test
  public void testAuthenticationFailedRequest() throws Exception
  {
    MockHttpServletRequest req = new MockHttpServletRequest();
    MockHttpServletResponse resp = new MockHttpServletResponse();

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, (request, response) -> {
    });

    Assert.assertEquals(401, resp.getStatus());
    Assert.assertEquals("application/json", resp.getContentType());
    Assert.assertEquals("UTF-8", resp.getCharacterEncoding());
  }

  @Test
  public void testMissingAuthorizationCheckAndNotCommitted() throws ServletException, IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    AuthenticationResult authenticationResult = new AuthenticationResult("so-very-valid", "so-very-valid", null, null);

    MockHttpServletRequest req = new MockHttpServletRequest();
    req.requestUri = "uri";
    req.method = "GET";
    req.remoteAddr = "1.2.3.4";
    req.remoteHost = "aHost";

    MockHttpServletResponse resp = new MockHttpServletResponse();
    resp.setStatus(200);

    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, (request, response) -> {
    });

    Assert.assertEquals(403, resp.getStatus());
  }

  @Test
  public void testMissingAuthorizationCheckWithForbidden() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    AuthenticationResult authenticationResult = new AuthenticationResult("so-very-valid", "so-very-valid", null, null);

    MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);

    MockHttpServletResponse resp = new MockHttpServletResponse();
    resp.setStatus(403);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, (request, response) -> {
    });

    Assert.assertEquals(403, resp.getStatus());
  }

  @Test
  public void testMissingAuthorizationCheckWith404Keeps404() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    AuthenticationResult authenticationResult = new AuthenticationResult("so-very-valid", "so-very-valid", null, null);

    MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);

    MockHttpServletResponse resp = new MockHttpServletResponse();
    resp.setStatus(404);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, (request, response) -> {
    });

    Assert.assertEquals(404, resp.getStatus());
  }

  @Test
  public void testMissingAuthorizationCheckWith307Keeps307() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    AuthenticationResult authenticationResult = new AuthenticationResult("so-very-valid", "so-very-valid", null, null);

    MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);

    MockHttpServletResponse resp = new MockHttpServletResponse();
    resp.setStatus(307);

    PreResponseAuthorizationCheckFilter filter = new PreResponseAuthorizationCheckFilter(
        authenticators,
        new DefaultObjectMapper()
    );
    filter.doFilter(req, resp, (request, response) -> {
    });

    Assert.assertEquals(307, resp.getStatus());
  }
}
