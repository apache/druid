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

import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pac4j.core.config.Config;
import org.pac4j.core.engine.SecurityLogic;
import org.pac4j.core.exception.http.ForbiddenAction;
import org.pac4j.core.exception.http.FoundAction;
import org.pac4j.core.exception.http.HttpAction;
import org.pac4j.core.exception.http.WithLocationAction;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.jee.context.JEEContext;
import org.pac4j.jee.http.adapter.JEEHttpActionAdapter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
public class Pac4jFilterTest
{
  private static final String DRUID_AUTHENTICATION_RESULT = "Druid-Authentication-Result";

  @Mock
  private HttpServletRequest request;
  @Mock
  private HttpServletResponse response;
  @Mock
  private PrintWriter printWriter;
  @Mock
  private FilterChain filterChain;
  @Mock
  private HttpSession session;
  @Mock
  private Config pac4jConfig;

  private JEEContext context;
  private Pac4jFilter pac4jFilter;

  @BeforeEach
  public void setUp() throws IOException
  {
    // Mock the PrintWriter for the response
    lenient().when(response.getWriter()).thenReturn(printWriter);
    context = new JEEContext(request, response);
    pac4jFilter = new Pac4jFilter("test", "testAuthorizer", pac4jConfig, "/callback", "testPassphrase");
  }

  @Test
  public void testInit()
  {
    // Test that init method doesn't throw exceptions
    pac4jFilter.init(null);
    // No exception should be thrown
  }

  @Test
  public void testDestroy()
  {
    // Test that destroy method doesn't throw exceptions
    pac4jFilter.destroy();
    // No exception should be thrown
  }

  @Test
  public void testDoFilterWithAlreadyAuthenticatedRequest() throws IOException, ServletException
  {
    // Mock request with existing authentication result
    Mockito.when(request.getAttribute(DRUID_AUTHENTICATION_RESULT)).thenReturn("already-authenticated");
    
    // Call doFilter
    pac4jFilter.doFilter(request, response, filterChain);
    
    // Verify it continues the filter chain without doing authentication
    Mockito.verify(filterChain).doFilter(request, response);
  }

  @Test
  public void testDoFilterWithNullAuthenticationResult() throws IOException, ServletException
  {
    // Mock request without authentication result
    Mockito.when(request.getAttribute(DRUID_AUTHENTICATION_RESULT)).thenReturn(null);
    Mockito.when(request.getRequestURI()).thenReturn("/some/path");
    
    // This will attempt to do authentication, which may throw due to missing pac4j config
    // but we're mainly testing the basic flow
    try {
      pac4jFilter.doFilter(request, response, filterChain);
    }
    catch (Exception e) {
      // Expected due to mock limitations, but we verified the basic flow
    }
    
    // Verify that the authentication attribute was checked
    Mockito.verify(request).getAttribute(DRUID_AUTHENTICATION_RESULT);
    Mockito.verify(request).getRequestURI();
  }

  @Test
  public void testDoFilterWithCallbackPath() throws IOException, ServletException
  {
    // Mock request for callback path
    Mockito.when(request.getAttribute(DRUID_AUTHENTICATION_RESULT)).thenReturn(null);
    Mockito.when(request.getRequestURI()).thenReturn("/callback");
    Mockito.when(request.getSession()).thenReturn(session);
    Mockito.when(session.getAttribute("pac4j.originalUrl")).thenReturn("/original");
    
    // This will attempt to do callback logic
    try {
      pac4jFilter.doFilter(request, response, filterChain);
    }
    catch (Exception e) {
      // Expected due to mock limitations
    }
    
    // Verify that the callback path was detected
    Mockito.verify(request).getRequestURI();
    Mockito.verify(request).getSession();
  }

  @Test
  public void testDoFilterWithCallbackPathAndNoOriginalUrl() throws IOException, ServletException
  {
    // Mock request for callback path without original URL
    Mockito.when(request.getAttribute(DRUID_AUTHENTICATION_RESULT)).thenReturn(null);
    Mockito.when(request.getRequestURI()).thenReturn("/callback");
    Mockito.when(request.getSession()).thenReturn(session);
    Mockito.when(session.getAttribute("pac4j.originalUrl")).thenReturn(null);
    
    // This will attempt to do callback logic
    try {
      pac4jFilter.doFilter(request, response, filterChain);
    }
    catch (Exception e) {
      // Expected due to mock limitations
    }
    
    // Verify that the callback path was detected and session was accessed
    Mockito.verify(request).getRequestURI();
    Mockito.verify(session).getAttribute("pac4j.originalUrl");
  }

  @Test
  public void testActionAdapterForRedirection()
  {
    HttpAction httpAction = new FoundAction("testUrl");
    Mockito.doReturn(httpAction.getCode()).when(response).getStatus();
    Mockito.doReturn(((WithLocationAction) httpAction).getLocation()).when(response).getHeader(any());
    JEEHttpActionAdapter.INSTANCE.adapt(httpAction, context);
    Assertions.assertEquals(response.getStatus(), 302);
    Assertions.assertEquals(response.getHeader("Location"), "testUrl");
  }

  @Test
  public void testActionAdapterForForbidden()
  {
    HttpAction httpAction = ForbiddenAction.INSTANCE;
    Mockito.doReturn(httpAction.getCode()).when(response).getStatus();
    JEEHttpActionAdapter.INSTANCE.adapt(httpAction, context);
    Assertions.assertEquals(response.getStatus(), HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  public void testFilterCreation()
  {
    // Test that filter can be created without exceptions
    Pac4jFilter filter = new Pac4jFilter("testName", "testAuthorizer", pac4jConfig, "/test-callback", "testPassphrase");
    Assertions.assertNotNull(filter);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDoFilterSetsProfileAttributesInAuthenticationResult() throws IOException, ServletException
  {
    List<String> groups = Arrays.asList("group-a", "group-b");

    CommonProfile profile = new CommonProfile();
    profile.setId("test-user-id");
    profile.addAttribute("groups", groups);
    profile.addAttribute("email", "test@example.com");

    SecurityLogic stubbedSecurityLogic = (ctx, ss, config, adapter, actionAdapter, clients, authorizers, matchers, params) -> {
      try {
        return adapter.adapt(ctx, ss, Collections.singletonList(profile));
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    Pac4jFilter filter = new Pac4jFilter("test", "testAuthorizer", pac4jConfig, "/callback", "testPassphrase")
    {
      @Override
      protected SecurityLogic createSecurityLogic()
      {
        return stubbedSecurityLogic;
      }
    };

    Mockito.when(request.getAttribute(DRUID_AUTHENTICATION_RESULT)).thenReturn(null);
    Mockito.when(request.getRequestURI()).thenReturn("/druid/v2/datasources");

    filter.doFilter(request, response, filterChain);

    ArgumentCaptor<AuthenticationResult> captor = ArgumentCaptor.forClass(AuthenticationResult.class);
    Mockito.verify(request).setAttribute(Mockito.eq(AuthConfig.DRUID_AUTHENTICATION_RESULT), captor.capture());

    AuthenticationResult result = captor.getValue();
    Assertions.assertEquals("test-user-id", result.getIdentity());

    Map<String, Object> profileInContext = (Map<String, Object>) result.getContext().get("profile");
    Assertions.assertNotNull(profileInContext);
    Map<String, Object> attributes = (Map<String, Object>) profileInContext.get("attributes");
    Assertions.assertNotNull(attributes);
    Assertions.assertEquals(groups, attributes.get("groups"));
    Assertions.assertEquals("test@example.com", attributes.get("email"));

    Mockito.verify(filterChain).doFilter(request, response);
  }
}
