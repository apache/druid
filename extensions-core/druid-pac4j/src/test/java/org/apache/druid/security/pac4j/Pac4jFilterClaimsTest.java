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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.engine.DefaultSecurityLogic;
import org.pac4j.core.engine.SecurityGrantedAccessAdapter;
import org.pac4j.core.http.adapter.HttpActionAdapter;
import org.pac4j.core.profile.UserProfile;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class Pac4jFilterClaimsTest
{
  @Mock
  private Config pac4jConfig;

  @Mock
  private HttpServletRequest req;
  @Mock
  private HttpServletResponse resp;
  @Mock
  private FilterChain chain;
  @Mock
  private UserProfile profile;

  @Mock
  private DefaultSecurityLogic securityLogic;
  @Mock
  private DefaultCallbackLogic callbackLogic;

  @Mock
  private Supplier<DefaultSecurityLogic> securityLogicFactory;
  @Mock
  private Supplier<DefaultCallbackLogic> callbackLogicFactory;

  private Pac4jFilter filter;

  @Before
  public void setUp()
  {
    filter = new Pac4jFilter(
        "testPac4j",
        "basic",
        pac4jConfig,
        "/callback",
        "cookiePassphrase"
    );

    when(securityLogicFactory.get()).thenReturn(securityLogic);

    setField(filter, "securityLogicFactory", securityLogicFactory);
    setField(filter, "callbackLogicFactory", callbackLogicFactory);

    when(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(null);
    when(req.getRequestURI()).thenReturn("/some/api");
  }

  @Test
  public void skipWhenAuthResultAlreadyPresent() throws IOException, ServletException
  {
    when(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .thenReturn(new AuthenticationResult("id", "a", "n", Map.of()));

    filter.doFilter(req, resp, chain);

    verify(chain).doFilter(req, resp);
    verifyNoInteractions(securityLogicFactory, callbackLogicFactory, securityLogic, callbackLogic);
  }

  @Test
  public void setsRolesInContextWhenPresent() throws IOException, ServletException
  {
    when(profile.getId()).thenReturn("user1");
    when(profile.getRoles()).thenReturn(Set.of("admin", "dev"));

    doAnswer(inv -> {
      WebContext ctx = inv.getArgument(0);
      SessionStore store = inv.getArgument(1);
      SecurityGrantedAccessAdapter adapter = inv.getArgument(3);

      adapter.adapt(ctx, store, Set.of(profile), Collections.emptyMap());
      return null;
    }).when(securityLogic).perform(
        any(WebContext.class),
        any(SessionStore.class),
        eq(pac4jConfig),
        any(),
        any(HttpActionAdapter.class),
        isNull(),
        eq("none"),
        isNull()
    );

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);

    filter.doFilter(req, resp, chain);

    verify(req).setAttribute(eq(AuthConfig.DRUID_AUTHENTICATION_RESULT), captor.capture());
    verify(chain).doFilter(req, resp);

    AuthenticationResult ar = (AuthenticationResult) captor.getValue();
    assertEquals("user1", ar.getIdentity());
    assertNotNull(ar.getContext());
    assertTrue(ar.getContext().containsKey(Pac4jFilter.ROLE_CLAIM_CONTEXT_KEY));

    Set<String> roles = (Set<String>) ar.getContext().get(Pac4jFilter.ROLE_CLAIM_CONTEXT_KEY);
    assertEquals(Set.of("admin", "dev"), roles);
  }

  @Test
  public void noRolesDoesNotSetRolesKey() throws IOException, ServletException
  {
    when(profile.getId()).thenReturn("user2");
    when(profile.getRoles()).thenReturn(null);

    doAnswer(inv -> {
      WebContext ctx = inv.getArgument(0);
      SessionStore store = inv.getArgument(1);
      SecurityGrantedAccessAdapter adapter = inv.getArgument(3);

      adapter.adapt(ctx, store, Set.of(profile), Collections.emptyMap());
      return null;
    }).when(securityLogic).perform(
        any(WebContext.class),
        any(SessionStore.class),
        eq(pac4jConfig),
        any(),
        any(HttpActionAdapter.class),
        isNull(),
        eq("none"),
        isNull()
    );

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);

    filter.doFilter(req, resp, chain);

    verify(req).setAttribute(eq(AuthConfig.DRUID_AUTHENTICATION_RESULT), captor.capture());
    verify(chain).doFilter(req, resp);

    AuthenticationResult ar = (AuthenticationResult) captor.getValue();
    assertEquals("user2", ar.getIdentity());
    assertNotNull(ar.getContext());
    assertFalse(ar.getContext().containsKey(Pac4jFilter.ROLE_CLAIM_CONTEXT_KEY));
  }

  @Test
  public void emptyProfilesDoesNotSetAuthResultAndDoesNotContinueChain() throws IOException, ServletException
  {
    doAnswer(inv -> {
      WebContext ctx = inv.getArgument(0);
      SessionStore store = inv.getArgument(1);
      SecurityGrantedAccessAdapter adapter = inv.getArgument(3);

      adapter.adapt(ctx, store, Collections.emptySet(), Collections.emptyMap());
      return null;
    }).when(securityLogic).perform(
        any(WebContext.class),
        any(SessionStore.class),
        eq(pac4jConfig),
        any(),
        any(HttpActionAdapter.class),
        isNull(),
        eq("none"),
        isNull()
    );

    filter.doFilter(req, resp, chain);

    verify(req, never()).setAttribute(eq(AuthConfig.DRUID_AUTHENTICATION_RESULT), any());
    verify(chain, never()).doFilter(any(), any());
  }

  private static void setField(Object target, String name, Object value)
  {
    try {
      Field f = target.getClass().getDeclaredField(name);
      f.setAccessible(true);
      f.set(target, value);
    }
    catch (ReflectiveOperationException e) {
      throw new AssertionError("Failed setting field: " + name, e);
    }
  }
}