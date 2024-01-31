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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.JEEContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.engine.CallbackLogic;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.engine.DefaultSecurityLogic;
import org.pac4j.core.engine.SecurityLogic;
import org.pac4j.core.http.adapter.JEEHttpActionAdapter;
import org.pac4j.core.profile.UserProfile;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;

public class Pac4jFilter implements Filter
{
  private static final Logger LOGGER = new Logger(Pac4jFilter.class);

  private final Config pac4jConfig;
  private final SecurityLogic<Object, JEEContext> securityLogic;
  private final CallbackLogic<Object, JEEContext> callbackLogic;
  private final SessionStore<JEEContext> sessionStore;

  private final String name;
  private final String authorizerName;

  public Pac4jFilter(String name, String authorizerName, Config pac4jConfig, String cookiePassphrase)
  {
    this.pac4jConfig = pac4jConfig;
    this.securityLogic = new DefaultSecurityLogic<>();
    this.callbackLogic = new DefaultCallbackLogic<>();

    this.name = name;
    this.authorizerName = authorizerName;

    this.sessionStore = new Pac4jSessionStore<>(cookiePassphrase);
  }

  @Override
  public void init(FilterConfig filterConfig)
  {
  }


  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException
  {
    // If there's already an auth result, then we have authenticated already, skip this or else caller
    // could get HTTP redirect even if one of the druid authenticators in chain has successfully authenticated.
    if (servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT) != null) {
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    }

    HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
    HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
    JEEContext context = new JEEContext(httpServletRequest, httpServletResponse, sessionStore);

    if (Pac4jCallbackResource.SELF_URL.equals(httpServletRequest.getRequestURI())) {
      callbackLogic.perform(
          context,
          pac4jConfig,
          JEEHttpActionAdapter.INSTANCE,
          "/",
          true, false, false, null);
    } else {
      Object uid = securityLogic.perform(
          context,
          pac4jConfig,
          (JEEContext ctx, Collection<UserProfile> profiles, Object... parameters) -> {
            if (profiles.isEmpty()) {
              LOGGER.warn("No profiles found after OIDC auth.");
              return null;
            } else {
              return profiles.iterator().next().getId();
            }
          },
          JEEHttpActionAdapter.INSTANCE,
          null, "none", null, null);
      // Changed the Authorizer from null to "none".
      // In the older version, if it is null, it simply grant access and returns authorized.
      // But in the newer pac4j version, it uses CsrfAuthorizer as default, And because of this, It was returning 403 in API calls.
      if (uid != null) {
        AuthenticationResult authenticationResult = new AuthenticationResult(uid.toString(), authorizerName, name, null);
        servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
        filterChain.doFilter(servletRequest, servletResponse);
      }
    }
  }

  @Override
  public void destroy()
  {
  }
}
