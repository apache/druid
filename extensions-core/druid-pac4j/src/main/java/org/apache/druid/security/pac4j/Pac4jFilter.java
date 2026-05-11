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
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.engine.DefaultSecurityLogic;
import org.pac4j.core.exception.http.HttpAction;
import org.pac4j.jee.context.JEEContext;
import org.pac4j.jee.http.adapter.JEEHttpActionAdapter;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class Pac4jFilter implements Filter
{
  private static final Logger LOGGER = new Logger(Pac4jFilter.class);

  private final Config pac4jConfig;
  private final Pac4jSessionStore sessionStore;
  private final String callbackPath;
  private final String name;
  private final String authorizerName;

  public Pac4jFilter(
      String name,
      String authorizerName,
      Config pac4jConfig,
      String callbackPath,
      String cookiePassphrase
  )
  {
    this.pac4jConfig = pac4jConfig;
    this.callbackPath = callbackPath;
    this.name = name;
    this.authorizerName = authorizerName;
    this.sessionStore = new Pac4jSessionStore(cookiePassphrase);
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

    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;
    JEEContext context = new JEEContext(request, response);

    if (request.getRequestURI().equals(callbackPath)) {
      DefaultCallbackLogic callbackLogic = new DefaultCallbackLogic();
      String originalUrl = (String) request.getSession().getAttribute("pac4j.originalUrl");
      String redirectUrl = originalUrl != null ? originalUrl : "/";

      callbackLogic.perform(
          context,
          sessionStore,
          pac4jConfig,
          JEEHttpActionAdapter.INSTANCE,
          redirectUrl,                      // Redirect to original URL or root
          null,
          null
      );
    } else {
      DefaultSecurityLogic securityLogic = new DefaultSecurityLogic();
      try {
        securityLogic.perform(
            context,
            sessionStore,
            pac4jConfig,
            (ctx, session, profiles, parameters) -> {
              try {
                // Extract user ID from pac4j profiles and create AuthenticationResult
                if (profiles != null && !profiles.isEmpty()) {
                  String uid = profiles.iterator().next().getId();
                  if (uid != null) {
                    AuthenticationResult authenticationResult = new AuthenticationResult(uid, authorizerName, name, null);
                    servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
                    filterChain.doFilter(servletRequest, servletResponse);
                  }
                } else {
                  LOGGER.warn("No profiles found after OIDC auth.");
                  // Don't continue the filter chain - let pac4j handle the authentication failure
                }
              }
              catch (IOException | ServletException e) {
                throw new RuntimeException(e);
              }
              return null;
            },
            JEEHttpActionAdapter.INSTANCE,
            null,
            "none",  // Use "none" instead of authorizerName to avoid CSRF issues
            null
        );
      }
      catch (HttpAction e) {
        JEEHttpActionAdapter.INSTANCE.adapt(e, context);
      }
    }
  }

  @Override
  public void destroy()
  {
  }
}
