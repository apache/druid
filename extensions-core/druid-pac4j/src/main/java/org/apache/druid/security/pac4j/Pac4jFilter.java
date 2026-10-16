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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.pac4j.core.config.Config;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.engine.DefaultSecurityLogic;
import org.pac4j.core.exception.http.HttpAction;
import org.pac4j.core.profile.UserProfile;
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
import java.util.Set;
import java.util.function.Supplier;

public class Pac4jFilter implements Filter
{
  private static final Logger LOGGER = new Logger(Pac4jFilter.class);
  public static final String ROLE_CLAIM_CONTEXT_KEY = "druidRoles";

  private final Config pac4jConfig;
  private final Pac4jSessionStore sessionStore;
  private final String callbackPath;
  private final String name;
  private final String authorizerName;
  private final Supplier<DefaultSecurityLogic> securityLogicFactory;
  private final Supplier<DefaultCallbackLogic> callbackLogicFactory;

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
    this.securityLogicFactory = Suppliers.memoize(DefaultSecurityLogic::new);
    this.callbackLogicFactory = Suppliers.memoize(DefaultCallbackLogic::new);
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
      DefaultCallbackLogic callbackLogic = callbackLogicFactory.get();
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
      DefaultSecurityLogic securityLogic = securityLogicFactory.get();
      try {
        securityLogic.perform(
            context,
            sessionStore,
            pac4jConfig,
            (ctx, session, profiles, parameters) -> {
              try {
                // Extract user ID from pac4j profiles and create AuthenticationResult
                if (profiles != null && !profiles.isEmpty()) {
                  UserProfile profile = profiles.iterator().next();
                  String uid = profile.getId();
                  if (uid != null) {
                    final Set<String> roles = profile.getRoles();
                    LOGGER.debug("Collected identity: %s with roles: %s", uid, roles);
                    final ImmutableMap.Builder<String, Object> authResultContext = ImmutableMap.builder();
                    if (roles != null && !roles.isEmpty()) {
                      authResultContext.put(ROLE_CLAIM_CONTEXT_KEY, roles);
                    }
                    AuthenticationResult authenticationResult = new AuthenticationResult(uid, authorizerName, name, authResultContext.build());
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
