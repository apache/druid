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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.pac4j.core.context.HttpConstants;
import org.pac4j.oidc.profile.creator.TokenValidator;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.ParseException;
import java.util.Optional;

public class JwtAuthFilter implements Filter
{
  private static final Logger LOG = new Logger(JwtAuthFilter.class);

  private final String authorizerName;
  private final String name;
  private final OIDCConfig oidcConfig;
  private final TokenValidator tokenValidator;

  public JwtAuthFilter(String authorizerName, String name, OIDCConfig oidcConfig, TokenValidator tokenValidator)
  {
    this.authorizerName = authorizerName;
    this.name = name;
    this.oidcConfig = oidcConfig;
    this.tokenValidator = tokenValidator;
  }

  @Override
  public void init(FilterConfig filterConfig)
  {

  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException
  {
    // Skip this filter if the request has already been authenticated
    if (servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT) != null) {
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    }

    HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
    HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
    Optional<String> idToken = extractBearerToken(httpServletRequest);

    if (idToken.isPresent()) {
      try {
        // Parses the JWT and performs the ID Token validation specified in the OpenID spec: https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation
        IDTokenClaimsSet claims = tokenValidator.validate(JWTParser.parse(idToken.get()), null);
        if (claims != null) {
          Optional<String> claim = Optional.ofNullable(claims.getStringClaim(oidcConfig.getOidcClaim()));

          if (claim.isPresent()) {
            LOG.debug("Authentication successful for " + oidcConfig.getClientID());
            AuthenticationResult authenticationResult = new AuthenticationResult(
                claim.get(),
                authorizerName,
                name,
                null
            );
            servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
          } else {
            LOG.error(
                "Authentication failed! Please ensure that the ID token is valid and it contains the configured claim.");
            httpServletResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
          }
        }
      }
      catch (BadJOSEException | JOSEException | ParseException e) {
        LOG.error(e, "Failed to parse JWT token");
      }
    }
    filterChain.doFilter(servletRequest, servletResponse);
  }


  @Override
  public void destroy()
  {

  }

  private static Optional<String> extractBearerToken(HttpServletRequest request)
  {
    String header = request.getHeader(HttpConstants.AUTHORIZATION_HEADER);
    if (header == null || !header.startsWith(HttpConstants.BEARER_HEADER_PREFIX)) {
      LOG.debug("Request does not contain bearer authentication scheme");
      return Optional.empty();
    }
    String headerWithoutPrefix = header.substring(HttpConstants.BEARER_HEADER_PREFIX.length());
    return Optional.of(headerWithoutPrefix);
  }
}
