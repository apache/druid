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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

/**
 * Authenticates requests coming from a specific domain and directs them to an authorizer.
 */
@JsonTypeName(AuthConfig.TRUSTED_DOMAIN_NAME)
public class TrustedDomainAuthenticator implements Authenticator
{
  private static final Logger LOGGER = new Logger(TrustedDomainAuthenticator.class);
  private static final String DEFAULT_IDENTITY = "defaultUser";
  private static final String X_FORWARDED_FOR = "X-Forwarded-For";

  private static final boolean DEFAULT_USE_FORWARDED_HEADERS = false;
  private final AuthenticationResult authenticationResult;
  private final String domain;
  private final boolean useForwardedHeaders;

  @JsonCreator
  public TrustedDomainAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("domain") String domain,
      @JsonProperty("useForwardedHeaders") Boolean useForwardedHeaders,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("identity") String identity
  )
  {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(domain), "Invalid domain name %s", domain);
    this.domain = domain;
    this.useForwardedHeaders = useForwardedHeaders == null ? DEFAULT_USE_FORWARDED_HEADERS : useForwardedHeaders;
    this.authenticationResult = new AuthenticationResult(
        identity == null ? DEFAULT_IDENTITY : identity,
        authorizerName,
        name,
        null
    );
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return null;
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  @Override
  public Filter getFilter()
  {
    return new Filter()
    {
      @Override
      public void init(FilterConfig filterConfig)
      {

      }

      @Override
      public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
          throws IOException, ServletException
      {
        String remoteAddr = request.getRemoteAddr();
        LOGGER.debug("Client IP Address: %s", remoteAddr);
        // get forwarded hosts address
        if (useForwardedHeaders) {
          String forwarded_for = getForwardedAddress(request);
          if (forwarded_for != null) {
            LOGGER.debug("Forwarded IP Address: %s", remoteAddr);
            remoteAddr = forwarded_for;
          }
        }
        if (remoteAddr.endsWith(domain)) {
          request.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
        }
        chain.doFilter(request, response);
      }

      @Override
      public void destroy()
      {

      }
    };
  }

  @Nullable
  private static String getForwardedAddress(ServletRequest request)
  {
    if (request instanceof HttpServletRequest) {
      String forwarded_for = ((HttpServletRequest) request).getHeader(X_FORWARDED_FOR);
      if (!Strings.isNullOrEmpty(forwarded_for)) {
        return forwarded_for.split(",")[0];
      }
    }
    return null;
  }

  @Override
  @Nullable
  public String getAuthChallengeHeader()
  {
    return null;
  }

  @Override
  @Nullable
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    return null;
  }
}
