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

package org.apache.druid.security.basic.authentication;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Provider;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.security.basic.BasicAuthDBConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityAuthenticationException;
import org.apache.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheManager;
import org.apache.druid.security.basic.authentication.validator.CredentialsValidator;
import org.apache.druid.security.basic.authentication.validator.MetadataStoreCredentialsValidator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

@JsonTypeName("basic")
public class BasicHTTPAuthenticator implements Authenticator
{
  private static final Logger LOG = new Logger(BasicHTTPAuthenticator.class);

  private final String name;
  private final String authorizerName;
  private final BasicAuthDBConfig dbConfig;
  private final CredentialsValidator credentialsValidator;
  private final boolean skipOnFailure;

  @JsonCreator
  public BasicHTTPAuthenticator(
      @JacksonInject Provider<BasicAuthenticatorCacheManager> cacheManager,
      @JsonProperty("name") String name,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("initialAdminPassword") PasswordProvider initialAdminPassword,
      @JsonProperty("initialInternalClientPassword") PasswordProvider initialInternalClientPassword,
      @JsonProperty("enableCacheNotifications") Boolean enableCacheNotifications,
      @JsonProperty("cacheNotificationTimeout") Long cacheNotificationTimeout,
      @JsonProperty("credentialIterations") Integer credentialIterations,
      @JsonProperty("skipOnFailure") Boolean skipOnFailure,
      @JsonProperty("credentialsValidator") CredentialsValidator credentialsValidator
  )
  {
    this.name = name;
    this.authorizerName = authorizerName;
    this.dbConfig = new BasicAuthDBConfig(
        initialAdminPassword,
        initialInternalClientPassword,
        null,
        null,
        null,
        enableCacheNotifications == null ? true : enableCacheNotifications,
        cacheNotificationTimeout == null ? BasicAuthDBConfig.DEFAULT_CACHE_NOTIFY_TIMEOUT_MS : cacheNotificationTimeout,
        credentialIterations == null ? BasicAuthUtils.DEFAULT_KEY_ITERATIONS : credentialIterations
    );
    if (credentialsValidator == null) {
      this.credentialsValidator = new MetadataStoreCredentialsValidator(cacheManager);
    } else {
      this.credentialsValidator = credentialsValidator;
    }
    this.skipOnFailure = skipOnFailure == null ? false : skipOnFailure;
  }

  @Override
  public Filter getFilter()
  {
    return new BasicHTTPAuthenticationFilter();
  }

  @Override
  public String getAuthChallengeHeader()
  {
    return "Basic";
  }

  @Override
  @Nullable
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    String user = (String) context.get("user");
    String password = (String) context.get("password");

    if (user == null || password == null) {
      return null;
    }

    return credentialsValidator.validateCredentials(name, authorizerName, user, password.toCharArray());
  }


  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return BasicHTTPAuthenticationFilter.class;
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

  public BasicAuthDBConfig getDbConfig()
  {
    return dbConfig;
  }

  public class BasicHTTPAuthenticationFilter implements Filter
  {
    @Override
    public void init(FilterConfig filterConfig)
    {

    }


    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
        throws IOException, ServletException
    {
      HttpServletResponse httpResp = (HttpServletResponse) servletResponse;

      String encodedUserSecret = BasicAuthUtils.getEncodedUserSecretFromHttpReq((HttpServletRequest) servletRequest);
      if (encodedUserSecret == null) {
        // Request didn't have HTTP Basic auth credentials, move on to the next filter
        filterChain.doFilter(servletRequest, servletResponse);
        return;
      }

      // At this point, encodedUserSecret is not null, indicating that the request intends to perform
      // Basic HTTP authentication.
      String decodedUserSecret = BasicAuthUtils.decodeUserSecret(encodedUserSecret);
      if (decodedUserSecret == null) {
        // We recognized a Basic auth header, but could not decode the user secret.
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }

      String[] splits = decodedUserSecret.split(":");
      if (splits.length != 2) {
        // The decoded user secret is not of the right format
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }

      String user = splits[0];
      char[] password = splits[1].toCharArray();

      // If any authentication error occurs we send a 401 response immediately and do not proceed further down the filter chain.
      // If the authentication result is null and skipOnFailure property is false, we send a 401 response and do not proceed
      // further down the filter chain. If the authentication result is null and skipOnFailure is true then move on to the next filter.
      // Authentication results, for instance, can be null if a user doesn't exists within a user store
      try {
        AuthenticationResult authenticationResult = credentialsValidator.validateCredentials(
            name,
            authorizerName,
            user,
            password
        );
        if (authenticationResult != null) {
          servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
          filterChain.doFilter(servletRequest, servletResponse);
        } else {
          if (skipOnFailure) {
            LOG.info("Skipping failed authenticator %s ", name);
            filterChain.doFilter(servletRequest, servletResponse);
          } else {
            httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
          }
        }
      }
      catch (BasicSecurityAuthenticationException ex) {
        LOG.info("Exception authenticating user %s - %s", user, ex.getMessage());
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                           String.format(Locale.getDefault(),
                                         "User authentication failed username[%s].", user));
      }
    }

    @Override
    public void destroy()
    {

    }
  }
}
