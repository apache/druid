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

package io.druid.security.basic.authentication;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Provider;
import io.druid.java.util.common.IAE;
import io.druid.security.basic.BasicAuthDBConfig;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheManager;
import io.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import io.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authenticator;

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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("basic")
public class BasicHTTPAuthenticator implements Authenticator
{
  private final Provider<BasicAuthenticatorCacheManager> cacheManager;
  private final String name;
  private final String authorizerName;
  private final BasicAuthDBConfig dbConfig;

  @JsonCreator
  public BasicHTTPAuthenticator(
      @JacksonInject Provider<BasicAuthenticatorCacheManager> cacheManager,
      @JsonProperty("name") String name,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("initialAdminPassword") String initialAdminPassword,
      @JsonProperty("initialInternalClientPassword") String initialInternalClientPassword,
      @JsonProperty("enableCacheNotifications") Boolean enableCacheNotifications,
      @JsonProperty("cacheNotificationTimeout") Long cacheNotificationTimeout,
      @JsonProperty("credentialIterations") Integer credentialIterations
  )
  {
    this.name = name;
    this.authorizerName = authorizerName;
    this.dbConfig = new BasicAuthDBConfig(
        initialAdminPassword,
        initialInternalClientPassword,
        enableCacheNotifications == null ? true : enableCacheNotifications,
        cacheNotificationTimeout == null ? BasicAuthDBConfig.DEFAULT_CACHE_NOTIFY_TIMEOUT_MS : cacheNotificationTimeout,
        credentialIterations == null ? BasicAuthUtils.DEFAULT_KEY_ITERATIONS : credentialIterations
    );
    this.cacheManager = cacheManager;
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

    if (checkCredentials(user, password.toCharArray())) {
      return new AuthenticationResult(user, authorizerName, name, null);
    } else {
      return null;
    }
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
    public void doFilter(
        ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain
    ) throws IOException, ServletException
    {
      HttpServletResponse httpResp = (HttpServletResponse) servletResponse;

      String encodedUserSecret = BasicAuthUtils.getEncodedUserSecretFromHttpReq((HttpServletRequest) servletRequest);
      if (encodedUserSecret == null) {
        // Request didn't have HTTP Basic auth credentials, move on to the next filter
        filterChain.doFilter(servletRequest, servletResponse);
        return;
      }

      // At this point, encodedUserSecret is not null, indicating that the request intends to perform
      // Basic HTTP authentication. If any errors occur with the authentication, we send a 401 response immediately
      // and do not proceed further down the filter chain.
      String decodedUserSecret = BasicAuthUtils.decodeUserSecret(encodedUserSecret);
      if (decodedUserSecret == null) {
        // We recognized a Basic auth header, but could not decode the user secret.
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }

      String[] splits = decodedUserSecret.split(":");
      if (splits.length != 2) {
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }

      String user = splits[0];
      char[] password = splits[1].toCharArray();

      if (checkCredentials(user, password)) {
        AuthenticationResult authenticationResult = new AuthenticationResult(user, authorizerName, name, null);
        servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
        filterChain.doFilter(servletRequest, servletResponse);
      } else {
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      }
    }

    @Override
    public void destroy()
    {

    }
  }

  private boolean checkCredentials(String username, char[] password)
  {
    Map<String, BasicAuthenticatorUser> userMap = cacheManager.get().getUserMap(name);
    if (userMap == null) {
      throw new IAE("No authenticator found with prefix: [%s]", name);
    }

    BasicAuthenticatorUser user = userMap.get(username);
    if (user == null) {
      return false;
    }
    BasicAuthenticatorCredentials credentials = user.getCredentials();
    if (credentials == null) {
      return false;
    }

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        password,
        credentials.getSalt(),
        credentials.getIterations()
    );

    return Arrays.equals(recalculatedHash, credentials.getHash());
  }
}
