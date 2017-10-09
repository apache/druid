/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.basic.authentication;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.metamx.http.client.CredentialedHttpClient;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.auth.BasicCredentials;
import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.db.BasicSecurityStorageConnector;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authenticator;
import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.Attributes;

import org.jboss.netty.handler.codec.http.HttpHeaders;

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
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("basic")
public class BasicHTTPAuthenticator implements Authenticator
{
  private final BasicSecurityStorageConnector dbConnector;
  private final String internalClientUsername;
  private final String internalClientPassword;
  private final String authorizerName;

  @JsonCreator
  public BasicHTTPAuthenticator(
      @JacksonInject BasicSecurityStorageConnector dbConnector,
      @JsonProperty("internalClientUsername") String internalClientUsername,
      @JsonProperty("internalClientPassword") String internalClientPassword,
      @JsonProperty("authorizerName") String authorizerName
  )
  {
    this.dbConnector = dbConnector;
    this.internalClientUsername = internalClientUsername;
    this.internalClientPassword = internalClientPassword;
    this.authorizerName = authorizerName;
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

    if (dbConnector.checkCredentials(user, password.toCharArray())) {
      return new AuthenticationResult(user, authorizerName, null);
    } else {
      return null;
    }
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    return new CredentialedHttpClient(
        new BasicCredentials(internalClientUsername, internalClientPassword),
        baseClient
    );
  }

  @Override
  public org.eclipse.jetty.client.HttpClient createEscalatedJettyClient(org.eclipse.jetty.client.HttpClient baseClient)
  {
    baseClient.getAuthenticationStore().addAuthentication(new Authentication()
    {
      @Override
      public boolean matches(String type, URI uri, String realm)
      {
        return true;
      }

      @Override
      public Result authenticate(
          final Request request, ContentResponse response, Authentication.HeaderInfo headerInfo, Attributes context
      )
      {
        return new Result()
        {
          @Override
          public URI getURI()
          {
            return request.getURI();
          }

          @Override
          public void apply(Request request)
          {
            try {
              final String unencodedCreds = StringUtils.format("%s:%s", internalClientUsername, internalClientPassword);
              final String base64Creds = BasicAuthUtils.getEncodedCredentials(unencodedCreds);
              request.getHeaders().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + base64Creds);
            }
            catch (Throwable e) {
              Throwables.propagate(e);
            }
          }
        };
      }
    });
    return baseClient;
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    return new AuthenticationResult(internalClientUsername, authorizerName, null);
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

  public class BasicHTTPAuthenticationFilter implements Filter
  {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {

    }

    @Override
    public void doFilter(
        ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain
    ) throws IOException, ServletException
    {
      HttpServletResponse httpResp = (HttpServletResponse) servletResponse;
      String userSecret = BasicAuthUtils.getBasicUserSecretFromHttpReq((HttpServletRequest) servletRequest);
      if (userSecret == null) {
        // Request didn't have HTTP Basic auth credentials, move on to the next filter
        filterChain.doFilter(servletRequest, servletResponse);
        return;
      }

      String[] splits = userSecret.split(":");
      if (splits.length != 2) {
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }

      String user = splits[0];
      char[] password = splits[1].toCharArray();

      if (dbConnector.checkCredentials(user, password)) {
        AuthenticationResult authenticationResult = new AuthenticationResult(user, authorizerName, null);
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
}
