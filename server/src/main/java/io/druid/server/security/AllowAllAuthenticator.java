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

package io.druid.server.security;

import com.metamx.http.client.HttpClient;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

/**
 * Should only be used in conjunction with AllowAllAuthorizer.
 */
public class AllowAllAuthenticator implements Authenticator
{
  public static final AuthenticationResult ALLOW_ALL_RESULT = new AuthenticationResult("allowAll", "allowAll", null);

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
      public void init(FilterConfig filterConfig) throws ServletException
      {

      }

      @Override
      public void doFilter(
          ServletRequest request, ServletResponse response, FilterChain chain
      ) throws IOException, ServletException
      {
        request.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, ALLOW_ALL_RESULT);
        chain.doFilter(request, response);
      }

      @Override
      public void destroy()
      {

      }
    };
  }

  @Override
  public String getAuthChallengeHeader()
  {
    return null;
  }

  @Override
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    return ALLOW_ALL_RESULT;
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    return baseClient;
  }

  @Override
  public org.eclipse.jetty.client.HttpClient createEscalatedJettyClient(org.eclipse.jetty.client.HttpClient baseClient)
  {
    return baseClient;
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    return ALLOW_ALL_RESULT;
  }
}
