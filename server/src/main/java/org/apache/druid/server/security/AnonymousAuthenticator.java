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
 * Authenticates all requests and directs them to an authorizer.
 */
@JsonTypeName("anonymous")
public class AnonymousAuthenticator implements Authenticator
{
  private static final String DEFAULT_IDENTITY = "defaultUser";
  private final AuthenticationResult anonymousResult;

  @JsonCreator
  public AnonymousAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("identity") String identity
  )
  {
    this.anonymousResult = new AuthenticationResult(
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
        request.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, anonymousResult);
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
    return anonymousResult;
  }
}
