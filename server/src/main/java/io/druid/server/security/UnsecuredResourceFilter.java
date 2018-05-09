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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * Sets necessary request attributes for requests sent to endpoints that don't need authentication or
 * authorization checks. This Filter is placed before all Authenticators in the filter chain.
 */
public class UnsecuredResourceFilter implements Filter
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
    // PreResponseAuthorizationCheckFilter checks that this attribute is set,
    // but the value doesn't matter since we skip authorization checks for requests that go through this filter
    servletRequest.setAttribute(
        AuthConfig.DRUID_AUTHENTICATION_RESULT,
        new AuthenticationResult(AuthConfig.ALLOW_ALL_NAME, AuthConfig.ALLOW_ALL_NAME, AuthConfig.ALLOW_ALL_NAME, null)
    );

    // This request will not go to an Authorizer, so we need to set this for PreResponseAuthorizationCheckFilter
    servletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    servletRequest.setAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH, true);
    filterChain.doFilter(servletRequest, servletResponse);
  }

  @Override
  public void destroy()
  {

  }
}
