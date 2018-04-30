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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import java.io.IOException;

public class AllowOptionsResourceFilter implements Filter
{
  private final boolean allowUnauthenticatedHttpOptions;

  public AllowOptionsResourceFilter(
      boolean allowUnauthenticatedHttpOptions
  )
  {
    this.allowUnauthenticatedHttpOptions = allowUnauthenticatedHttpOptions;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException
  {

  }

  @Override
  public void doFilter(
      ServletRequest request, ServletResponse response, FilterChain chain
  ) throws IOException, ServletException
  {
    HttpServletRequest httpReq = (HttpServletRequest) request;

    // Druid itself doesn't explictly handle OPTIONS requests, no resource handler will authorize such requests.
    // so this filter catches all OPTIONS requests and authorizes them.
    if (HttpMethod.OPTIONS.equals(httpReq.getMethod())) {
      if (httpReq.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT) == null) {
        // If the request already had credentials and authenticated successfully, keep the authenticated identity.
        // Otherwise, allow the unauthenticated request.
        if (allowUnauthenticatedHttpOptions) {
          httpReq.setAttribute(
              AuthConfig.DRUID_AUTHENTICATION_RESULT,
              new AuthenticationResult(AuthConfig.ALLOW_ALL_NAME, AuthConfig.ALLOW_ALL_NAME, null, null)
          );
        } else {
          ((HttpServletResponse) response).sendError(HttpServletResponse.SC_UNAUTHORIZED);
        }
      }

      httpReq.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    }

    chain.doFilter(request, response);
  }

  @Override
  public void destroy()
  {

  }
}
