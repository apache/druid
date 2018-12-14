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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * Used to wrap Filters created by Authenticators, this wrapper filter skips itself if a request already
 * has an authentication check (so that Authenticator implementations don't have to perform this check themselves)
 */
public class AuthenticationWrappingFilter implements Filter
{
  private final Filter delegate;

  public AuthenticationWrappingFilter(
      final Filter delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException
  {
    delegate.init(filterConfig);
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException
  {
    // If there's already an auth result, then we have authenticated already, skip this.
    if (request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT) != null) {
      chain.doFilter(request, response);
    } else {
      delegate.doFilter(request, response, chain);
    }
  }

  @Override
  public void destroy()
  {
    delegate.destroy();
  }
}
