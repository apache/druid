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

package io.druid.server.http;

import com.google.inject.Inject;

import io.druid.java.util.common.logger.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;

/**
 */
public class RedirectFilter implements Filter
{
  private static final Logger log = new Logger(RedirectFilter.class);

  private final RedirectInfo redirectInfo;

  @Inject
  public RedirectFilter(
      RedirectInfo redirectInfo
  )
  {
    this.redirectInfo = redirectInfo;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
      throws IOException, ServletException
  {
    HttpServletRequest request;
    HttpServletResponse response;

    try {
      request = (HttpServletRequest) req;
      response = (HttpServletResponse) res;
    }
    catch (ClassCastException e) {
      throw new ServletException("non-HTTP request or response");
    }

    if (redirectInfo.doLocal(request.getRequestURI())) {
      chain.doFilter(request, response);
    } else {
      URL url = redirectInfo.getRedirectURL(request.getScheme(), request.getQueryString(), request.getRequestURI());
      log.debug("Forwarding request to [%s]", url);

      if (url == null) {
        // We apparently have nothing to redirect to, so let's do a Service Unavailable
        response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        return;
      }

      response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
      response.setHeader("Location", url.toString());
    }
  }

  @Override
  public void destroy() {}
}
