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

package org.apache.druid.server.initialization.jetty;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.commons.lang.CharUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.server.initialization.ServerConfig;
import org.eclipse.jetty.client.api.Response;

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
import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Adds response headers that we want to have on all responses.
 */
public class StandardResponseHeaderFilterHolder implements ServletFilterHolder
{
  private static final Set<String> STANDARD_HEADERS = ImmutableSet.of("Cache-Control", "Content-Security-Policy");
  private static final String DEFAULT_CONTENT_SECURITY_POLICY = "frame-ancestors 'none'";

  private final String contentSecurityPolicy;

  @Inject
  public StandardResponseHeaderFilterHolder(final ServerConfig serverConfig)
  {
    this.contentSecurityPolicy = asContentSecurityPolicyHeaderValue(serverConfig.getContentSecurityPolicy());
  }

  /**
   * Remove any standard headers in proxyResponse if they were also set in the origin response, serverResponse.
   * This prevents duplicates headers from appearing in proxy responses.
   *
   * Used by implementations of {@link org.eclipse.jetty.proxy.AsyncProxyServlet}.
   */
  public static void deduplicateHeadersInProxyServlet(
      final HttpServletResponse proxyResponse,
      final Response serverResponse
  )
  {
    for (final String headerName : StandardResponseHeaderFilterHolder.STANDARD_HEADERS) {
      if (serverResponse.getHeaders().containsKey(headerName) && proxyResponse.containsHeader(headerName)) {
        ((org.eclipse.jetty.server.Response) proxyResponse).getHttpFields().remove(headerName);
      }
    }
  }

  static String asContentSecurityPolicyHeaderValue(@Nullable final String contentSecurityPolicy)
  {
    if (contentSecurityPolicy == null || contentSecurityPolicy.trim().isEmpty()) {
      return DEFAULT_CONTENT_SECURITY_POLICY;
    } else {
      // Header values must be ASCII or RFC 2047 encoded. We don't have an RFC 2047 encoder handy, so require
      // that the value be plain ASCII.
      for (int i = 0; i < contentSecurityPolicy.length(); i++) {
        if (!CharUtils.isAscii(contentSecurityPolicy.charAt(i))) {
          throw new IAE("Content-Security-Policy header value must be fully ASCII");
        }
      }

      return contentSecurityPolicy;
    }
  }

  @Override
  public Filter getFilter()
  {
    return new StandardResponseHeaderFilter(contentSecurityPolicy);
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return StandardResponseHeaderFilter.class;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return Collections.emptyMap();
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Nullable
  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  static class StandardResponseHeaderFilter implements Filter
  {
    private final String contentSecurityPolicy;

    public StandardResponseHeaderFilter(final String contentSecurityPolicy)
    {
      this.contentSecurityPolicy = contentSecurityPolicy;
    }

    @Override
    public void init(FilterConfig filterConfig)
    {
      // Nothing to do.
    }

    @Override
    public void doFilter(
        ServletRequest request,
        ServletResponse response,
        FilterChain chain
    ) throws IOException, ServletException
    {
      final HttpServletRequest httpRequest = (HttpServletRequest) request;
      final HttpServletResponse httpResponse = (HttpServletResponse) response;

      if (!HttpMethod.POST.equals(httpRequest.getMethod())) {
        // Disable client-side caching on non-POSTs. (POST requests are not typically cached.)
        httpResponse.setHeader("Cache-Control", "no-cache, no-store, max-age=0");

        // Set the desired Content-Security-Policy on non-POSTs. (It's for web pages, which we don't serve via POST.)
        httpResponse.setHeader("Content-Security-Policy", contentSecurityPolicy);
      }

      chain.doFilter(request, response);
    }

    @Override
    public void destroy()
    {
      // Nothing to do.
    }
  }
}
