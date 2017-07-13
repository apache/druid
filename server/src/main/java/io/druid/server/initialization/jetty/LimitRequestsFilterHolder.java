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

package io.druid.server.initialization.jetty;

import com.google.common.collect.ImmutableMap;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LimitRequestsFilterHolder implements ServletFilterHolder
{
  private final String path;
  private final int maxActiveRequests;

  public LimitRequestsFilterHolder(String path, int maxActiveRequests)
  {
    this.path = path;
    this.maxActiveRequests = maxActiveRequests;
  }

  @Override
  public Filter getFilter()
  {
    return new LimitRequestsFilter(maxActiveRequests);
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return ImmutableMap.of();
  }

  @Override
  public String getPath()
  {
    return path;
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  private static class LimitRequestsFilter implements Filter
  {
    private final int maxActiveRequests;

    private AtomicInteger activeRequestsCount = new AtomicInteger();

    public LimitRequestsFilter(int maxActiveRequests)
    {
      this.maxActiveRequests = maxActiveRequests;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException
    {

      int curr = activeRequestsCount.incrementAndGet();
      try {
        if (curr <= maxActiveRequests) {
          chain.doFilter(request, response);
        } else {
          // See https://tools.ietf.org/html/rfc6585 for status code 429 explanation.
          ((HttpServletResponse)response).sendError(429, "Too Many Requests");
        }
      } finally {
        activeRequestsCount.decrementAndGet();
      }
    }

    @Override
    public void destroy() { }
  }
}
