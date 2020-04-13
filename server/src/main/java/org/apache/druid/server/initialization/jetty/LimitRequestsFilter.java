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

import com.google.common.base.Preconditions;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class LimitRequestsFilter implements Filter
{
  private final int maxActiveRequests;

  private final AtomicInteger activeRequestsCount = new AtomicInteger();

  public LimitRequestsFilter(int maxActiveRequests)
  {
    Preconditions.checkArgument(
        maxActiveRequests > 0 && maxActiveRequests < Integer.MAX_VALUE,
        "maxActiveRequests must be > 0 and < Integer.MAX_VALUE."
    );
    this.maxActiveRequests = maxActiveRequests;
  }

  @Override
  public void init(FilterConfig filterConfig)
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
        ((HttpServletResponse) response).sendError(429, "Too Many Requests");
      }
    }
    finally {
      activeRequestsCount.decrementAndGet();
    }
  }

  @Override
  public void destroy()
  {

  }

  public int getActiveRequestsCount()
  {
    return activeRequestsCount.get();
  }
}
