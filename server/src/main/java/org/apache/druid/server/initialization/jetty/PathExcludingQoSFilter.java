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

import org.eclipse.jetty.ee8.servlets.QoSFilter;
import org.eclipse.jetty.http.pathmap.PathSpecSet;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * A {@link QoSFilter} that allows a set of request paths to bypass QoS
 * throttling entirely. This is useful for lightweight leadership / health-check
 * endpoints (such as {@code /druid/coordinator/v1/isLeader}) that must remain
 * responsive even when heavier APIs matched by the same broad filter path-spec
 * have saturated the QoS semaphore. Servlet filter mappings cannot express path
 * exclusions, so the exclusion is applied here at request time.
 *
 * <p>Excluded requests are passed straight down the filter chain without ever
 * acquiring the QoS semaphore; all other requests are handled by the standard
 * {@link QoSFilter} behavior.
 */
public class PathExcludingQoSFilter extends QoSFilter
{
  private final PathSpecSet excludedPaths;

  public PathExcludingQoSFilter(String[] excludedPaths)
  {
    this.excludedPaths = new PathSpecSet();
    if (excludedPaths != null) {
      for (String path : excludedPaths) {
        this.excludedPaths.add(path);
      }
    }
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException
  {
    if (isExcluded(request)) {
      chain.doFilter(request, response);
    } else {
      super.doFilter(request, response, chain);
    }
  }

  private boolean isExcluded(ServletRequest request)
  {
    if (!(request instanceof HttpServletRequest)) {
      return false;
    }
    final HttpServletRequest httpRequest = (HttpServletRequest) request;
    final String contextPath = httpRequest.getContextPath();
    String path = httpRequest.getRequestURI();
    if (contextPath != null && !contextPath.isEmpty() && path.startsWith(contextPath)) {
      path = path.substring(contextPath.length());
    }
    return excludedPaths.test(path);
  }
}
