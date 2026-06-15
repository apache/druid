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

package org.apache.druid.server.audit;

import org.apache.druid.audit.RequestHeaderContextConfig;
import org.apache.druid.java.util.common.logger.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Servlet filter that reads the headers configured via {@link RequestHeaderContextConfig}
 * from each inbound HTTP request, captures any present values into a thread-local
 * {@link RequestHeaderContext} keyed by the configured context-key, and clears the
 * thread-local in a finally block so the values don't leak across Jetty's pooled threads.
 */
public class RequestHeaderContextFilter implements Filter
{
  private static final Logger log = new Logger(RequestHeaderContextFilter.class);

  private final RequestHeaderContextConfig config;

  public RequestHeaderContextFilter(RequestHeaderContextConfig config)
  {
    this.config = config;
  }

  @Override
  public void init(FilterConfig filterConfig)
  {
    // No-op.
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException
  {
    // Always pair with clear() on the exit, even when the filter is effectively a no-op for
    // this request, so that any stale value bound by other code on this pooled thread is
    // wiped before the chain runs (defense in depth).
    try {
      if (request instanceof HttpServletRequest && !config.getHeaderToContextKey().isEmpty()) {
        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        Map<String, String> captured = null;
        for (Map.Entry<String, String> entry : config.getHeaderToContextKey().entrySet()) {
          final String value = httpRequest.getHeader(entry.getKey());
          if (value != null && !value.isEmpty()) {
            if (captured == null) {
              captured = new HashMap<>();
            }
            captured.put(entry.getValue(), value);
          }
        }
        if (captured != null) {
          RequestHeaderContext.bind(captured);
          // Debug-level so operators can confirm header capture/propagation without log spam.
          // The keys are the configured context-keys (e.g. traceId); values are caller-supplied.
          log.debug("Captured request-header context %s from inbound request", captured.keySet());
        }
      }
      chain.doFilter(request, response);
    }
    finally {
      RequestHeaderContext.clear();
    }
  }

  @Override
  public void destroy()
  {
    // No-op.
  }
}
