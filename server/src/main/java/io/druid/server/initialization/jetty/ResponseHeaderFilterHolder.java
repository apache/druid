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
import java.util.Enumeration;
import java.util.Map;

public class ResponseHeaderFilterHolder implements ServletFilterHolder
{
  private final String path;
  private final Map<String, String> headers;

  public ResponseHeaderFilterHolder(String path, Map<String, String> headers)
  {
    this.path = path;
    this.headers = headers;
  }

  @Override
  public Filter getFilter()
  {
    return new ResponseHeaderFilter();
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return ResponseHeaderFilter.class;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return ImmutableMap.copyOf(headers);
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

  private static class ResponseHeaderFilter implements Filter
  {
    private volatile FilterConfig config;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
      this.config = filterConfig;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException
    {
      if (config != null) {
        Enumeration<String> it = config.getInitParameterNames();
        while (it.hasMoreElements()) {
          String key = it.nextElement();
          ((HttpServletResponse) response).setHeader(key, config.getInitParameter(key));
        }
      }

      chain.doFilter(request, response);
    }

    @Override
    public void destroy()
    {
    }
  }
}
