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

import com.google.inject.Inject;
import org.apache.druid.query.context.RequestHeaderContextConfig;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletRequest;
import java.util.EnumSet;
import java.util.Map;

/** Captures configured request headers after authentication through the standard servlet filter bindings. */
public class RequestHeaderContextFilterHolder implements ServletFilterHolder
{
  private static final String ATTRIBUTE = RequestHeaderContextFilterHolder.class.getName();

  private final RequestHeaderContextConfig config;

  @Inject
  public RequestHeaderContextFilterHolder(final RequestHeaderContextConfig config)
  {
    this.config = config;
  }

  /** Captured values belong to the request and remain available during asynchronous processing. */
  @SuppressWarnings("unchecked")
  public static Map<String, String> contextFor(final HttpServletRequest request)
  {
    final Map<String, String> values = (Map<String, String>) request.getAttribute(ATTRIBUTE);
    return values == null ? Map.of() : values;
  }

  @Override
  public Filter getFilter()
  {
    return (request, response, chain) -> {
      request.setAttribute(ATTRIBUTE, config.capture(((HttpServletRequest) request)::getHeader));
      chain.doFilter(request, response);
    };
  }

  @Nullable
  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return Map.of();
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return EnumSet.of(DispatcherType.REQUEST);
  }
}
