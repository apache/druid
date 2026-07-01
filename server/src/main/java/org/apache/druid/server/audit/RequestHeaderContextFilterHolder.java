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

import com.google.inject.Inject;
import org.apache.druid.audit.RequestHeaderContextConfig;
import org.apache.druid.server.initialization.jetty.ServletFilterHolder;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

/**
 * Wires {@link RequestHeaderContextFilter} into the Jetty pipeline on every Druid HTTP
 * endpoint.
 */
public class RequestHeaderContextFilterHolder implements ServletFilterHolder
{
  private final RequestHeaderContextConfig config;

  @Inject
  public RequestHeaderContextFilterHolder(RequestHeaderContextConfig config)
  {
    this.config = config;
  }

  @Override
  public Filter getFilter()
  {
    return new RequestHeaderContextFilter(config);
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
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

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }
}
