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
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;

import io.druid.java.util.common.logger.Logger;

import org.eclipse.jetty.servlets.QoSFilter;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

public class JettyBindings
{
  private static final Logger log = new Logger(JettyBindings.class);

  private JettyBindings()
  {
    // No instantiation.
  }

  public static void addQosFilter(Binder binder, String path, int maxRequests)
  {
    if (maxRequests <= 0) {
      return;
    }

    Multibinder.newSetBinder(binder, ServletFilterHolder.class)
               .addBinding()
               .toInstance(new QosFilterHolder(path, maxRequests));
  }

  private static class QosFilterHolder implements ServletFilterHolder
  {
    private final String path;
    private final int maxRequests;

    public QosFilterHolder(String path, int maxRequests)
    {
      this.path = path;
      this.maxRequests = maxRequests;
    }

    @Override
    public Filter getFilter()
    {
      return new QoSFilter();
    }

    @Override
    public Class<? extends Filter> getFilterClass()
    {
      return QoSFilter.class;
    }

    @Override
    public Map<String, String> getInitParameters()
    {
      return ImmutableMap.of("maxRequests", String.valueOf(maxRequests));
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
  }
}
