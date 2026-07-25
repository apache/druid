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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.eclipse.jetty.server.Handler;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

public class JettyBindings
{
  private JettyBindings()
  {
    // No instantiation.
  }

  public static void addQosFilter(Binder binder, String path, int maxRequests)
  {
    addQosFilter(binder, new String[]{path}, maxRequests);
  }

  public static void addQosFilter(Binder binder, String[] paths, int maxRequests)
  {
    addQosFilter(binder, paths, maxRequests, null);
  }

  /**
   * Registers a QoS filter for the given {@code paths}, exempting any request
   * that matches one of {@code excludedPaths} (servlet path-specs) from QoS
   * throttling. Exclusions are needed because servlet filter mappings cannot
   * express a "match this prefix except this sub-path" rule; see
   * {@link PathExcludingQoSFilter}.
   */
  public static void addQosFilter(Binder binder, String[] paths, int maxRequests, String[] excludedPaths)
  {
    if (maxRequests <= 0) {
      return;
    }

    Multibinder.newSetBinder(binder, QosFilterHolder.class)
               .addBinding()
               .toInstance(new QosFilterHolder(paths, maxRequests, excludedPaths));
  }

  public static void addHandler(Binder binder, Class<? extends Handler> handlerClass)
  {
    Multibinder.newSetBinder(binder, Handler.class)
               .addBinding()
               .to(handlerClass);
  }

  public static class QosFilterHolder implements ServletFilterHolder
  {
    private final String[] paths;
    private final int maxRequests;

    private final long timeoutMs;

    private final String[] excludedPaths;

    public QosFilterHolder(String[] paths, int maxRequests, long timeoutMs, String[] excludedPaths)
    {
      this.paths = paths;
      this.maxRequests = maxRequests;
      this.timeoutMs = timeoutMs;
      this.excludedPaths = excludedPaths == null ? new String[0] : excludedPaths;
    }

    public QosFilterHolder(String[] paths, int maxRequests, long timeoutMs)
    {
      this(paths, maxRequests, timeoutMs, null);
    }

    public QosFilterHolder(String[] paths, int maxRequests, String[] excludedPaths)
    {
      this(paths, maxRequests, -1, excludedPaths);
    }

    public QosFilterHolder(String[] paths, int maxRequests)
    {
      this(paths, maxRequests, -1, null);
    }

    @Override
    public Filter getFilter()
    {
      return new PathExcludingQoSFilter(excludedPaths);
    }

    @Override
    public Class<? extends Filter> getFilterClass()
    {
      return PathExcludingQoSFilter.class;
    }

    public String[] getExcludedPaths()
    {
      return excludedPaths;
    }

    @Override
    public Map<String, String> getInitParameters()
    {
      if (timeoutMs < 0) {
        return ImmutableMap.of("maxRequests", String.valueOf(maxRequests));
      }
      if (timeoutMs > Integer.MAX_VALUE) {
        // QoSFilter tries to parse the suspendMs parameter as an int, so we can't set it to more than Integer
        // .MAX_VALUE.
        return ImmutableMap.of("maxRequests", String.valueOf(maxRequests), "suspendMs", String.valueOf(Integer.MAX_VALUE));
      }
      return ImmutableMap.of("maxRequests", String.valueOf(maxRequests), "suspendMs", String.valueOf(timeoutMs));
    }

    @Override
    public String getPath()
    {
      return null;
    }

    @Override
    public String[] getPaths()
    {
      return paths;
    }

    @Override
    public EnumSet<DispatcherType> getDispatcherType()
    {
      return null;
    }
  }
}
