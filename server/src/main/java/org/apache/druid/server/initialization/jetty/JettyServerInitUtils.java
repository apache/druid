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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.servlet.GuiceFilter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AllowHttpMethodsResourceFilter;
import org.eclipse.jetty.rewrite.handler.HeaderPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletContextHandler;

import javax.ws.rs.HttpMethod;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class JettyServerInitUtils
{
  private static final String[] GZIP_METHODS = new String[]{HttpMethod.GET, HttpMethod.POST};

  public static GzipHandler wrapWithDefaultGzipHandler(final Handler handler, int inflateBufferSize, int compressionLevel)
  {
    GzipHandler gzipHandler = new GzipHandler();
    gzipHandler.setMinGzipSize(0);
    gzipHandler.setIncludedMethods(GZIP_METHODS);
    gzipHandler.setInflateBufferSize(inflateBufferSize);
    gzipHandler.setCompressionLevel(compressionLevel);

    // We don't actually have any precomputed .gz resources, and checking for them inside jars is expensive.
    gzipHandler.setCheckGzExists(false);
    gzipHandler.setHandler(handler);
    return gzipHandler;
  }

  /**
   * Add any filters that were registered with {@link JettyBindings#addQosFilter}. These must be added first in
   * the filter chain, because when a request is suspended and later resumed due to QoS constraints, its filter
   * chain is restarted. Placing QoSFilters first in the chain avoids double-execution of other filters.
   */
  public static void addQosFilters(ServletContextHandler handler, Injector injector)
  {
    final Set<JettyBindings.QosFilterHolder> filters =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    addFilters(handler, filters);
  }

  public static void addExtensionFilters(ServletContextHandler handler, Injector injector)
  {
    final Set<ServletFilterHolder> filters =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    addFilters(handler, filters);
  }

  public static void addFilters(ServletContextHandler handler, Set<? extends ServletFilterHolder> filterHolders)
  {
    for (ServletFilterHolder servletFilterHolder : filterHolders) {
      // Check the Filter first to guard against people who don't read the docs and return the Class even
      // when they have an instance.
      FilterHolder holder;
      if (servletFilterHolder.getFilter() != null) {
        holder = new FilterHolder(servletFilterHolder.getFilter());
      } else if (servletFilterHolder.getFilterClass() != null) {
        holder = new FilterHolder(servletFilterHolder.getFilterClass());
      } else {
        throw new ISE(
            "Filter[%s] for paths[%s] didn't have a Filter!?",
            servletFilterHolder,
            Arrays.toString(servletFilterHolder.getPaths())
        );
      }

      if (servletFilterHolder.getInitParameters() != null) {
        holder.setInitParameters(servletFilterHolder.getInitParameters());
      }

      FilterMapping filterMapping = new FilterMapping();
      filterMapping.setFilterName(holder.getName());
      filterMapping.setPathSpecs(servletFilterHolder.getPaths());
      filterMapping.setDispatcherTypes(servletFilterHolder.getDispatcherType());

      handler.getServletHandler().addFilter(holder, filterMapping);
    }
  }

  public static Handler getJettyRequestLogHandler()
  {
    // Ref: http://www.eclipse.org/jetty/documentation/9.2.6.v20141205/configuring-jetty-request-logs.html
    RequestLogHandler requestLogHandler = new RequestLogHandler();
    requestLogHandler.setRequestLog(new JettyRequestLog());

    return requestLogHandler;
  }

  public static void addAllowHttpMethodsFilter(ServletContextHandler root, List<String> allowedHttpMethods)
  {
    FilterHolder holder = new FilterHolder(new AllowHttpMethodsResourceFilter(allowedHttpMethods));
    root.addFilter(
        holder,
        "/*",
        null
    );
  }
  
  public static void maybeAddHSTSPatternRule(ServerConfig serverConfig, RewriteHandler rewriteHandler)
  {
    if (serverConfig.isEnableHSTS()) {
      rewriteHandler.addRule(getHSTSHeaderPattern());
    }
  }

  public static void maybeAddHSTSRewriteHandler(ServerConfig serverConfig, HandlerList handlerList)
  {
    if (serverConfig.isEnableHSTS()) {
      RewriteHandler rewriteHandler = new RewriteHandler();
      rewriteHandler.addRule(getHSTSHeaderPattern());
      handlerList.addHandler(rewriteHandler);
    }
  }

  /**
   * Using this {@link FilterHolder} forces the underlying {@link GuiceFilter}
   * to be instantiated with the 1-argument constructor that uses an injected
   * non-static FilterPipeline. Since the FilterPipeline is non-static, multiple
   * Jetty servers may be initialized by separate Guice Injectors running in the
   * same JVM. This is currently needed only for running simulation tests.
   *
   * @see GuiceFilter#GuiceFilter
   */
  public static FilterHolder getGuiceFilterHolder(Injector injector)
  {
    return new FilterHolder(injector.getInstance(GuiceFilter.class));
  }

  private static HeaderPatternRule getHSTSHeaderPattern()
  {
    return new HeaderPatternRule("*", "Strict-Transport-Security", "max-age=63072000; includeSubDomains");
  }
}
