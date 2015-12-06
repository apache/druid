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

package io.druid.cli;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import io.druid.server.coordinator.DruidCoordinatorConfig;
import io.druid.server.http.OverlordProxyServlet;
import io.druid.server.http.RedirectFilter;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceCollection;

/**
 */
class CoordinatorJettyServerInitializer implements JettyServerInitializer
{
  private final DruidCoordinatorConfig config;

  @Inject
  CoordinatorJettyServerInitializer(DruidCoordinatorConfig config)
  {
    this.config = config;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

    ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);

    root.addServlet(holderPwd, "/");
    if(config.getConsoleStatic() == null) {
      ResourceCollection staticResources = new ResourceCollection(
          Resource.newClassPathResource("io/druid/console"),
          Resource.newClassPathResource("static")
      );
      root.setBaseResource(staticResources);
    } else {
      // used for console development
      root.setResourceBase(config.getConsoleStatic());
    }
    JettyServerInitUtils.addExtensionFilters(root, injector);
    root.addFilter(JettyServerInitUtils.defaultGzipFilterHolder(), "/*", null);

    // /status should not redirect, so add first
    root.addFilter(GuiceFilter.class, "/status/*", null);

    // redirect anything other than status to the current lead
    root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);

    // The coordinator really needs a standarized api path
    // Can't use '/*' here because of Guice and Jetty static content conflicts
    root.addFilter(GuiceFilter.class, "/info/*", null);
    root.addFilter(GuiceFilter.class, "/druid/coordinator/*", null);
    // this will be removed in the next major release
    root.addFilter(GuiceFilter.class, "/coordinator/*", null);

    root.addServlet(new ServletHolder(injector.getInstance(OverlordProxyServlet.class)), "/druid/indexer/*");

    HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{JettyServerInitUtils.getJettyRequestLogHandler(), root});

    server.setHandler(handlerList);
  }
}
