/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.cli;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import io.druid.server.coordinator.DruidCoordinatorConfig;
import io.druid.server.http.RedirectFilter;
import io.druid.server.initialization.BaseJettyServerInitializer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

/**
 */
class CoordinatorJettyServerInitializer extends BaseJettyServerInitializer
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

    ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);

    root.addServlet(holderPwd, "/");
    if(config.getConsoleStatic() == null) {
      root.setBaseResource(Resource.newClassPathResource("static"));
    } else {
      root.setResourceBase(config.getConsoleStatic());
    }
    root.addFilter(defaultGzipFilterHolder(), "/*", null);

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

    HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{root});

    server.setHandler(handlerList);
  }
}
