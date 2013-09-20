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

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import io.druid.server.http.RedirectFilter;
import io.druid.server.initialization.JettyServerInitializer;
import io.druid.server.master.DruidMaster;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;

/**
*/
class CoordinatorJettyServerInitializer implements JettyServerInitializer
{
  @Override
  public void initialize(Server server, Injector injector)
  {
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setResourceBase(DruidMaster.class.getClassLoader().getResource("static").toExternalForm());

    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.setContextPath("/");

    HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{resourceHandler, root, new DefaultHandler()});
    server.setHandler(handlerList);

    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    root.addFilter(GzipFilter.class, "/*", null);
    root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);
    root.addFilter(GuiceFilter.class, "/*", null);
  }
}
