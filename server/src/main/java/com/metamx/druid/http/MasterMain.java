/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.http;

import com.google.common.collect.Iterables;
import com.google.inject.ConfigurationException;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.curator.discovery.DiscoveryModule;
import com.metamx.druid.guice.DbConnectorModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.JacksonConfigManagerModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.MasterModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.guice.annotations.Self;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerInitializer;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.metrics.MetricsModule;
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
public class MasterMain
{
  private static final Logger log = new Logger(MasterMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    Injector injector = Initialization.makeInjector(
        new LifecycleModule().register(DruidMaster.class),
        EmitterModule.class,
        HttpClientModule.class,
        DbConnectorModule.class,
        JacksonConfigManagerModule.class,
        CuratorModule.class,
        new MetricsModule(),
        new DiscoveryModule().register(Self.class),
        ServerModule.class,
        new JettyServerModule(new MasterJettyServerInitializer())
            .addResource(InfoResource.class)
            .addResource(MasterResource.class)
            .addResource(StatusResource.class),
        MasterModule.class
    );

    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    try {
      lifecycle.start();
    }
    catch (Throwable t) {
      log.error(t, "Error when starting up.  Failing.");
      System.exit(1);
    }

    lifecycle.join();
  }

  private static class MasterJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      try {
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setResourceBase(MasterMain.class.getClassLoader().getResource("static").toExternalForm());

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
      catch (ConfigurationException e) {
        throw new ProvisionException(Iterables.getFirst(e.getErrorMessages(), null).getMessage());
      }
    }
  }
}
