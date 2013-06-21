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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.curator.discovery.DiscoveryModule;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.MasterModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.initialization.DruidNodeConfig;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerInitializer;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.metrics.MetricsModule;
import com.metamx.metrics.MonitorScheduler;
import org.eclipse.jetty.server.Server;
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
        new LifecycleModule(Key.get(MonitorScheduler.class), Key.get(DruidMaster.class)),
        EmitterModule.class,
        HttpClientModule.class,
        CuratorModule.class,
        new MetricsModule(),
        DiscoveryModule.class,
        ServerModule.class,
        new JettyServerModule(new MasterJettyServerInitializer()),
        MasterModule.class
    );

    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    final DruidNodeConfig nodeConfig = injector.getInstance(DruidNodeConfig.class);

    final ServiceAnnouncer serviceAnnouncer = injector.getInstance(ServiceAnnouncer.class);

    try {
      Initialization.announceDefaultService(nodeConfig, serviceAnnouncer, lifecycle);
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
      final ServletContextHandler staticContext = new ServletContextHandler(server, "/static", ServletContextHandler.SESSIONS);
      staticContext.addServlet(new ServletHolder(injector.getInstance(RedirectServlet.class)), "/*");

      staticContext.setResourceBase(ComputeMain.class.getClassLoader().getResource("static").toExternalForm());

      final ServletContextHandler root = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new StatusServlet()), "/status");
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addEventListener(new GuiceServletConfig(injector));
      root.addFilter(GzipFilter.class, "/*", null);
      root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);
      root.addFilter(GuiceFilter.class, "/info/*", null);
      root.addFilter(GuiceFilter.class, "/master/*", null);
    }
  }
}
