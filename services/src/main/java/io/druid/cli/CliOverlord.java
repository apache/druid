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
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.curator.discovery.DiscoveryModule;
import com.metamx.druid.guice.AWSModule;
import com.metamx.druid.guice.DbConnectorModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.JacksonConfigManagerModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.OverlordModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.guice.TaskLogsModule;
import com.metamx.druid.http.RedirectFilter;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.indexing.coordinator.TaskMaster;
import com.metamx.druid.indexing.coordinator.http.IndexerCoordinatorResource;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerInitializer;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.metrics.MetricsModule;
import io.airlift.command.Command;
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
import org.eclipse.jetty.util.resource.ResourceCollection;

/**
 */
@Command(
    name = "overlord",
    description = "Runs an Overlord node, see https://github.com/metamx/druid/wiki/Indexing-Service for a description"
)
public class CliOverlord extends ServerRunnable
{
  private static Logger log = new Logger(CliOverlord.class);

  public CliOverlord()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
        new LifecycleModule(),
        EmitterModule.class,
        HttpClientModule.global(),
        CuratorModule.class,
        new MetricsModule(),
        ServerModule.class,
        new AWSModule(),
        new DbConnectorModule(),
        new JacksonConfigManagerModule(),
        new JettyServerModule(new OverlordJettyServerInitializer())
            .addResource(IndexerCoordinatorResource.class)
            .addResource(StatusResource.class),
        new DiscoveryModule(),
        new TaskLogsModule(),
        new OverlordModule()
    );
  }

  private static class OverlordJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      ResourceHandler resourceHandler = new ResourceHandler();
      resourceHandler.setBaseResource(
          new ResourceCollection(
              new String[]{
                  TaskMaster.class.getClassLoader().getResource("static").toExternalForm(),
                  TaskMaster.class.getClassLoader().getResource("indexer_static").toExternalForm()
              }
          )
      );

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
}
