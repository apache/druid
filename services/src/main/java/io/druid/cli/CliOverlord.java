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
import io.airlift.command.Command;
import io.druid.curator.CuratorModule;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.AWSModule;
import io.druid.guice.DbConnectorModule;
import io.druid.guice.HttpClientModule;
import io.druid.guice.JacksonConfigManagerModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.OverlordModule;
import io.druid.guice.ServerModule;
import io.druid.guice.TaskLogsModule;
import io.druid.indexing.coordinator.TaskMaster;
import io.druid.indexing.coordinator.http.IndexerCoordinatorResource;
import io.druid.server.StatusResource;
import io.druid.server.http.RedirectFilter;
import io.druid.server.initialization.EmitterModule;
import io.druid.server.initialization.Initialization;
import io.druid.server.initialization.JettyServerInitializer;
import io.druid.server.initialization.JettyServerModule;
import io.druid.server.metrics.MetricsModule;
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
        new ServerModule(),
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
