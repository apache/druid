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

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.curator.CuratorModule;
import io.druid.guice.AWSModule;
import io.druid.guice.HttpClientModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.MiddleManagerModule;
import io.druid.guice.ServerModule;
import io.druid.guice.TaskLogsModule;
import io.druid.indexing.worker.WorkerTaskMonitor;
import io.druid.indexing.worker.http.WorkerResource;
import io.druid.server.StatusResource;
import io.druid.server.initialization.EmitterModule;
import io.druid.server.initialization.JettyServerInitializer;
import io.druid.server.initialization.JettyServerModule;
import io.druid.server.metrics.MetricsModule;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;

import java.util.List;

/**
 */
@Command(
    name = "middleManager",
    description = "Runs a Middle Manager, this is a \"task\" node used as part of the remote indexing service."
)
public class CliMiddleManager extends ServerRunnable
{
  private static final Logger log = new Logger(CliMiddleManager.class);

  public CliMiddleManager()
  {
    super(log);
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new LifecycleModule().register(WorkerTaskMonitor.class),
        EmitterModule.class,
        HttpClientModule.global(),
        CuratorModule.class,
        new MetricsModule(),
        new ServerModule(),
        new JettyServerModule(new MiddleManagerJettyServerInitializer())
            .addResource(StatusResource.class)
            .addResource(WorkerResource.class),
        new AWSModule(),
        new TaskLogsModule(),
        new MiddleManagerModule()
    );
  }

  private static class MiddleManagerJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(GzipFilter.class, "/*", null);
      root.addFilter(GuiceFilter.class, "/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root, new DefaultHandler()});
      server.setHandler(handlerList);
    }
  }
}
