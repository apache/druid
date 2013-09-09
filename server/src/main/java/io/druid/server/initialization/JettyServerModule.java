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

package io.druid.server.initialization;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.WebConfig;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.JSR311Resource;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.servlet.ServletException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class JettyServerModule extends JerseyServletModule
{
  private static final Logger log = new Logger(JettyServerModule.class);

  private final JettyServerInitializer initializer;
  private final List<Class<?>> resources = Lists.newArrayList();

  public JettyServerModule(
      JettyServerInitializer initializer
  )
  {
    this.initializer = initializer;
  }

  public JettyServerModule addResource(Class<?> resource)
  {
    resources.add(resource);
    return this;
  }

  @Override
  protected void configureServlets()
  {
    Binder binder = binder();

    JsonConfigProvider.bind(binder, "druid.server.http", ServerConfig.class);

    binder.bind(GuiceContainer.class).to(DruidGuiceContainer.class);
    binder.bind(DruidGuiceContainer.class).in(Scopes.SINGLETON);
    serve("/*").with(DruidGuiceContainer.class);

    for (Class<?> resource : resources) {
      Jerseys.addResource(binder, resource);
      binder.bind(resource).in(LazySingleton.class);
    }

    binder.bind(Key.get(Server.class, Names.named("ForTheEagerness"))).to(Server.class).asEagerSingleton();
  }

  public static class DruidGuiceContainer extends GuiceContainer
  {
    private final Set<Class<?>> resources;

    @Inject
    public DruidGuiceContainer(
        Injector injector,
        @JSR311Resource Set<Class<?>> resources
    )
    {
      super(injector);
      this.resources = resources;
    }

    @Override
    protected ResourceConfig getDefaultResourceConfig(
        Map<String, Object> props, WebConfig webConfig
    ) throws ServletException
    {
      return new DefaultResourceConfig(resources);
    }
  }

  @Provides @LazySingleton
  public Server getServer(Injector injector, Lifecycle lifecycle, @Self DruidNode node, ServerConfig config)
  {
    final Server server = makeJettyServer(node, config);
    try {
      initializer.initialize(server, injector);
    }
    catch (ConfigurationException e) {
      throw new ProvisionException(Iterables.getFirst(e.getErrorMessages(), null).getMessage());
    }


    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            server.start();
          }

          @Override
          public void stop()
          {
            try {
              server.stop();
            }
            catch (Exception e) {
              log.warn(e, "Unable to stop Jetty server.");
            }
          }
        }
    );
    return server;
  }

  private static Server makeJettyServer(@Self DruidNode node, ServerConfig config)
  {
    final QueuedThreadPool threadPool = new QueuedThreadPool();
    threadPool.setMinThreads(config.getNumThreads());
    threadPool.setMaxThreads(config.getNumThreads());

    final Server server = new Server();
    server.setThreadPool(threadPool);

    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setPort(node.getPort());
    connector.setMaxIdleTime(Ints.checkedCast(config.getMaxIdleTime().toStandardDuration().getMillis()));
    connector.setStatsOn(true);

    server.setConnectors(new Connector[]{connector});

    return server;
  }
}
