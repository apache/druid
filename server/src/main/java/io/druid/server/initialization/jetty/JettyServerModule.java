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

package io.druid.server.initialization.jetty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
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
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;
import io.druid.server.StatusResource;
import io.druid.server.initialization.ServerConfig;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;

import javax.servlet.ServletException;
import java.util.Map;
import java.util.Set;

/**
 */
public class JettyServerModule extends JerseyServletModule
{
  private static final Logger log = new Logger(JettyServerModule.class);

  @Override
  protected void configureServlets()
  {
    Binder binder = binder();

    JsonConfigProvider.bind(binder, "druid.server.http", ServerConfig.class);

    binder.bind(GuiceContainer.class).to(DruidGuiceContainer.class);
    binder.bind(DruidGuiceContainer.class).in(Scopes.SINGLETON);
    binder.bind(CustomExceptionMapper.class).in(Singleton.class);

    serve("/*").with(DruidGuiceContainer.class);

    Jerseys.addResource(binder, StatusResource.class);
    binder.bind(StatusResource.class).in(LazySingleton.class);

    //Adding empty binding for ServletFilterHolders so that injector returns
    //an empty set when no external modules provide ServletFilterHolder impls
    Multibinder.newSetBinder(binder, ServletFilterHolder.class);
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

  @Provides
  @LazySingleton
  public Server getServer(Injector injector, Lifecycle lifecycle, @Self DruidNode node, ServerConfig config)
  {
    final Server server = makeJettyServer(node, config);
    initializeServer(injector, lifecycle, server);
    return server;
  }

  @Provides
  @Singleton
  public JacksonJsonProvider getJacksonJsonProvider(@Json ObjectMapper objectMapper)
  {
    final JacksonJsonProvider provider = new JacksonJsonProvider();
    provider.setMapper(objectMapper);
    return provider;
  }

  static Server makeJettyServer(DruidNode node, ServerConfig config)
  {
    final QueuedThreadPool threadPool = new QueuedThreadPool();
    threadPool.setMinThreads(config.getNumThreads());
    threadPool.setMaxThreads(config.getNumThreads());
    threadPool.setDaemon(true);

    final Server server = new Server(threadPool);

    // Without this bean set, the default ScheduledExecutorScheduler runs as non-daemon, causing lifecycle hooks to fail
    // to fire on main exit. Related bug: https://github.com/druid-io/druid/pull/1627
    server.addBean(new ScheduledExecutorScheduler("JettyScheduler", true), true);

    ServerConnector connector = new ServerConnector(server);
    connector.setPort(node.getPort());
    connector.setIdleTimeout(Ints.checkedCast(config.getMaxIdleTime().toStandardDuration().getMillis()));
    // workaround suggested in -
    // https://bugs.eclipse.org/bugs/show_bug.cgi?id=435322#c66 for jetty half open connection issues during failovers
    connector.setAcceptorPriorityDelta(-1);

    server.setConnectors(new Connector[]{connector});

    return server;
  }

  static void initializeServer(Injector injector, Lifecycle lifecycle, final Server server)
  {
    JettyServerInitializer initializer = injector.getInstance(JettyServerInitializer.class);
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
  }

}
