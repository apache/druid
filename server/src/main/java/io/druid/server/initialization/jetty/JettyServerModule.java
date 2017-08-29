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
import com.fasterxml.jackson.jaxrs.smile.JacksonSmileProvider;
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
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;
import com.metamx.metrics.MonitorUtils;
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
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import io.druid.server.StatusResource;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.initialization.TLSServerConfig;
import io.druid.server.metrics.DataSourceTaskIdHolder;
import io.druid.server.metrics.MetricsModule;
import io.druid.server.metrics.MonitorsConfig;
import org.apache.http.HttpVersion;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;

import javax.servlet.ServletException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class JettyServerModule extends JerseyServletModule
{
  private static final Logger log = new Logger(JettyServerModule.class);

  private static final AtomicInteger activeConnections = new AtomicInteger();

  @Override
  protected void configureServlets()
  {
    Binder binder = binder();

    JsonConfigProvider.bind(binder, "druid.server.http", ServerConfig.class);
    JsonConfigProvider.bind(binder, "druid.server.https", TLSServerConfig.class);

    binder.bind(GuiceContainer.class).to(DruidGuiceContainer.class);
    binder.bind(DruidGuiceContainer.class).in(Scopes.SINGLETON);
    binder.bind(CustomExceptionMapper.class).in(Singleton.class);

    serve("/*").with(DruidGuiceContainer.class);

    Jerseys.addResource(binder, StatusResource.class);
    binder.bind(StatusResource.class).in(LazySingleton.class);

    // Adding empty binding for ServletFilterHolders and Handlers so that injector returns an empty set if none
    // are provided by extensions.
    Multibinder.newSetBinder(binder, Handler.class);
    Multibinder.newSetBinder(binder, ServletFilterHolder.class);

    MetricsModule.register(binder, JettyMonitor.class);
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
  public Server getServer(
      final Injector injector,
      final Lifecycle lifecycle,
      @Self final DruidNode node,
      final ServerConfig config,
      final TLSServerConfig TLSServerConfig
  )
  {
    final Server server = makeJettyServer(node, config, TLSServerConfig);
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

  @Provides
  @Singleton
  public JacksonSmileProvider getJacksonSmileProvider(@Smile ObjectMapper objectMapper)
  {
    final JacksonSmileProvider provider = new JacksonSmileProvider();
    provider.setMapper(objectMapper);
    return provider;
  }

  static Server makeJettyServer(DruidNode node, ServerConfig config, TLSServerConfig tlsServerConfig)
  {
    final QueuedThreadPool threadPool = new QueuedThreadPool();
    threadPool.setMinThreads(config.getNumThreads());
    threadPool.setMaxThreads(config.getNumThreads());
    threadPool.setDaemon(true);

    final Server server = new Server(threadPool);

    // Without this bean set, the default ScheduledExecutorScheduler runs as non-daemon, causing lifecycle hooks to fail
    // to fire on main exit. Related bug: https://github.com/druid-io/druid/pull/1627
    server.addBean(new ScheduledExecutorScheduler("JettyScheduler", true), true);

    final List<ServerConnector> serverConnectors = new ArrayList<>();

    if (config.isPlaintext()) {
      log.info("Creating http connector with port [%d]", node.getPlaintextPort());
      final ServerConnector connector = new ServerConnector(server);
      connector.setPort(node.getPlaintextPort());
      serverConnectors.add(connector);
    }
    if (config.isTls()) {
      log.info("Creating https connector with port [%d]", node.getTlsPort());
      final SslContextFactory sslContextFactory = new SslContextFactory(tlsServerConfig.getKeyStorePath());
      sslContextFactory.setKeyStoreType(tlsServerConfig.getKeyStoreType());
      sslContextFactory.setKeyStorePassword(tlsServerConfig.getKeyStorePasswordProvider().getPassword());
      sslContextFactory.setCertAlias(tlsServerConfig.getCertAlias());
      sslContextFactory.setKeyManagerPassword(tlsServerConfig.getKeyManagerPasswordProvider() == null
                                              ? null
                                              : tlsServerConfig.getKeyManagerPasswordProvider().getPassword());
      final HttpConfiguration httpsConfiguration = new HttpConfiguration();
      httpsConfiguration.setSecureScheme("https");
      httpsConfiguration.setSecurePort(node.getTlsPort());
      httpsConfiguration.addCustomizer(new SecureRequestCustomizer());
      final ServerConnector connector = new ServerConnector(
          server,
          new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.toString()),
          new HttpConnectionFactory(httpsConfiguration)
      );
      connector.setPort(node.getTlsPort());
      serverConnectors.add(connector);
    }

    final ServerConnector[] connectors = new ServerConnector[serverConnectors.size()];
    int index = 0;
    for (ServerConnector connector : serverConnectors) {
      connectors[index++] = connector;
      connector.setIdleTimeout(Ints.checkedCast(config.getMaxIdleTime().toStandardDuration().getMillis()));
      // workaround suggested in -
      // https://bugs.eclipse.org/bugs/show_bug.cgi?id=435322#c66 for jetty half open connection issues during failovers
      connector.setAcceptorPriorityDelta(-1);

      List<ConnectionFactory> monitoredConnFactories = new ArrayList<>();
      for (ConnectionFactory cf : connector.getConnectionFactories()) {
        monitoredConnFactories.add(new JettyMonitoringConnectionFactory(cf, activeConnections));
      }
      connector.setConnectionFactories(monitoredConnFactories);
    }

    server.setConnectors(connectors);

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

  @Provides
  @Singleton
  public JettyMonitor getJettyMonitor(
      DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    return new JettyMonitor(dataSourceTaskIdHolder.getDataSource(), dataSourceTaskIdHolder.getTaskId());
  }

  public static class JettyMonitor extends AbstractMonitor
  {
    private final Map<String, String[]> dimensions;

    public JettyMonitor(String dataSource, String taskId)
    {
      this.dimensions = MonitorsConfig.mapOfDatasourceAndTaskID(dataSource, taskId);
    }

    @Override
    public boolean doMonitor(ServiceEmitter emitter)
    {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
      MonitorUtils.addDimensionsToBuilder(
          builder, dimensions
      );
      emitter.emit(builder.build("jetty/numOpenConnections", activeConnections.get()));
      return true;
    }
  }
}
