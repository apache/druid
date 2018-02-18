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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
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
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.java.util.metrics.AbstractMonitor;
import io.druid.java.util.metrics.MonitorUtils;
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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.servlet.ServletException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
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
    binder.bind(ForbiddenExceptionMapper.class).in(Singleton.class);

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
    return makeAndInitializeServer(
        injector,
        lifecycle,
        node,
        config,
        TLSServerConfig,
        injector.getExistingBinding(Key.get(SslContextFactory.class))
    );
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

  static Server makeAndInitializeServer(
      Injector injector,
      Lifecycle lifecycle,
      DruidNode node,
      ServerConfig config,
      TLSServerConfig tlsServerConfig,
      Binding<SslContextFactory> sslContextFactoryBinding
  )
  {
    // adjusting to make config.getNumThreads() mean, "number of threads
    // that concurrently handle the requests".
    int numServerThreads = config.getNumThreads() + getMaxJettyAcceptorsSelectorsNum(node);

    final QueuedThreadPool threadPool;
    if (config.getQueueSize() == Integer.MAX_VALUE) {
      threadPool = new QueuedThreadPool();
      threadPool.setMinThreads(numServerThreads);
      threadPool.setMaxThreads(numServerThreads);
    } else {
      threadPool = new QueuedThreadPool(
          numServerThreads,
          numServerThreads,
          60000, // same default is used in other case when threadPool = new QueuedThreadPool()
          new LinkedBlockingQueue<>(config.getQueueSize())
      );
    }

    threadPool.setDaemon(true);

    final Server server = new Server(threadPool);

    // Without this bean set, the default ScheduledExecutorScheduler runs as non-daemon, causing lifecycle hooks to fail
    // to fire on main exit. Related bug: https://github.com/druid-io/druid/pull/1627
    server.addBean(new ScheduledExecutorScheduler("JettyScheduler", true), true);

    final List<ServerConnector> serverConnectors = new ArrayList<>();

    if (node.isEnablePlaintextPort()) {
      log.info("Creating http connector with port [%d]", node.getPlaintextPort());
      HttpConfiguration httpConfiguration = new HttpConfiguration();
      httpConfiguration.setRequestHeaderSize(config.getMaxRequestHeaderSize());
      final ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(httpConfiguration));
      connector.setPort(node.getPlaintextPort());
      serverConnectors.add(connector);
    }

    final SslContextFactory sslContextFactory;
    if (node.isEnableTlsPort()) {
      log.info("Creating https connector with port [%d]", node.getTlsPort());
      if (sslContextFactoryBinding == null) {
        // Never trust all certificates by default
        sslContextFactory = new SslContextFactory(false);
        sslContextFactory.setKeyStorePath(tlsServerConfig.getKeyStorePath());
        sslContextFactory.setKeyStoreType(tlsServerConfig.getKeyStoreType());
        sslContextFactory.setKeyStorePassword(tlsServerConfig.getKeyStorePasswordProvider().getPassword());
        sslContextFactory.setCertAlias(tlsServerConfig.getCertAlias());
        sslContextFactory.setKeyManagerFactoryAlgorithm(tlsServerConfig.getKeyManagerFactoryAlgorithm() == null
                                                        ? KeyManagerFactory.getDefaultAlgorithm()
                                                        : tlsServerConfig.getKeyManagerFactoryAlgorithm());
        sslContextFactory.setKeyManagerPassword(tlsServerConfig.getKeyManagerPasswordProvider() == null ?
                                                null : tlsServerConfig.getKeyManagerPasswordProvider().getPassword());
        if (tlsServerConfig.getIncludeCipherSuites() != null) {
          sslContextFactory.setIncludeCipherSuites(
              tlsServerConfig.getIncludeCipherSuites()
                             .toArray(new String[tlsServerConfig.getIncludeCipherSuites().size()]));
        }
        if (tlsServerConfig.getExcludeCipherSuites() != null) {
          sslContextFactory.setExcludeCipherSuites(
              tlsServerConfig.getExcludeCipherSuites()
                             .toArray(new String[tlsServerConfig.getExcludeCipherSuites().size()]));
        }
        if (tlsServerConfig.getIncludeProtocols() != null) {
          sslContextFactory.setIncludeProtocols(
              tlsServerConfig.getIncludeProtocols().toArray(new String[tlsServerConfig.getIncludeProtocols().size()]));
        }
        if (tlsServerConfig.getExcludeProtocols() != null) {
          sslContextFactory.setExcludeProtocols(
              tlsServerConfig.getExcludeProtocols().toArray(new String[tlsServerConfig.getExcludeProtocols().size()]));
        }
      } else {
        sslContextFactory = sslContextFactoryBinding.getProvider().get();
      }

      final HttpConfiguration httpsConfiguration = new HttpConfiguration();
      httpsConfiguration.setSecureScheme("https");
      httpsConfiguration.setSecurePort(node.getTlsPort());
      httpsConfiguration.addCustomizer(new SecureRequestCustomizer());
      httpsConfiguration.setRequestHeaderSize(config.getMaxRequestHeaderSize());
      final ServerConnector connector = new ServerConnector(
          server,
          new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.toString()),
          new HttpConnectionFactory(httpsConfiguration)
      );
      connector.setPort(node.getTlsPort());
      serverConnectors.add(connector);
    } else {
      sslContextFactory = null;
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

    // initialize server
    JettyServerInitializer initializer = injector.getInstance(JettyServerInitializer.class);
    try {
      initializer.initialize(server, injector);
    }
    catch (Exception e) {
      throw new RE(e, "server initialization exception");
    }

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            log.info("Starting Jetty Server...");
            server.start();
            if (node.isEnableTlsPort()) {
              // Perform validation
              Preconditions.checkNotNull(sslContextFactory);
              final SSLEngine sslEngine = sslContextFactory.newSSLEngine();
              if (sslEngine.getEnabledCipherSuites() == null || sslEngine.getEnabledCipherSuites().length == 0) {
                throw new ISE(
                    "No supported cipher suites found, supported suites [%s], configured suites include list: [%s] exclude list: [%s]",
                    Arrays.toString(sslEngine.getSupportedCipherSuites()),
                    tlsServerConfig.getIncludeCipherSuites(),
                    tlsServerConfig.getExcludeCipherSuites()
                );
              }
              if (sslEngine.getEnabledProtocols() == null || sslEngine.getEnabledProtocols().length == 0) {
                throw new ISE(
                    "No supported protocols found, supported protocols [%s], configured protocols include list: [%s] exclude list: [%s]",
                    Arrays.toString(sslEngine.getSupportedProtocols()),
                    tlsServerConfig.getIncludeProtocols(),
                    tlsServerConfig.getExcludeProtocols()
                );
              }
            }
          }

          @Override
          public void stop()
          {
            try {
              log.info("Stopping Jetty Server...");
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

  private static int getMaxJettyAcceptorsSelectorsNum(DruidNode druidNode)
  {
    // This computation is based on Jetty v9.3.19 which uses upto 8(4 acceptors and 4 selectors) threads per
    // ServerConnector
    int numServerConnector = (druidNode.isEnablePlaintextPort() ? 1 : 0) + (druidNode.isEnableTlsPort() ? 1 : 0);
    return numServerConnector * 8;
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
