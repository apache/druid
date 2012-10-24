package com.metamx.druid.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.CachingClusteredClient;
import com.metamx.druid.client.ClientConfig;
import com.metamx.druid.client.ClientInventoryManager;
import com.metamx.druid.client.ClientSideServerView;
import com.metamx.druid.client.cache.CacheBroker;
import com.metamx.druid.client.cache.CacheMonitor;
import com.metamx.druid.client.cache.MapCacheBroker;
import com.metamx.druid.client.cache.MapCacheBrokerConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.initialization.ZkClientConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.druid.query.ReflectionQueryToolChestWarehouse;
import com.metamx.emitter.core.Emitters;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import com.metamx.metrics.MonitorSchedulerConfig;
import com.metamx.metrics.SysMonitor;
import com.metamx.phonebook.PhoneBook;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import org.I0Itec.zkclient.ZkClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 */

public class BrokerMain
{
  private static final Logger log = new Logger(BrokerMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final ObjectMapper smileMapper = new DefaultObjectMapper(new SmileFactory());
    smileMapper.getJsonFactory().setCodec(smileMapper);

    final Properties props = Initialization.loadProperties();
    final Lifecycle lifecycle = new Lifecycle();
    final ConfigurationObjectFactory configFactory = Config.createFactory(props);
    final ZkClient zkClient = Initialization.makeZkClient(configFactory.build(ZkClientConfig.class), lifecycle);
    final PhoneBook phoneBook = Initialization.createYellowPages(
        jsonMapper, zkClient, "Client-ZKYP--%s", lifecycle
    );

    final HttpClient httpClient = HttpClientInit.createClient(
        HttpClientConfig.builder()
                        .withNumConnections(
                            Integer.parseInt(props.getProperty("druid.client.http.connections"))
                        )
                        .build(),
        lifecycle
    );
    final HttpClient emitterHttpClient = HttpClientInit.createClient(
        HttpClientConfig.builder().withNumConnections(1).build(), lifecycle
    );
    final ServiceEmitter emitter = new ServiceEmitter(
        props.getProperty("druid.service"),
        props.getProperty("druid.host"),
        Emitters.create(props, emitterHttpClient, jsonMapper, lifecycle)
    );

    final QueryToolChestWarehouse warehouse = new ReflectionQueryToolChestWarehouse();
    final ClientConfig clientConfig = configFactory.build(ClientConfig.class);
    final ClientSideServerView view = new ClientSideServerView(warehouse, smileMapper, httpClient);
    final ClientInventoryManager clientInventoryManager = new ClientInventoryManager(
        clientConfig.getClientInventoryManagerConfig(),
        phoneBook,
        view
    );
    lifecycle.addManagedInstance(clientInventoryManager);

    final CacheBroker cacheBroker = MapCacheBroker.create(
        configFactory.buildWithReplacements(MapCacheBrokerConfig.class, ImmutableMap.of("prefix", "druid.bard.cache"))
    );
    final CachingClusteredClient baseClient = new CachingClusteredClient(warehouse, view, cacheBroker, smileMapper);
    lifecycle.addManagedInstance(baseClient);

    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);
    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final MonitorScheduler monitorScheduler = new MonitorScheduler(
        configFactory.build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        ImmutableList.<Monitor>of(
            new JvmMonitor(),
            new SysMonitor(),
            new CacheMonitor(cacheBroker)
        )
    );
    lifecycle.addManagedInstance(monitorScheduler);

    final ServiceDiscoveryConfig serviceDiscoveryConfig = configFactory.build(ServiceDiscoveryConfig.class);
    CuratorFramework curatorFramework = Initialization.makeCuratorFrameworkClient(
        serviceDiscoveryConfig.getZkHosts(),
        lifecycle
    );

    final ServiceDiscovery serviceDiscovery = Initialization.makeServiceDiscoveryClient(
        curatorFramework,
        configFactory.build(ServiceDiscoveryConfig.class),
        lifecycle
    );

    final RequestLogger requestLogger = Initialization.makeRequestLogger(
        scheduledExecutorFactory.create(
            1,
            "RequestLogger--%d"
        ),
        props
    );
    lifecycle.addManagedInstance(requestLogger);

    final ClientQuerySegmentWalker texasRanger = new ClientQuerySegmentWalker(warehouse, emitter, baseClient);

    final Injector injector = Guice.createInjector(new ClientServletModule(texasRanger, clientInventoryManager, jsonMapper));
    final Server server = Initialization.makeJettyServer(configFactory.build(ServerConfig.class));
    final Context root = new Context(server, "/druid/v2", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(
        new ServletHolder(new QueryServlet(jsonMapper, smileMapper, texasRanger, emitter, requestLogger)),
        "/*"
    );

    root.addEventListener(new GuiceServletConfig(injector));
    root.addFilter(GuiceFilter.class, "/heatmap/*", 0);
    root.addFilter(GuiceFilter.class, "/datasources/*", 0);

    try {
      lifecycle.start();
    }
    catch (Throwable t) {
      log.error(t, "Error when starting up.  Failing.");
      System.exit(1);
    }

    Runtime.getRuntime().addShutdownHook(
        new Thread(
            new Runnable()
            {
              @Override
              public void run()
              {
                log.info("Running shutdown hook");
                lifecycle.stop();
              }
            }
        )
    );

    server.start();
    server.join();
  }
}
