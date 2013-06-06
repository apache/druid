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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.ServerInventoryView;
import com.metamx.druid.client.ServerInventoryViewConfig;
import com.metamx.druid.client.indexing.IndexingServiceClient;
import com.metamx.druid.concurrent.Execs;
import com.metamx.druid.config.ConfigManager;
import com.metamx.druid.config.ConfigManagerConfig;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.db.DatabaseRuleManagerConfig;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.db.DatabaseSegmentManagerConfig;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.initialization.ZkPathsConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterConfig;
import com.metamx.druid.master.LoadQueueTaskMaster;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Emitters;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.response.ToStringResponseHandler;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import com.metamx.metrics.MonitorSchedulerConfig;
import com.metamx.metrics.SysMonitor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceProvider;
import org.joda.time.Duration;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.servlet.GzipFilter;
import org.skife.config.ConfigurationObjectFactory;
import org.skife.jdbi.v2.DBI;

import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class MasterMain
{
  private static final Logger log = new Logger(MasterMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final Properties props = Initialization.loadProperties();
    final ConfigurationObjectFactory configFactory = Config.createFactory(props);
    final Lifecycle lifecycle = new Lifecycle();

    final HttpClientConfig.Builder httpClientConfigBuilder = HttpClientConfig.builder().withNumConnections(1);

    final String emitterTimeout = props.getProperty("druid.emitter.timeOut");
    if (emitterTimeout != null) {
      httpClientConfigBuilder.withReadTimeout(new Duration(emitterTimeout));
    }
    final HttpClient httpClient = HttpClientInit.createClient(httpClientConfigBuilder.build(), lifecycle);

    final ServiceEmitter emitter = new ServiceEmitter(
        PropUtils.getProperty(props, "druid.service"),
        PropUtils.getProperty(props, "druid.host"),
        Emitters.create(props, httpClient, jsonMapper, lifecycle)
    );
    EmittingLogger.registerEmitter(emitter);

    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);

    final ServiceDiscoveryConfig serviceDiscoveryConfig = configFactory.build(ServiceDiscoveryConfig.class);
    CuratorFramework curatorFramework = Initialization.makeCuratorFramework(
        serviceDiscoveryConfig,
        lifecycle
    );

    final ZkPathsConfig zkPaths = configFactory.build(ZkPathsConfig.class);

    final ExecutorService exec = Executors.newFixedThreadPool(
        1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ServerInventoryView-%s").build()
    );
    ServerInventoryView serverInventoryView = new ServerInventoryView(
        configFactory.build(ServerInventoryViewConfig.class), zkPaths, curatorFramework, exec, jsonMapper
    );
    lifecycle.addManagedInstance(serverInventoryView);

    final DbConnectorConfig dbConnectorConfig = configFactory.build(DbConnectorConfig.class);
    final DatabaseRuleManagerConfig databaseRuleManagerConfig = configFactory.build(DatabaseRuleManagerConfig.class);
    final DBI dbi = new DbConnector(dbConnectorConfig).getDBI();
    DbConnector.createSegmentTable(dbi, PropUtils.getProperty(props, "druid.database.segmentTable"));
    DbConnector.createRuleTable(dbi, PropUtils.getProperty(props, "druid.database.ruleTable"));
    DatabaseRuleManager.createDefaultRule(
        dbi, databaseRuleManagerConfig.getRuleTable(), databaseRuleManagerConfig.getDefaultDatasource(), jsonMapper
    );

    final DatabaseSegmentManager databaseSegmentManager = new DatabaseSegmentManager(
        jsonMapper,
        scheduledExecutorFactory.create(1, "DatabaseSegmentManager-Exec--%d"),
        configFactory.build(DatabaseSegmentManagerConfig.class),
        dbi
    );
    final DatabaseRuleManager databaseRuleManager = new DatabaseRuleManager(
        jsonMapper,
        scheduledExecutorFactory.create(1, "DatabaseRuleManager-Exec--%d"),
        databaseRuleManagerConfig,
        dbi
    );

    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final List<Monitor> monitors = Lists.newArrayList();
    monitors.add(new JvmMonitor());
    if (Boolean.parseBoolean(props.getProperty("druid.monitoring.monitorSystem", "false"))) {
      monitors.add(new SysMonitor());
    }

    final MonitorScheduler healthMonitor = new MonitorScheduler(
        configFactory.build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        monitors
    );
    lifecycle.addManagedInstance(healthMonitor);

    final DruidMasterConfig druidMasterConfig = configFactory.build(DruidMasterConfig.class);

    final ServiceDiscovery serviceDiscovery = Initialization.makeServiceDiscoveryClient(
        curatorFramework,
        serviceDiscoveryConfig,
        lifecycle
    );
    final ServiceAnnouncer serviceAnnouncer = Initialization.makeServiceAnnouncer(
        serviceDiscoveryConfig, serviceDiscovery
    );
    Initialization.announceDefaultService(serviceDiscoveryConfig, serviceAnnouncer, lifecycle);

    ServiceProvider serviceProvider = null;
    if (druidMasterConfig.getMergerServiceName() != null) {
      serviceProvider = Initialization.makeServiceProvider(
          druidMasterConfig.getMergerServiceName(),
          serviceDiscovery,
          lifecycle
      );
    }
    IndexingServiceClient indexingServiceClient = new IndexingServiceClient(httpClient, jsonMapper, serviceProvider);

    final ConfigManagerConfig configManagerConfig = configFactory.build(ConfigManagerConfig.class);
    DbConnector.createConfigTable(dbi, configManagerConfig.getConfigTable());
    JacksonConfigManager configManager = new JacksonConfigManager(
        new ConfigManager(dbi, configManagerConfig), jsonMapper
    );

    final LoadQueueTaskMaster taskMaster = new LoadQueueTaskMaster(
        curatorFramework, jsonMapper, Execs.singleThreaded("Master-PeonExec--%d")
    );

    final DruidMaster master = new DruidMaster(
        druidMasterConfig,
        zkPaths,
        configManager,
        databaseSegmentManager,
        serverInventoryView,
        databaseRuleManager,
        curatorFramework,
        emitter,
        scheduledExecutorFactory,
        indexingServiceClient,
        taskMaster
    );
    lifecycle.addManagedInstance(master);

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

    final Injector injector = Guice.createInjector(
        new MasterServletModule(
            serverInventoryView,
            databaseSegmentManager,
            databaseRuleManager,
            master,
            jsonMapper,
            indexingServiceClient
        )
    );

    final Server server = Initialization.makeJettyServer(configFactory.build(ServerConfig.class));

    final RedirectInfo redirectInfo = new RedirectInfo()
    {
      @Override
      public boolean doLocal()
      {
        return master.isClusterMaster();
      }

      @Override
      public URL getRedirectURL(String queryString, String requestURI)
      {
        try {
          final String currentMaster = master.getCurrentMaster();
          if (currentMaster == null) {
            return null;
          }

          String location = String.format("http://%s%s", currentMaster, requestURI);

          if (queryString != null) {
            location = String.format("%s?%s", location, queryString);
          }

          return new URL(location);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };

    final Context staticContext = new Context(server, "/static", Context.SESSIONS);
    staticContext.addServlet(new ServletHolder(new RedirectServlet(redirectInfo)), "/*");

    staticContext.setResourceBase(ComputeMain.class.getClassLoader().getResource("static").toExternalForm());

    final Context root = new Context(server, "/", Context.SESSIONS);
    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    root.addEventListener(new GuiceServletConfig(injector));
    root.addFilter(GzipFilter.class, "/*", 0);
    root.addFilter(
        new FilterHolder(
            new RedirectFilter(
                new ToStringResponseHandler(Charsets.UTF_8),
                redirectInfo
            )
        ), "/*", 0
    );
    root.addFilter(GuiceFilter.class, "/info/*", 0);
    root.addFilter(GuiceFilter.class, "/master/*", 0);

    server.start();
    server.join();
  }
}
