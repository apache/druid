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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.ServerInventoryManager;
import com.metamx.druid.client.ServerInventoryManagerConfig;
import com.metamx.druid.coordination.DruidClusterInfo;
import com.metamx.druid.coordination.DruidClusterInfoConfig;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.db.DatabaseRuleManagerConfig;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.db.DatabaseSegmentManagerConfig;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.initialization.ZkClientConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterConfig;
import com.metamx.druid.master.LoadQueuePeon;
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
import com.metamx.phonebook.PhoneBook;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceProvider;
import org.I0Itec.zkclient.ZkClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;
import org.skife.jdbi.v2.DBI;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class MasterMain
{
  private static final Logger log = new Logger(ServerMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final Properties props = Initialization.loadProperties();
    final ConfigurationObjectFactory configFactory = Config.createFactory(props);
    final Lifecycle lifecycle = new Lifecycle();

    final HttpClient httpClient = HttpClientInit.createClient(
        HttpClientConfig.builder().withNumConnections(1).build(), lifecycle
    );

    final ServiceEmitter emitter = new ServiceEmitter(
        PropUtils.getProperty(props, "druid.service"),
        PropUtils.getProperty(props, "druid.host"),
        Emitters.create(props, httpClient, jsonMapper, lifecycle)
    );
    EmittingLogger.registerEmitter(emitter);

    final ZkClient zkClient = Initialization.makeZkClient(configFactory.build(ZkClientConfig.class), lifecycle);

    final PhoneBook masterYp = Initialization.createPhoneBook(jsonMapper, zkClient, "Master-ZKYP--%s", lifecycle);
    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);

    final ServerInventoryManager serverInventoryManager =
        new ServerInventoryManager(configFactory.build(ServerInventoryManagerConfig.class), masterYp);

    final DbConnectorConfig dbConnectorConfig = configFactory.build(DbConnectorConfig.class);
    final DBI dbi = new DbConnector(dbConnectorConfig).getDBI();
    DbConnector.createSegmentTable(dbi, PropUtils.getProperty(props, "druid.database.segmentTable"));
    DbConnector.createRuleTable(dbi, PropUtils.getProperty(props, "druid.database.ruleTable"));
    final DatabaseSegmentManager databaseSegmentManager = new DatabaseSegmentManager(
        jsonMapper,
        scheduledExecutorFactory.create(1, "DatabaseSegmentManager-Exec--%d"),
        configFactory.build(DatabaseSegmentManagerConfig.class),
        dbi
    );
    final DatabaseRuleManagerConfig databaseRuleManagerConfig = configFactory.build(DatabaseRuleManagerConfig.class);
    final DatabaseRuleManager databaseRuleManager = new DatabaseRuleManager(
        jsonMapper,
        scheduledExecutorFactory.create(1, "DatabaseRuleManager-Exec--%d"),
        databaseRuleManagerConfig,
        dbi
    );
    DatabaseRuleManager.createDefaultRule(
        dbi,
        databaseRuleManagerConfig.getRuleTable(),
        databaseRuleManagerConfig.getDefaultDatasource(),
        jsonMapper
    );

    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final MonitorScheduler healthMonitor = new MonitorScheduler(
        configFactory.build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        ImmutableList.<Monitor>of(
            new JvmMonitor(),
            new SysMonitor()
        )
    );
    lifecycle.addManagedInstance(healthMonitor);

    final DruidMasterConfig druidMasterConfig = configFactory.build(DruidMasterConfig.class);

    final ServiceDiscoveryConfig serviceDiscoveryConfig = configFactory.build(ServiceDiscoveryConfig.class);
    CuratorFramework curatorFramework = Initialization.makeCuratorFrameworkClient(
        serviceDiscoveryConfig,
        lifecycle
    );

    final ServiceDiscovery serviceDiscovery = Initialization.makeServiceDiscoveryClient(
        curatorFramework,
        serviceDiscoveryConfig,
        lifecycle
    );

    ServiceProvider serviceProvider = null;
    if (druidMasterConfig.getMergerServiceName() != null) {
      serviceProvider = Initialization.makeServiceProvider(
          druidMasterConfig.getMergerServiceName(),
          serviceDiscovery,
          lifecycle
      );
    }

    final DruidClusterInfo druidClusterInfo = new DruidClusterInfo(
        configFactory.build(DruidClusterInfoConfig.class),
        masterYp
    );

    final DruidMaster master = new DruidMaster(
        druidMasterConfig,
        druidClusterInfo,
        jsonMapper,
        databaseSegmentManager,
        serverInventoryManager,
        databaseRuleManager,
        masterYp,
        emitter,
        scheduledExecutorFactory,
        new ConcurrentHashMap<String, LoadQueuePeon>(),
        serviceProvider
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
            serverInventoryManager,
            databaseSegmentManager,
            databaseRuleManager,
            druidClusterInfo,
            master,
            jsonMapper
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
          return (queryString == null) ?
                 new URL(String.format("http://%s%s", druidClusterInfo.getMasterHost(), requestURI)) :
                 new URL(String.format("http://%s%s?%s", druidClusterInfo.getMasterHost(), requestURI, queryString));
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
