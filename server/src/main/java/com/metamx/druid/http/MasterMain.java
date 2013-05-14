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
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.ServerInventoryView;
import com.metamx.druid.client.indexing.IndexingServiceClient;
import com.metamx.druid.concurrent.Execs;
import com.metamx.druid.config.ConfigManager;
import com.metamx.druid.config.ConfigManagerConfig;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.curator.discovery.DiscoveryModule;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.guice.DruidGuiceExtensions;
import com.metamx.druid.guice.DruidSecondaryModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.MasterModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.initialization.ConfigFactoryModule;
import com.metamx.druid.initialization.DruidNodeConfig;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.PropertiesModule;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ZkPathsConfig;
import com.metamx.druid.jackson.JacksonModule;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterConfig;
import com.metamx.druid.master.LoadQueueTaskMaster;
import com.metamx.druid.metrics.MetricsModule;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.response.ToStringResponseHandler;
import com.metamx.metrics.MonitorScheduler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceProvider;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;
import org.skife.jdbi.v2.DBI;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.Arrays;

/**
 */
public class MasterMain
{
  private static final Logger log = new Logger(MasterMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    Injector injector = makeInjector(
        DruidSecondaryModule.class,
        new LifecycleModule(Key.get(MonitorScheduler.class)),
        EmitterModule.class,
        CuratorModule.class,
        MetricsModule.class,
        DiscoveryModule.class,
        ServerModule.class,
        MasterModule.class
    );

    final ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);
    final ConfigurationObjectFactory configFactory = injector.getInstance(ConfigurationObjectFactory.class);
    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    final ServiceEmitter emitter = injector.getInstance(ServiceEmitter.class);

    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);

    CuratorFramework curatorFramework = injector.getInstance(CuratorFramework.class);

    final ZkPathsConfig zkPaths = configFactory.build(ZkPathsConfig.class);

    ServerInventoryView serverInventoryView = injector.getInstance(ServerInventoryView.class);


    final DatabaseSegmentManager databaseSegmentManager = injector.getInstance(DatabaseSegmentManager.class);
    final DatabaseRuleManager databaseRuleManager = injector.getInstance(DatabaseRuleManager.class);

    final DruidMasterConfig druidMasterConfig = configFactory.build(DruidMasterConfig.class);
    final DruidNodeConfig nodeConfig = configFactory.build(DruidNodeConfig.class);

    final ServiceDiscovery<Void> serviceDiscovery = injector.getInstance(Key.get(new TypeLiteral<ServiceDiscovery<Void>>(){}));
    final ServiceAnnouncer serviceAnnouncer = injector.getInstance(ServiceAnnouncer.class);
    Initialization.announceDefaultService(nodeConfig, serviceAnnouncer, lifecycle);

    IndexingServiceClient indexingServiceClient = null;
    if (druidMasterConfig.getMergerServiceName() != null) {
      ServiceProvider serviceProvider = Initialization.makeServiceProvider(
          druidMasterConfig.getMergerServiceName(),
          serviceDiscovery,
          lifecycle
      );
//      indexingServiceClient = new IndexingServiceClient(httpClient, jsonMapper, serviceProvider); TODO
    }

    DBI dbi = injector.getInstance(DBI.class);
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

    final Injector injector2 = Guice.createInjector(
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
    root.addEventListener(new GuiceServletConfig(injector2));
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

  private static Injector makeInjector(final Object... modules)
  {
    final Injector baseInjector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new PropertiesModule("runtime.properties"),
        new ConfigFactoryModule()
    );

    return Guice.createInjector(
        Iterables.transform(
            Arrays.asList(modules),
            new Function<Object, Module>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Module apply(@Nullable Object input)
              {
                if (input instanceof Module) {
                  baseInjector.injectMembers(input);
                  return (Module) input;
                }
                if (input instanceof Class) {
                  if (Module.class.isAssignableFrom((Class) input)) {
                    return baseInjector.getInstance((Class<? extends Module>) input);
                  }
                  else {
                    throw new ISE("Class[%s] does not implement %s", input.getClass(), Module.class);
                  }
                }
                throw new ISE("Unknown module type[%s]", input.getClass());
              }
            }
        )
    );
  }
}
