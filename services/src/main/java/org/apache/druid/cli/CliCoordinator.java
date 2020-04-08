/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.CoordinatorSegmentWatcherConfig;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.HttpServerInventoryViewResource;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ConditionalMultibind;
import org.apache.druid.guice.ConfigProvider;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.CoordinatorIndexingServiceDuty;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.http.JettyHttpClientModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ExecutorServices;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.MetadataRuleManagerConfig;
import org.apache.druid.metadata.MetadataRuleManagerProvider;
import org.apache.druid.metadata.MetadataStorage;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SegmentsMetadataManagerProvider;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.server.audit.AuditManagerProvider;
import org.apache.druid.server.coordinator.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.CachingCostBalancerStrategyConfig;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.KillStalePendingSegments;
import org.apache.druid.server.coordinator.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.KillUnusedSegments;
import org.apache.druid.server.http.ClusterResource;
import org.apache.druid.server.http.CompactionResource;
import org.apache.druid.server.http.CoordinatorCompactionConfigsResource;
import org.apache.druid.server.http.CoordinatorDynamicConfigsResource;
import org.apache.druid.server.http.CoordinatorRedirectInfo;
import org.apache.druid.server.http.CoordinatorResource;
import org.apache.druid.server.http.DataSourcesResource;
import org.apache.druid.server.http.IntervalsResource;
import org.apache.druid.server.http.LookupCoordinatorResource;
import org.apache.druid.server.http.MetadataResource;
import org.apache.druid.server.http.RedirectFilter;
import org.apache.druid.server.http.RedirectInfo;
import org.apache.druid.server.http.RulesResource;
import org.apache.druid.server.http.SelfDiscoveryResource;
import org.apache.druid.server.http.ServersResource;
import org.apache.druid.server.http.TiersResource;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManagerConfig;
import org.apache.druid.server.router.TieredBrokerConfig;
import org.eclipse.jetty.server.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 *
 */
@Command(
    name = "coordinator",
    description = "Runs the Coordinator, see https://druid.apache.org/docs/latest/Coordinator.html for a description."
)
public class CliCoordinator extends ServerRunnable
{
  private static final Logger log = new Logger(CliCoordinator.class);
  private static final String AS_OVERLORD_PROPERTY = "druid.coordinator.asOverlord.enabled";

  private Properties properties;
  private boolean beOverlord;

  public CliCoordinator()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    this.properties = properties;
    beOverlord = isOverlord(properties);

    if (beOverlord) {
      log.info("Coordinator is configured to act as Overlord as well (%s = true).", AS_OVERLORD_PROPERTY);
    }
  }

  @Override
  protected List<? extends Module> getModules()
  {
    List<Module> modules = new ArrayList<>();

    modules.add(JettyHttpClientModule.global());

    modules.add(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant()
                  .annotatedWith(Names.named("serviceName"))
                  .to(TieredBrokerConfig.DEFAULT_COORDINATOR_SERVICE_NAME);
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8081);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8281);

            ConfigProvider.bind(binder, DruidCoordinatorConfig.class);

            binder.bind(MetadataStorage.class).toProvider(MetadataStorageProvider.class);

            JsonConfigProvider.bind(binder, "druid.manager.segments", SegmentsMetadataManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.manager.rules", MetadataRuleManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.manager.lookups", LookupCoordinatorManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.balancer", BalancerStrategyFactory.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.segment", CoordinatorSegmentWatcherConfig.class);
            JsonConfigProvider.bind(
                binder,
                "druid.coordinator.balancer.cachingCost",
                CachingCostBalancerStrategyConfig.class
            );

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            if (beOverlord) {
              binder.bind(RedirectInfo.class).to(CoordinatorOverlordRedirectInfo.class).in(LazySingleton.class);
            } else {
              binder.bind(RedirectInfo.class).to(CoordinatorRedirectInfo.class).in(LazySingleton.class);
            }

            binder.bind(SegmentsMetadataManager.class)
                  .toProvider(SegmentsMetadataManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(MetadataRuleManager.class)
                  .toProvider(MetadataRuleManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(AuditManager.class)
                  .toProvider(AuditManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(IndexingServiceClient.class).to(HttpIndexingServiceClient.class).in(LazySingleton.class);

            binder.bind(LookupCoordinatorManager.class).in(LazySingleton.class);
            binder.bind(CoordinatorServerView.class);
            binder.bind(DruidCoordinator.class);

            LifecycleModule.register(binder, CoordinatorServerView.class);
            LifecycleModule.register(binder, MetadataStorage.class);
            LifecycleModule.register(binder, DruidCoordinator.class);

            binder.bind(JettyServerInitializer.class)
                  .to(CoordinatorJettyServerInitializer.class);

            Jerseys.addResource(binder, CoordinatorResource.class);
            Jerseys.addResource(binder, CompactionResource.class);
            Jerseys.addResource(binder, CoordinatorDynamicConfigsResource.class);
            Jerseys.addResource(binder, CoordinatorCompactionConfigsResource.class);
            Jerseys.addResource(binder, TiersResource.class);
            Jerseys.addResource(binder, RulesResource.class);
            Jerseys.addResource(binder, ServersResource.class);
            Jerseys.addResource(binder, DataSourcesResource.class);
            Jerseys.addResource(binder, MetadataResource.class);
            Jerseys.addResource(binder, IntervalsResource.class);
            Jerseys.addResource(binder, LookupCoordinatorResource.class);
            Jerseys.addResource(binder, ClusterResource.class);
            Jerseys.addResource(binder, HttpServerInventoryViewResource.class);

            LifecycleModule.register(binder, Server.class);
            LifecycleModule.register(binder, DataSourcesResource.class);

            final ConditionalMultibind<CoordinatorDuty> conditionalMultibind = ConditionalMultibind.create(
                properties,
                binder,
                CoordinatorDuty.class,
                CoordinatorIndexingServiceDuty.class
            );

            if (conditionalMultibind.matchCondition("druid.coordinator.merge.on", Predicates.equalTo("true"))) {
              throw new UnsupportedOperationException(
                  "'druid.coordinator.merge.on' is not supported anymore. "
                  + "Please consider using Coordinator's automatic compaction instead. "
                  + "See https://druid.apache.org/docs/latest/operations/segment-optimization.html and "
                  + "https://druid.apache.org/docs/latest/operations/api-reference.html#compaction-configuration "
                  + "for more details about compaction."
              );
            }

            conditionalMultibind.addConditionBinding(
                "druid.coordinator.kill.on",
                Predicates.equalTo("true"),
                KillUnusedSegments.class
            );
            conditionalMultibind.addConditionBinding(
                "druid.coordinator.kill.pendingSegments.on",
                "true",
                Predicates.equalTo("true"),
                KillStalePendingSegments.class
            );

            bindNodeRoleAndAnnouncer(
                binder,
                Coordinator.class,
                DiscoverySideEffectsProvider.builder(NodeRole.COORDINATOR).build()
            );

            Jerseys.addResource(binder, SelfDiscoveryResource.class);
            LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));
          }

          @Provides
          @LazySingleton
          public LoadQueueTaskMaster getLoadQueueTaskMaster(
              CuratorFramework curator,
              ObjectMapper jsonMapper,
              ScheduledExecutorFactory factory,
              DruidCoordinatorConfig config,
              @EscalatedGlobal HttpClient httpClient,
              ZkPathsConfig zkPaths,
              Lifecycle lifecycle
          )
          {
            boolean useHttpLoadQueuePeon = "http".equalsIgnoreCase(config.getLoadQueuePeonType());
            ExecutorService callBackExec;
            if (useHttpLoadQueuePeon) {
              callBackExec = Execs.singleThreaded("LoadQueuePeon-callbackexec--%d");
            } else {
              callBackExec = Execs.multiThreaded(
                  config.getNumCuratorCallBackThreads(),
                  "LoadQueuePeon-callbackexec--%d"
              );
            }
            ExecutorServices.manageLifecycle(lifecycle, callBackExec);
            return new LoadQueueTaskMaster(
                curator,
                jsonMapper,
                factory.create(1, "Master-PeonExec--%d"),
                callBackExec,
                config,
                httpClient,
                zkPaths
            );
          }
        }
    );

    if (beOverlord) {
      modules.addAll(new CliOverlord().getModules(false));
    } else {
      // Only add LookupSerdeModule if !beOverlord, since CliOverlord includes it, and having two copies causes
      // the injector to get confused due to having multiple bindings for the same classes.
      modules.add(new LookupSerdeModule());
    }

    return modules;
  }

  public static boolean isOverlord(Properties properties)
  {
    return Boolean.parseBoolean(properties.getProperty(AS_OVERLORD_PROPERTY));
  }
}
