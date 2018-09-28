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
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.HttpServerInventoryViewResource;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.ConditionalMultibind;
import org.apache.druid.guice.ConfigProvider;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.CoordinatorIndexingServiceHelper;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.http.JettyHttpClientModule;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.MetadataRuleManagerConfig;
import org.apache.druid.metadata.MetadataRuleManagerProvider;
import org.apache.druid.metadata.MetadataSegmentManager;
import org.apache.druid.metadata.MetadataSegmentManagerConfig;
import org.apache.druid.metadata.MetadataSegmentManagerProvider;
import org.apache.druid.metadata.MetadataStorage;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.server.audit.AuditManagerProvider;
import org.apache.druid.server.coordinator.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorCleanupPendingSegments;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorHelper;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorSegmentKiller;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorSegmentMerger;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorVersionConverter;
import org.apache.druid.server.http.ClusterResource;
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
import org.apache.druid.server.http.ServersResource;
import org.apache.druid.server.http.TiersResource;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManagerConfig;
import org.apache.druid.server.router.TieredBrokerConfig;
import org.apache.curator.framework.CuratorFramework;
import org.eclipse.jetty.server.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 */
@Command(
    name = "coordinator",
    description = "Runs the Coordinator, see http://druid.io/docs/latest/Coordinator.html for a description."
)
public class CliCoordinator extends ServerRunnable
{
  private static final Logger log = new Logger(CliCoordinator.class);

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
      log.info("Coordinator is configured to act as Overlord as well.");
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

            binder.bind(MetadataStorage.class)
                  .toProvider(MetadataStorageProvider.class);

            JsonConfigProvider.bind(binder, "druid.manager.segments", MetadataSegmentManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.manager.rules", MetadataRuleManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.manager.lookups", LookupCoordinatorManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.balancer", BalancerStrategyFactory.class);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            if (beOverlord) {
              binder.bind(RedirectInfo.class).to(CoordinatorOverlordRedirectInfo.class).in(LazySingleton.class);
            } else {
              binder.bind(RedirectInfo.class).to(CoordinatorRedirectInfo.class).in(LazySingleton.class);
            }

            binder.bind(MetadataSegmentManager.class)
                  .toProvider(MetadataSegmentManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(MetadataRuleManager.class)
                  .toProvider(MetadataRuleManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(AuditManager.class)
                  .toProvider(AuditManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(IndexingServiceClient.class).to(HttpIndexingServiceClient.class).in(LazySingleton.class);
            binder.bind(CoordinatorServerView.class).in(LazySingleton.class);

            binder.bind(LookupCoordinatorManager.class).in(LazySingleton.class);
            binder.bind(DruidCoordinator.class);

            LifecycleModule.register(binder, MetadataStorage.class);
            LifecycleModule.register(binder, DruidCoordinator.class);

            binder.bind(JettyServerInitializer.class)
                  .to(CoordinatorJettyServerInitializer.class);

            Jerseys.addResource(binder, CoordinatorResource.class);
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

            ConditionalMultibind.create(
                properties,
                binder,
                DruidCoordinatorHelper.class,
                CoordinatorIndexingServiceHelper.class
            ).addConditionBinding(
                "druid.coordinator.merge.on",
                Predicates.equalTo("true"),
                DruidCoordinatorSegmentMerger.class
            ).addConditionBinding(
                "druid.coordinator.conversion.on",
                Predicates.equalTo("true"),
                DruidCoordinatorVersionConverter.class
            ).addConditionBinding(
                "druid.coordinator.kill.on",
                Predicates.equalTo("true"),
                DruidCoordinatorSegmentKiller.class
            ).addConditionBinding(
                "druid.coordinator.kill.pendingSegments.on",
                Predicates.equalTo("true"),
                DruidCoordinatorCleanupPendingSegments.class
            );

            binder.bind(DiscoverySideEffectsProvider.Child.class).annotatedWith(Coordinator.class).toProvider(
                new DiscoverySideEffectsProvider(
                    DruidNodeDiscoveryProvider.NODE_TYPE_COORDINATOR,
                    ImmutableList.of()
                )
            ).in(LazySingleton.class);
            LifecycleModule.registerKey(binder, Key.get(DiscoverySideEffectsProvider.Child.class, Coordinator.class));
          }

          @Provides
          @LazySingleton
          public LoadQueueTaskMaster getLoadQueueTaskMaster(
              CuratorFramework curator,
              ObjectMapper jsonMapper,
              ScheduledExecutorFactory factory,
              DruidCoordinatorConfig config,
              @EscalatedGlobal HttpClient httpClient,
              ZkPathsConfig zkPaths
          )
          {
            return new LoadQueueTaskMaster(
                curator, jsonMapper, factory.create(1, "Master-PeonExec--%d"),
                Executors.newSingleThreadExecutor(), config, httpClient, zkPaths
            );
          }
        }
    );

    if (beOverlord) {
      modules.addAll(new CliOverlord().getModules(false));
    }

    return modules;
  }

  public static boolean isOverlord(Properties properties)
  {
    return Boolean.parseBoolean(properties.getProperty("druid.coordinator.asOverlord.enabled"));
  }
}
