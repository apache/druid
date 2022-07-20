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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rvesse.airline.annotations.Command;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
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
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.CoordinatorIndexingServiceDuty;
import org.apache.druid.guice.annotations.CoordinatorMetadataStoreManagementDuty;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.http.JettyHttpClientModule;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
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
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.server.audit.AuditManagerProvider;
import org.apache.druid.server.coordinator.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.CachingCostBalancerStrategyConfig;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.KillStalePendingSegments;
import org.apache.druid.server.coordinator.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.KillAuditLog;
import org.apache.druid.server.coordinator.duty.KillCompactionConfig;
import org.apache.druid.server.coordinator.duty.KillDatasourceMetadata;
import org.apache.druid.server.coordinator.duty.KillRules;
import org.apache.druid.server.coordinator.duty.KillSupervisors;
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
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
  protected Set<NodeRole> getNodeRoles(Properties properties)
  {
    return isOverlord(properties)
           ? ImmutableSet.of(NodeRole.COORDINATOR, NodeRole.OVERLORD)
           : ImmutableSet.of(NodeRole.COORDINATOR);
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

            JsonConfigProvider.bind(binder, SegmentsMetadataManagerConfig.CONFIG_PREFIX, SegmentsMetadataManagerConfig.class);
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

            // Binding for Set of indexing service coordinator Duty
            final ConditionalMultibind<CoordinatorDuty> conditionalIndexingServiceDutyMultibind = ConditionalMultibind.create(
                properties,
                binder,
                CoordinatorDuty.class,
                CoordinatorIndexingServiceDuty.class
            );
            if (conditionalIndexingServiceDutyMultibind.matchCondition("druid.coordinator.merge.on", Predicates.equalTo("true"))) {
              throw new UnsupportedOperationException(
                  "'druid.coordinator.merge.on' is not supported anymore. "
                  + "Please consider using Coordinator's automatic compaction instead. "
                  + "See https://druid.apache.org/docs/latest/operations/segment-optimization.html and "
                  + "https://druid.apache.org/docs/latest/operations/api-reference.html#compaction-configuration "
                  + "for more details about compaction."
              );
            }
            conditionalIndexingServiceDutyMultibind.addConditionBinding(
                "druid.coordinator.kill.on",
                "false",
                Predicates.equalTo("true"),
                KillUnusedSegments.class
            );
            conditionalIndexingServiceDutyMultibind.addConditionBinding(
                "druid.coordinator.kill.pendingSegments.on",
                "true",
                Predicates.equalTo("true"),
                KillStalePendingSegments.class
            );

            // Binding for Set of metadata store management coordinator Ddty
            final ConditionalMultibind<CoordinatorDuty> conditionalMetadataStoreManagementDutyMultibind = ConditionalMultibind.create(
                properties,
                binder,
                CoordinatorDuty.class,
                CoordinatorMetadataStoreManagementDuty.class
            );
            conditionalMetadataStoreManagementDutyMultibind.addConditionBinding(
                "druid.coordinator.kill.supervisor.on",
                "true",
                Predicates.equalTo("true"),
                KillSupervisors.class
            );
            conditionalMetadataStoreManagementDutyMultibind.addConditionBinding(
                "druid.coordinator.kill.audit.on",
                "true",
                Predicates.equalTo("true"),
                KillAuditLog.class
            );
            conditionalMetadataStoreManagementDutyMultibind.addConditionBinding(
                "druid.coordinator.kill.rule.on",
                "true",
                Predicates.equalTo("true"),
                KillRules.class
            );
            conditionalMetadataStoreManagementDutyMultibind.addConditionBinding(
                "druid.coordinator.kill.datasource.on",
                "true",
                Predicates.equalTo("true"),
                KillDatasourceMetadata.class
            );
            conditionalMetadataStoreManagementDutyMultibind.addConditionBinding(
                "druid.coordinator.kill.compaction.on",
                Predicates.equalTo("true"),
                KillCompactionConfig.class
            );

            bindAnnouncer(
                binder,
                Coordinator.class,
                DiscoverySideEffectsProvider.create()
            );

            Jerseys.addResource(binder, SelfDiscoveryResource.class);
            LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

            if (!beOverlord) {
              // These are needed to deserialize SupervisorSpec for Supervisor Auto Cleanup
              binder.bind(TaskStorage.class).toProvider(Providers.of(null));
              binder.bind(TaskMaster.class).toProvider(Providers.of(null));
              binder.bind(RowIngestionMetersFactory.class).toProvider(Providers.of(null));
            }

            binder.bind(CoordinatorCustomDutyGroups.class)
                  .toProvider(new CoordinatorCustomDutyGroupsProvider())
                  .in(LazySingleton.class);
          }

          @Provides
          @LazySingleton
          public LoadQueueTaskMaster getLoadQueueTaskMaster(
              Provider<CuratorFramework> curatorFrameworkProvider,
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
                curatorFrameworkProvider,
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

  private static class CoordinatorCustomDutyGroupsProvider implements Provider<CoordinatorCustomDutyGroups>
  {
    private Properties props;
    private JsonConfigurator configurator;
    private ObjectMapper jsonMapper;

    @Inject
    public void inject(Properties props, JsonConfigurator configurator, ObjectMapper jsonMapper)
    {
      this.props = props;
      this.configurator = configurator;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public CoordinatorCustomDutyGroups get()
    {
      try {
        Set<CoordinatorCustomDutyGroup> coordinatorCustomDutyGroups = new HashSet<>();
        if (Strings.isNullOrEmpty(props.getProperty("druid.coordinator.dutyGroups"))) {
          return new CoordinatorCustomDutyGroups(coordinatorCustomDutyGroups);
        }
        List<String> coordinatorCustomDutyGroupNames = jsonMapper.readValue(props.getProperty(
            "druid.coordinator.dutyGroups"), new TypeReference<List<String>>() {});
        for (String coordinatorCustomDutyGroupName : coordinatorCustomDutyGroupNames) {
          String dutyListProperty = StringUtils.format("druid.coordinator.%s.duties", coordinatorCustomDutyGroupName);
          if (Strings.isNullOrEmpty(props.getProperty(dutyListProperty))) {
            throw new IAE("Coordinator custom duty group given without any duty for group %s", coordinatorCustomDutyGroupName);
          }
          List<String> dutyForGroup = jsonMapper.readValue(props.getProperty(dutyListProperty), new TypeReference<List<String>>() {});
          List<CoordinatorCustomDuty> coordinatorCustomDuties = new ArrayList<>();
          for (String dutyName : dutyForGroup) {
            final String dutyPropertyBase = StringUtils.format(
                "druid.coordinator.%s.duty.%s",
                coordinatorCustomDutyGroupName,
                dutyName
            );
            final JsonConfigProvider<CoordinatorCustomDuty> coordinatorCustomDutyProvider = JsonConfigProvider.of(
                dutyPropertyBase,
                CoordinatorCustomDuty.class
            );

            String typeProperty = StringUtils.format("%s.type", dutyPropertyBase);
            Properties adjustedProps = new Properties(props);
            if (adjustedProps.containsKey(typeProperty)) {
              throw new IAE("'type' property [%s] is reserved.", typeProperty);
            } else {
              adjustedProps.put(typeProperty, dutyName);
            }
            coordinatorCustomDutyProvider.inject(adjustedProps, configurator);
            Supplier<CoordinatorCustomDuty> coordinatorCustomDutySupplier = coordinatorCustomDutyProvider.get();
            if (coordinatorCustomDutySupplier == null) {
              throw new ISE("Could not create CoordinatorCustomDuty with name: %s for group: %s", dutyName, coordinatorCustomDutyGroupName);
            }
            CoordinatorCustomDuty coordinatorCustomDuty = coordinatorCustomDutySupplier.get();
            if (coordinatorCustomDuty == null) {
              throw new ISE("Could not create CoordinatorCustomDuty with name: %s for group: %s", dutyName, coordinatorCustomDutyGroupName);
            }
            coordinatorCustomDuties.add(coordinatorCustomDuty);
          }
          String groupPeriodPropKey = StringUtils.format("druid.coordinator.%s.period", coordinatorCustomDutyGroupName);
          if (Strings.isNullOrEmpty(props.getProperty(groupPeriodPropKey))) {
            throw new IAE("Run period for coordinator custom duty group must be set for group %s", coordinatorCustomDutyGroupName);
          }
          Duration groupPeriod = new Duration(props.getProperty(groupPeriodPropKey));
          coordinatorCustomDutyGroups.add(new CoordinatorCustomDutyGroup(coordinatorCustomDutyGroupName, groupPeriod, coordinatorCustomDuties));
        }
        return new CoordinatorCustomDutyGroups(coordinatorCustomDutyGroups);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
