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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import org.apache.druid.client.CoordinatorSegmentWatcherConfig;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.HttpServerInventoryViewResource;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.MetadataConfigModule;
import org.apache.druid.guice.MetadataManagerModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.SegmentSchemaCacheModule;
import org.apache.druid.guice.SupervisorCleanupModule;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.http.JettyHttpClientModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ExecutorServices;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.metadata.MetadataStorage;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.druid.segment.metadata.SegmentMetadataCacheConfig;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.CloneStatusManager;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.config.CoordinatorKillConfigs;
import org.apache.druid.server.coordinator.config.CoordinatorPeriodConfig;
import org.apache.druid.server.coordinator.config.CoordinatorRunConfig;
import org.apache.druid.server.coordinator.config.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.http.ClusterResource;
import org.apache.druid.server.http.CoordinatorCompactionConfigsResource;
import org.apache.druid.server.http.CoordinatorCompactionResource;
import org.apache.druid.server.http.CoordinatorDynamicConfigSyncer;
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
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManagerConfig;
import org.apache.druid.server.metrics.ServiceStatusMonitor;
import org.apache.druid.server.router.TieredBrokerConfig;
import org.apache.druid.storage.local.LocalTmpStorageConfig;
import org.eclipse.jetty.server.Server;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
  private boolean isSegmentSchemaCacheEnabled;

  public CliCoordinator()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    this.properties = properties;
    beOverlord = isOverlord(properties);
    isSegmentSchemaCacheEnabled = MetadataConfigModule.isSegmentSchemaCacheEnabled(properties);

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
    modules.add(new MetadataManagerModule());

    if (isSegmentSchemaCacheEnabled) {
      validateCentralizedDatasourceSchemaConfig(properties);
      modules.add(new SegmentSchemaCacheModule());
      modules.add(new QueryableModule());
    }

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

            binder.bind(MetadataStorage.class).toProvider(MetadataStorageProvider.class);

            JsonConfigProvider.bind(binder, "druid.manager.lookups", LookupCoordinatorManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.coordinator", CoordinatorRunConfig.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.kill", CoordinatorKillConfigs.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.period", CoordinatorPeriodConfig.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.loadqueuepeon.http", HttpLoadQueuePeonConfig.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.balancer", BalancerStrategyFactory.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.segment", CoordinatorSegmentWatcherConfig.class);
            JsonConfigProvider.bind(binder, "druid.coordinator.segmentMetadataCache", SegmentMetadataCacheConfig.class);
            binder.bind(DruidCoordinatorConfig.class);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            binder.bind(CoordinatorDynamicConfigSyncer.class).in(ManageLifecycle.class);
            if (beOverlord) {
              binder.bind(RedirectInfo.class).to(CoordinatorOverlordRedirectInfo.class).in(LazySingleton.class);
            } else {
              binder.bind(RedirectInfo.class).to(CoordinatorRedirectInfo.class).in(LazySingleton.class);
            }

            LifecycleModule.register(binder, CoordinatorServerView.class);

            if (!isSegmentSchemaCacheEnabled) {
              binder.bind(CoordinatorSegmentMetadataCache.class).toProvider(Providers.of(null));
              binder.bind(DirectDruidClientFactory.class).toProvider(Providers.of(null));
            }

            binder.bind(LookupCoordinatorManager.class).in(LazySingleton.class);
            binder.bind(CloneStatusManager.class).in(LazySingleton.class);

            binder.bind(DruidCoordinator.class);
            binder.bind(CompactionStatusTracker.class).in(LazySingleton.class);

            LifecycleModule.register(binder, MetadataStorage.class);
            LifecycleModule.register(binder, DruidCoordinator.class);

            binder.bind(JettyServerInitializer.class)
                  .to(CoordinatorJettyServerInitializer.class);

            Jerseys.addResource(binder, CoordinatorResource.class);
            Jerseys.addResource(binder, CoordinatorCompactionResource.class);
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

            bindAnnouncer(
                binder,
                Coordinator.class,
                DiscoverySideEffectsProvider.create()
            );

            Jerseys.addResource(binder, SelfDiscoveryResource.class);
            LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

            if (!beOverlord) {
              // Bind HeartbeatSupplier only when the service operates independently of Overlord.
              binder.bind(new TypeLiteral<Supplier<Map<String, Object>>>() {})
                  .annotatedWith(Names.named(ServiceStatusMonitor.HEARTBEAT_TAGS_BINDING))
                  .toProvider(HeartbeatSupplier.class);
              binder.bind(LocalTmpStorageConfig.class)
                    .toProvider(new LocalTmpStorageConfig.DefaultLocalTmpStorageConfigProvider("coordinator"))
                    .in(LazySingleton.class);
            }

            binder.bind(CoordinatorCustomDutyGroups.class)
                  .toProvider(new CoordinatorCustomDutyGroupsProvider())
                  .in(LazySingleton.class);
          }

          @Provides
          @LazySingleton
          public LoadQueueTaskMaster getLoadQueueTaskMaster(
              ObjectMapper jsonMapper,
              ScheduledExecutorFactory factory,
              DruidCoordinatorConfig config,
              @EscalatedGlobal HttpClient httpClient,
              Lifecycle lifecycle,
              CoordinatorConfigManager coordinatorConfigManager
          )
          {
            final ExecutorService callBackExec = Execs.singleThreaded("LoadQueuePeon-callbackexec--%d");
            ExecutorServices.manageLifecycle(lifecycle, callBackExec);
            return new LoadQueueTaskMaster(
                jsonMapper,
                factory.create(1, "Master-PeonExec--%d"),
                callBackExec,
                config.getHttpLoadQueuePeonConfig(),
                httpClient,
                coordinatorConfigManager::getCurrentDynamicConfig
            );
          }
        }
    );

    if (beOverlord) {
      CliOverlord cliOverlord = new CliOverlord();
      cliOverlord.configure(properties);
      modules.addAll(cliOverlord.getModules(false));
    } else {
      // Only add LookupSerdeModule if !beOverlord, since CliOverlord includes it, and having two copies causes
      // the injector to get confused due to having multiple bindings for the same classes.
      modules.add(new LookupSerdeModule());
      modules.add(new SupervisorCleanupModule());
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
            "druid.coordinator.dutyGroups"), new TypeReference<>() {});
        for (String coordinatorCustomDutyGroupName : coordinatorCustomDutyGroupNames) {
          String dutyListProperty = StringUtils.format("druid.coordinator.%s.duties", coordinatorCustomDutyGroupName);
          if (Strings.isNullOrEmpty(props.getProperty(dutyListProperty))) {
            throw new IAE("Coordinator custom duty group given without any duty for group %s", coordinatorCustomDutyGroupName);
          }
          List<String> dutyForGroup = jsonMapper.readValue(props.getProperty(dutyListProperty), new TypeReference<>() {});
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
            CoordinatorCustomDuty coordinatorCustomDuty = coordinatorCustomDutyProvider.get();
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

  private static class HeartbeatSupplier implements Provider<Supplier<Map<String, Object>>>
  {
    private final DruidLeaderSelector leaderSelector;

    @Inject
    public HeartbeatSupplier(@Coordinator DruidLeaderSelector leaderSelector)
    {
      this.leaderSelector = leaderSelector;
    }

    @Override
    public Supplier<Map<String, Object>> get()
    {
      return () -> {
        Map<String, Object> heartbeatTags = new HashMap<>();
        heartbeatTags.put("leader", leaderSelector.isLeader() ? 1 : 0);

        return heartbeatTags;
      };
    }
  }

}
