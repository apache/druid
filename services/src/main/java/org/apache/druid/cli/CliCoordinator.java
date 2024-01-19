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
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.client.CoordinatorSegmentWatcherConfig;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.HttpServerInventoryViewResource;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.ConfigProvider;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.ServerViewModule;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Global;
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
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.druid.segment.metadata.SegmentMetadataCacheConfig;
import org.apache.druid.segment.metadata.SegmentMetadataQuerySegmentWalker;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.MetadataManager;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.balancer.CachingCostBalancerStrategyConfig;
import org.apache.druid.server.coordinator.compact.CompactionSegmentSearchPolicy;
import org.apache.druid.server.coordinator.compact.NewestSegmentFirstPolicy;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
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
import org.apache.druid.server.metrics.ServiceStatusMonitor;
import org.apache.druid.server.router.TieredBrokerConfig;
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
  public static final String CENTRALIZED_DATASOURCE_SCHEMA_ENABLED = "druid.centralizedDatasourceSchema.enabled";

  private Properties properties;
  private boolean beOverlord;
  private boolean isSegmentMetadataCacheEnabled;

  public CliCoordinator()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    this.properties = properties;
    beOverlord = isOverlord(properties);
    isSegmentMetadataCacheEnabled = isSegmentMetadataCacheEnabled(properties);

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

    if (isSegmentMetadataCacheEnabled) {
      String serverViewType = (String) properties.getOrDefault(
          ServerViewModule.SERVERVIEW_TYPE_PROPERTY,
          ServerViewModule.DEFAULT_SERVERVIEW_TYPE
      );
      if (!serverViewType.equals(ServerViewModule.SERVERVIEW_TYPE_HTTP)) {
        throw DruidException
            .forPersona(DruidException.Persona.ADMIN)
            .ofCategory(DruidException.Category.UNSUPPORTED)
            .build(
                StringUtils.format(
                    "CentralizedDatasourceSchema feature is incompatible with config %1$s=%2$s. "
                    + "Please consider switching to http based segment discovery (set %1$s=%3$s) "
                    + "or disable the feature (set %4$s=false).",
                    ServerViewModule.SERVERVIEW_TYPE_PROPERTY,
                    serverViewType,
                    ServerViewModule.SERVERVIEW_TYPE_HTTP,
                    CliCoordinator.CENTRALIZED_DATASOURCE_SCHEMA_ENABLED
                ));
      }
      modules.add(new CoordinatorSegmentMetadataCacheModule());
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
            JsonConfigProvider.bind(binder, "druid.coordinator.segmentMetadataCache", SegmentMetadataCacheConfig.class);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            if (beOverlord) {
              binder.bind(RedirectInfo.class).to(CoordinatorOverlordRedirectInfo.class).in(LazySingleton.class);
            } else {
              binder.bind(RedirectInfo.class).to(CoordinatorRedirectInfo.class).in(LazySingleton.class);
            }

            LifecycleModule.register(binder, CoordinatorServerView.class);

            if (!isSegmentMetadataCacheEnabled) {
              binder.bind(CoordinatorSegmentMetadataCache.class).toProvider(Providers.of(null));
              binder.bind(DirectDruidClientFactory.class).toProvider(Providers.of(null));
            }

            binder.bind(SegmentsMetadataManager.class)
                  .toProvider(SegmentsMetadataManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(MetadataRuleManager.class)
                  .toProvider(MetadataRuleManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(LookupCoordinatorManager.class).in(LazySingleton.class);

            binder.bind(CoordinatorConfigManager.class);
            binder.bind(MetadataManager.class);
            binder.bind(DruidCoordinator.class);

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

            if (properties.containsKey("druid.coordinator.merge.on")) {
              throw new UnsupportedOperationException(
                  "'druid.coordinator.merge.on' is not supported anymore. "
                  + "Please consider using Coordinator's automatic compaction instead. "
                  + "See https://druid.apache.org/docs/latest/operations/segment-optimization.html and "
                  + "https://druid.apache.org/docs/latest/api-reference/api-reference.html#compaction-configuration "
                  + "for more details about compaction."
              );
            }

            //TODO: make this configurable when there are multiple search policies
            binder.bind(CompactionSegmentSearchPolicy.class).to(NewestSegmentFirstPolicy.class);

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
              // Bind HeartbeatSupplier only when the service operates independently of Overlord.
              binder.bind(new TypeLiteral<Supplier<Map<String, Object>>>() {})
                  .annotatedWith(Names.named(ServiceStatusMonitor.HEARTBEAT_TAGS_BINDING))
                  .toProvider(HeartbeatSupplier.class);
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

  private boolean isSegmentMetadataCacheEnabled(Properties properties)
  {
    return Boolean.parseBoolean(properties.getProperty(CENTRALIZED_DATASOURCE_SCHEMA_ENABLED));
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
    private final DruidCoordinator coordinator;

    @Inject
    public HeartbeatSupplier(DruidCoordinator coordinator)
    {
      this.coordinator = coordinator;
    }

    @Override
    public Supplier<Map<String, Object>> get()
    {
      return () -> {
        Map<String, Object> heartbeatTags = new HashMap<>();
        heartbeatTags.put("leader", coordinator.isLeader() ? 1 : 0);

        return heartbeatTags;
      };
    }
  }

  private static class CoordinatorSegmentMetadataCacheModule implements Module
  {
    @Override
    public void configure(Binder binder)
    {
      JsonConfigProvider.bind(binder, "druid.coordinator.segmentMetadata", SegmentMetadataQueryConfig.class);
      JsonConfigProvider.bind(binder, "druid.coordinator.query.scheduler", QuerySchedulerProvider.class, Global.class);
      JsonConfigProvider.bind(binder, "druid.coordinator.query.default", DefaultQueryConfig.class);
      JsonConfigProvider.bind(binder, "druid.coordinator.query.retryPolicy", RetryQueryRunnerConfig.class);
      JsonConfigProvider.bind(binder, "druid.coordinator.internal.query.config", InternalQueryConfig.class);
      JsonConfigProvider.bind(binder, "druid.centralizedDatasourceSchema", CentralizedDatasourceSchemaConfig.class);

      MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);
      toolChests.addBinding(SegmentMetadataQuery.class).to(SegmentMetadataQueryQueryToolChest.class);
      binder.bind(SegmentMetadataQueryQueryToolChest.class).in(LazySingleton.class);
      binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);

      final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder =
          DruidBinders.queryRunnerFactoryBinder(binder);
      queryFactoryBinder.addBinding(SegmentMetadataQuery.class).to(SegmentMetadataQueryRunnerFactory.class);
      binder.bind(SegmentMetadataQueryRunnerFactory.class).in(LazySingleton.class);

      binder.bind(GenericQueryMetricsFactory.class).to(DefaultGenericQueryMetricsFactory.class);

      binder.bind(QueryScheduler.class)
            .toProvider(Key.get(QuerySchedulerProvider.class, Global.class))
            .in(LazySingleton.class);
      binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);

      binder.bind(QuerySegmentWalker.class).to(SegmentMetadataQuerySegmentWalker.class).in(LazySingleton.class);
      LifecycleModule.register(binder, CoordinatorSegmentMetadataCache.class);
    }

    @LazySingleton
    @Provides
    public QueryWatcher getWatcher(QueryScheduler scheduler)
    {
      return scheduler;
    }
  }
}
