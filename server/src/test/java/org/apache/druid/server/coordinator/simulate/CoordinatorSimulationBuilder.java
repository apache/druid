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

package org.apache.druid.server.coordinator.simulate;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.client.DruidServer;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.DirectExecutorService;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.metrics.MetricsVerifier;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.segment.cache.NoopSegmentMetadataCache;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.CloneStatusManager;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.MetadataManager;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.balancer.CachingCostBalancerStrategyConfig;
import org.apache.druid.server.coordinator.balancer.CachingCostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.balancer.DiskNormalizedCostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.balancer.RandomBalancerStrategyFactory;
import org.apache.druid.server.coordinator.config.CoordinatorKillConfigs;
import org.apache.druid.server.coordinator.config.CoordinatorPeriodConfig;
import org.apache.druid.server.coordinator.config.CoordinatorRunConfig;
import org.apache.druid.server.coordinator.config.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.http.CoordinatorDynamicConfigSyncer;
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Builder for {@link CoordinatorSimulation}.
 */
public class CoordinatorSimulationBuilder
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper()
      .setInjectableValues(
          new InjectableValues.Std().addValue(
              DataSegment.PruneSpecsHolder.class,
              DataSegment.PruneSpecsHolder.DEFAULT
          )
      );
  private String balancerStrategy;
  private CoordinatorDynamicConfig dynamicConfig = CoordinatorDynamicConfig.builder().build();
  private List<DruidServer> servers;
  private List<DataSegment> segments;
  private final Map<String, List<Rule>> datasourceRules = new HashMap<>();
  private boolean loadImmediately = false;
  private boolean autoSyncInventory = true;

  /**
   * Specifies the balancer strategy to be used.
   * <p>
   * Default: "cost" ({@link CostBalancerStrategyFactory})
   */
  public CoordinatorSimulationBuilder withBalancer(String balancerStrategy)
  {
    this.balancerStrategy = balancerStrategy;
    return this;
  }

  public CoordinatorSimulationBuilder withServers(List<DruidServer> servers)
  {
    this.servers = servers;
    return this;
  }

  public CoordinatorSimulationBuilder withServers(DruidServer... servers)
  {
    return withServers(Arrays.asList(servers));
  }

  public CoordinatorSimulationBuilder withSegments(List<DataSegment> segments)
  {
    this.segments = segments;
    return this;
  }

  public CoordinatorSimulationBuilder withRules(String datasource, Rule... rules)
  {
    this.datasourceRules.put(datasource, Arrays.asList(rules));
    return this;
  }

  /**
   * Specifies whether segments should be loaded as soon as they are queued.
   * <p>
   * Default: false
   */
  public CoordinatorSimulationBuilder withImmediateSegmentLoading(boolean loadImmediately)
  {
    this.loadImmediately = loadImmediately;
    return this;
  }

  /**
   * Specifies whether the inventory view maintained by the coordinator
   * should be auto-synced as soon as any change is made to the cluster.
   * <p>
   * Default: true
   */
  public CoordinatorSimulationBuilder withAutoInventorySync(boolean autoSync)
  {
    this.autoSyncInventory = autoSync;
    return this;
  }

  /**
   * Specifies the CoordinatorDynamicConfig to be used in the simulation.
   * <p>
   * Default values: as specified in {@link CoordinatorDynamicConfig.Builder}.
   * <p>
   * Tests that verify balancing behaviour use batched segment sampling.
   * Otherwise, the segment sampling is random and can produce repeated values
   * leading to flakiness in the tests. The simulation sets this field to true by
   * default.
   */
  public CoordinatorSimulationBuilder withDynamicConfig(CoordinatorDynamicConfig dynamicConfig)
  {
    this.dynamicConfig = dynamicConfig;
    return this;
  }

  public CoordinatorSimulation build()
  {
    Preconditions.checkArgument(
        servers != null && !servers.isEmpty(),
        "Cannot run simulation for an empty cluster"
    );

    // Prepare the environment
    final TestServerInventoryView serverInventoryView = new TestServerInventoryView();
    servers.forEach(serverInventoryView::addServer);

    final Environment env = new Environment(
        serverInventoryView,
        dynamicConfig,
        loadImmediately,
        autoSyncInventory,
        balancerStrategy
    );

    if (segments != null) {
      segments.forEach(env.segmentManager::addSegment);
    }
    datasourceRules.forEach(
        (datasource, rules) ->
            env.ruleManager.overrideRule(datasource, rules, null)
    );

    // Build the coordinator
    final DruidCoordinator coordinator = new DruidCoordinator(
        env.coordinatorConfig,
        env.metadataManager,
        env.coordinatorInventoryView,
        env.serviceEmitter,
        env.executorFactory,
        new SimOverlordClient(env.segmentManager),
        env.loadQueueTaskMaster,
        env.loadQueueManager,
        new ServiceAnnouncer.Noop(),
        null,
        new CoordinatorCustomDutyGroups(Collections.emptySet()),
        env.lookupCoordinatorManager,
        env.leaderSelector,
        null,
        CentralizedDatasourceSchemaConfig.create(),
        new CompactionStatusTracker(OBJECT_MAPPER),
        env.configSyncer,
        env.cloneStatusManager
    );

    return new SimulationImpl(coordinator, env);
  }

  /**
   * Implementation of {@link CoordinatorSimulation}.
   */
  private static class SimulationImpl implements CoordinatorSimulation,
      CoordinatorSimulation.CoordinatorState, CoordinatorSimulation.ClusterState
  {
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final Environment env;
    private final DruidCoordinator coordinator;

    private SimulationImpl(DruidCoordinator coordinator, Environment env)
    {
      this.env = env;
      this.coordinator = coordinator;
    }

    @Override
    public void start()
    {
      if (!running.compareAndSet(false, true)) {
        throw new ISE("Simulation is already running");
      }

      try {
        env.setUp();
        coordinator.start();
        env.executorFactory.findExecutors();
      }
      catch (Exception e) {
        throw new ISE(e, "Exception while running simulation");
      }
    }

    @Override
    public void stop()
    {
      coordinator.stop();
      env.leaderSelector.stopBeingLeader();
      env.tearDown();
    }

    @Override
    public CoordinatorState coordinator()
    {
      return this;
    }

    @Override
    public ClusterState cluster()
    {
      return this;
    }

    @Override
    public void runCoordinatorCycle()
    {
      verifySimulationRunning();
      env.serviceEmitter.flush();

      // Invoke historical duties
      env.executorFactory.historicalDutiesRunner.finishNextPendingTasks(1);
    }

    @Override
    public void syncInventoryView()
    {
      verifySimulationRunning();
      Preconditions.checkState(
          !env.autoSyncInventory,
          "Cannot invoke syncInventoryView as simulation is running in auto-sync mode."
      );
      env.coordinatorInventoryView.sync(env.inventory);
    }

    @Override
    public void setDynamicConfig(CoordinatorDynamicConfig dynamicConfig)
    {
      env.setDynamicConfig(dynamicConfig);
    }

    @Override
    public void setRetentionRules(String datasource, Rule... rules)
    {
      env.ruleManager.overrideRule(
          datasource,
          Arrays.asList(rules),
          new AuditInfo("sim", "sim", "sim", "localhost")
      );
    }

    @Override
    public DruidServer getInventoryView(String serverName)
    {
      return env.coordinatorInventoryView.getInventoryValue(serverName);
    }

    @Override
    public void loadQueuedSegmentsSkipCallbacks()
    {
      loadSegments(false);
    }

    @Override
    public void loadQueuedSegments()
    {
      loadSegments(true);
    }

    private void loadSegments(boolean executeCallbacks)
    {
      verifySimulationRunning();
      Preconditions.checkState(
          !env.loadImmediately,
          "Cannot invoke loadQueuedSegments as simulation is running in immediate loading mode."
      );

      final BlockingExecutorService loadQueueExecutor = env.executorFactory.loadQueueExecutor;
      final BlockingExecutorService loadCallbackExecutor = env.executorFactory.loadCallbackExecutor;
      while (loadQueueExecutor.hasPendingTasks()
             || (executeCallbacks && loadCallbackExecutor.hasPendingTasks())) {
        // Drain all the items from the load queue executor
        // This sends at most 1 load/drop request to each server
        loadQueueExecutor.finishAllPendingTasks();

        // Load all the queued segments, handle their responses and execute callbacks
        int loadedSegments = env.executorFactory.historicalLoader.finishAllPendingTasks();
        loadQueueExecutor.finishNextPendingTasks(loadedSegments);
        if (executeCallbacks) {
          env.executorFactory.loadCallbackExecutor.finishAllPendingTasks();
        }
      }
    }

    @Override
    public void removeServer(DruidServer server)
    {
      env.inventory.removeServer(server);
    }

    @Override
    public void addServer(DruidServer server)
    {
      env.inventory.addServer(server);
    }

    @Override
    public void addSegments(List<DataSegment> segments)
    {
      if (segments != null) {
        segments.forEach(env.segmentManager::addSegment);
      }
    }

    private void verifySimulationRunning()
    {
      if (!running.get()) {
        throw new ISE("Simulation hasn't been started yet.");
      }
    }

    @Override
    public double getLoadPercentage(String datasource)
    {
      return coordinator.getDatasourceToLoadStatus().get(datasource);
    }

    @Override
    public MetricsVerifier getMetricsVerifier()
    {
      return env.serviceEmitter;
    }
  }

  /**
   * Environment for a coordinator simulation.
   */
  private static class Environment
  {
    private final Lifecycle lifecycle = new Lifecycle("coord-sim");
    private final StubServiceEmitter serviceEmitter
        = new StubServiceEmitter("coordinator", "coordinator");
    private final AtomicReference<CoordinatorDynamicConfig> dynamicConfig
        = new AtomicReference<>();
    private final TestDruidLeaderSelector leaderSelector
        = new TestDruidLeaderSelector();

    private final ExecutorFactory executorFactory;
    private final TestSegmentsMetadataManager segmentManager = new TestSegmentsMetadataManager();
    private final TestMetadataRuleManager ruleManager = new TestMetadataRuleManager();

    private final LoadQueueTaskMaster loadQueueTaskMaster;
    private final SegmentLoadQueueManager loadQueueManager;

    /**
     * Represents the current inventory of all servers (typically historicals)
     * actually present in the cluster.
     */
    private final TestServerInventoryView inventory;

    /**
     * Represents the view of the cluster inventory as seen by the coordinator.
     * When {@code autoSyncInventory=true}, this is the same as {@link #inventory}.
     */
    private final TestServerInventoryView coordinatorInventoryView;

    private final MetadataManager metadataManager;
    private final LookupCoordinatorManager lookupCoordinatorManager;
    private final DruidCoordinatorConfig coordinatorConfig;
    private final CoordinatorDynamicConfigSyncer configSyncer;
    private final CloneStatusManager cloneStatusManager;

    private final boolean loadImmediately;
    private final boolean autoSyncInventory;

    private final List<Object> mocks = new ArrayList<>();

    private Environment(
        TestServerInventoryView clusterInventory,
        CoordinatorDynamicConfig dynamicConfig,
        boolean loadImmediately,
        boolean autoSyncInventory,
        String balancerStrategy
    )
    {
      this.inventory = clusterInventory;
      this.loadImmediately = loadImmediately;
      this.autoSyncInventory = autoSyncInventory;
      this.executorFactory = new ExecutorFactory(loadImmediately);
      this.coordinatorInventoryView = autoSyncInventory
                                      ? clusterInventory
                                      : new TestServerInventoryView();
      HttpClient httpClient = new TestSegmentLoadingHttpClient(
          OBJECT_MAPPER,
          clusterInventory::getChangeHandlerForHost,
          executorFactory.create(1, ExecutorFactory.HISTORICAL_LOADER)
      );

      this.coordinatorConfig = new DruidCoordinatorConfig(
          new CoordinatorRunConfig(new Duration(1L), Duration.standardMinutes(1)),
          new CoordinatorPeriodConfig(null, null),
          CoordinatorKillConfigs.DEFAULT,
          createBalancerStrategy(balancerStrategy),
          new HttpLoadQueuePeonConfig(null, null, null)
      );

      JacksonConfigManager jacksonConfigManager = mockConfigManager();
      setDynamicConfig(dynamicConfig);

      this.loadQueueTaskMaster = new LoadQueueTaskMaster(
          OBJECT_MAPPER,
          executorFactory.create(1, ExecutorFactory.LOAD_QUEUE_EXECUTOR),
          executorFactory.create(1, ExecutorFactory.LOAD_CALLBACK_EXECUTOR),
          coordinatorConfig.getHttpLoadQueuePeonConfig(),
          httpClient,
          () -> dynamicConfig
      );

      this.loadQueueManager =
          new SegmentLoadQueueManager(coordinatorInventoryView, loadQueueTaskMaster);

      this.lookupCoordinatorManager = EasyMock.createNiceMock(LookupCoordinatorManager.class);
      mocks.add(jacksonConfigManager);
      mocks.add(lookupCoordinatorManager);

      this.metadataManager = new MetadataManager(
          null,
          new CoordinatorConfigManager(jacksonConfigManager, null, null, null),
          segmentManager,
          null,
          ruleManager,
          null,
          null,
          NoopSegmentMetadataCache.instance()
      );

      this.configSyncer = EasyMock.niceMock(CoordinatorDynamicConfigSyncer.class);
      this.cloneStatusManager = EasyMock.niceMock(CloneStatusManager.class);

      mocks.add(configSyncer);
      mocks.add(cloneStatusManager);
    }

    private void setUp() throws Exception
    {
      EmittingLogger.registerEmitter(serviceEmitter);
      inventory.setUp();
      coordinatorInventoryView.setUp();
      lifecycle.start();
      leaderSelector.becomeLeader();
      EasyMock.replay(mocks.toArray());
    }

    private void tearDown()
    {
      EasyMock.verify(mocks.toArray());
      executorFactory.tearDown();
      lifecycle.stop();
    }

    private void setDynamicConfig(CoordinatorDynamicConfig dynamicConfig)
    {
      this.dynamicConfig.set(dynamicConfig);
    }

    private JacksonConfigManager mockConfigManager()
    {
      final JacksonConfigManager jacksonConfigManager
          = EasyMock.createMock(JacksonConfigManager.class);
      EasyMock.expect(
          jacksonConfigManager.watch(
              EasyMock.eq(CoordinatorDynamicConfig.CONFIG_KEY),
              EasyMock.eq(CoordinatorDynamicConfig.class),
              EasyMock.anyObject()
          )
      ).andReturn(dynamicConfig).anyTimes();

      EasyMock.expect(
          jacksonConfigManager.watch(
              EasyMock.eq(DruidCompactionConfig.CONFIG_KEY),
              EasyMock.eq(DruidCompactionConfig.class),
              EasyMock.anyObject()
          )
      ).andReturn(new AtomicReference<>(DruidCompactionConfig.empty())).anyTimes();

      return jacksonConfigManager;
    }

    private BalancerStrategyFactory createBalancerStrategy(String strategyName)
    {
      if (strategyName == null) {
        return new CostBalancerStrategyFactory();
      }

      switch (strategyName) {
        case "cost":
          return new CostBalancerStrategyFactory();
        case "cachingCost":
          return buildCachingCostBalancerStrategy();
        case "diskNormalized":
          return new DiskNormalizedCostBalancerStrategyFactory();
        case "random":
          return new RandomBalancerStrategyFactory();
        default:
          throw new IAE("Unknown balancer stratgy: " + strategyName);
      }
    }

    private BalancerStrategyFactory buildCachingCostBalancerStrategy()
    {
      try {
        return new CachingCostBalancerStrategyFactory(
            this.coordinatorInventoryView,
            this.lifecycle,
            new CachingCostBalancerStrategyConfig()
        );
      }
      catch (Exception e) {
        throw new ISE(e, "Error building balancer strategy");
      }
    }
  }

  /**
   * Implementation of {@link ScheduledExecutorFactory} used to create and keep
   * a handle on the various executors used inside the coordinator.
   */
  private static class ExecutorFactory implements ScheduledExecutorFactory
  {
    static final String HISTORICAL_LOADER = "historical-loader-%d";
    static final String LOAD_QUEUE_EXECUTOR = "load-queue-%d";
    static final String LOAD_CALLBACK_EXECUTOR = "load-callback-%d";
    static final String COORDINATOR_RUNNER = "Coordinator-Exec-HistoricalManagementDuties-%d";

    private final Map<String, BlockingExecutorService> blockingExecutors = new HashMap<>();
    private final boolean directExecution;

    private BlockingExecutorService historicalLoader;
    private BlockingExecutorService loadQueueExecutor;
    private BlockingExecutorService loadCallbackExecutor;
    private BlockingExecutorService historicalDutiesRunner;

    private ExecutorFactory(boolean directExecution)
    {
      this.directExecution = directExecution;
    }

    @Override
    public ScheduledExecutorService create(int corePoolSize, String nameFormat)
    {
      boolean isCoordinatorRunner = COORDINATOR_RUNNER.equals(nameFormat);

      // Coordinator running executor must always be blocked
      final ExecutorService executorService =
          (directExecution && !isCoordinatorRunner)
          ? new DirectExecutorService()
          : blockingExecutors.computeIfAbsent(nameFormat, BlockingExecutorService::new);

      return new WrappingScheduledExecutorService(nameFormat, executorService, !isCoordinatorRunner);
    }

    private BlockingExecutorService findExecutor(String nameFormat)
    {
      return blockingExecutors.get(nameFormat);
    }

    private void findExecutors()
    {
      historicalDutiesRunner = findExecutor(COORDINATOR_RUNNER);
      historicalLoader = findExecutor(HISTORICAL_LOADER);
      loadQueueExecutor = findExecutor(LOAD_QUEUE_EXECUTOR);
      loadCallbackExecutor = findExecutor(LOAD_CALLBACK_EXECUTOR);
    }

    private void tearDown()
    {
      blockingExecutors.values().forEach(BlockingExecutorService::shutdown);
    }
  }

  private static class SimOverlordClient extends NoopOverlordClient
  {
    private final TestSegmentsMetadataManager segmentsMetadataManager;

    private SimOverlordClient(TestSegmentsMetadataManager segmentsMetadataManager)
    {
      this.segmentsMetadataManager = segmentsMetadataManager;
    }

    @Override
    public ListenableFuture<SegmentUpdateResponse> markSegmentsAsUnused(
        String dataSource,
        SegmentsToUpdateFilter filter
    )
    {
      final Set<SegmentId> segmentsToUpdate = new HashSet<>();
      if (filter.getSegmentIds() != null) {
        for (String idString : filter.getSegmentIds()) {
          SegmentId segmentId = SegmentId.tryParse(dataSource, idString);
          if (segmentId != null) {
            segmentsToUpdate.add(segmentId);
          }
        }
      }
      int numUpdatedSegments = segmentsMetadataManager.markSegmentsAsUnused(segmentsToUpdate);
      return Futures.immediateFuture(new SegmentUpdateResponse(numUpdatedSegments));
    }
  }

}
