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
import org.apache.druid.client.DruidServer;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.curator.ZkEnablementConfig;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.coordinator.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.CachingCostBalancerStrategyConfig;
import org.apache.druid.server.coordinator.CachingCostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.TestDruidCoordinatorConfig;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link CoordinatorSimulation}.
 *
 * @see #builder()
 */
public class CoordinatorSimulationImpl implements CoordinatorSimulation,
    CoordinatorSimulation.CoordinatorState, CoordinatorSimulation.ClusterState
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper()
      .setInjectableValues(
          new InjectableValues.Std().addValue(
              DataSegment.PruneSpecsHolder.class,
              DataSegment.PruneSpecsHolder.DEFAULT
          )
      );
  private static final long DEFAULT_COORDINATOR_PERIOD = 100L;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private final DruidCoordinator coordinator;
  private final Environment env;

  private CoordinatorSimulationImpl(DruidCoordinator coordinator, Environment env)
  {
    this.coordinator = coordinator;
    this.env = env;
  }

  /**
   * Creates a new simulation builder.
   */
  public static Builder builder()
  {
    return new Builder();
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

  // Blocked mode processing invocations

  @Override
  public void runCycle()
  {
    verifySimulationRunning();
    env.serviceEmitter.flush();
    env.executorFactory.coordinatorRunner.finishNextPendingTasks(1);
  }

  @Override
  public void syncInventoryView()
  {
    verifySimulationRunning();
    env.coordinatorInventoryView.sync(env.historicalInventoryView);
  }

  @Override
  public DruidServer getInventoryView(String serverName)
  {
    return env.coordinatorInventoryView.getInventoryValue(serverName);
  }

  @Override
  public void loadQueuedSegments()
  {
    verifySimulationRunning();

    final BlockingExecutorService loadQueueExecutor = env.executorFactory.loadQueueExecutor;
    while (loadQueueExecutor.hasPendingTasks()) {
      // Drain all the items from the load queue executor
      // This sends at most 1 load/drop request to each server
      loadQueueExecutor.finishAllPendingTasks();

      // Load all the queued segments, handle their responses and execute callbacks
      int loadedSegments = env.executorFactory.historicalLoader.finishAllPendingTasks();
      loadQueueExecutor.finishNextPendingTasks(loadedSegments);
      env.executorFactory.loadCallbackExecutor.finishAllPendingTasks();
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
    return coordinator.getLoadStatus().get(datasource);
  }

  @Override
  public List<Event> getMetricEvents()
  {
    return new ArrayList<>(env.serviceEmitter.getEvents());
  }

  /**
   * Builder for a coordinator simulation.
   */
  public static class Builder
  {
    private BalancerStrategyFactory balancerStrategyFactory;
    private List<DruidServer> servers;
    private List<DataSegment> segments;
    private final Map<String, List<Rule>> datasourceRules = new HashMap<>();

    public Builder balancer(BalancerStrategyFactory strategyFactory)
    {
      this.balancerStrategyFactory = strategyFactory;
      return this;
    }

    public Builder servers(List<DruidServer> servers)
    {
      this.servers = servers;
      return this;
    }

    public Builder segments(List<DataSegment> segments)
    {
      this.segments = segments;
      return this;
    }

    public Builder rulesForDatasource(String datasource, List<Rule> rules)
    {
      this.datasourceRules.put(datasource, rules);
      return this;
    }

    public CoordinatorSimulationImpl build()
    {
      Preconditions.checkArgument(
          servers != null && !servers.isEmpty(),
          "Cannot run simulation for an empty cluster"
      );

      // Prepare the config
      final DruidCoordinatorConfig coordinatorConfig = new TestDruidCoordinatorConfig.Builder()
          .withCoordinatorStartDelay(new Duration(1L))
          .withCoordinatorPeriod(new Duration(DEFAULT_COORDINATOR_PERIOD))
          .withCoordinatorKillPeriod(new Duration(DEFAULT_COORDINATOR_PERIOD))
          .withLoadQueuePeonRepeatDelay(new Duration("PT0S"))
          .withLoadQueuePeonType("http")
          .withCoordinatorKillIgnoreDurationToRetain(false)
          .build();

      // Prepare the environment
      final TestServerInventoryView serverInventoryView = new TestServerInventoryView();
      servers.forEach(serverInventoryView::addServer);

      final TestSegmentsMetadataManager segmentManager = new TestSegmentsMetadataManager();
      if (segments != null) {
        segments.forEach(segmentManager::addSegment);
      }

      final TestMetadataRuleManager ruleManager = new TestMetadataRuleManager();
      datasourceRules.forEach(
          (datasource, rules) ->
              ruleManager.overrideRule(datasource, rules, null)
      );

      final Environment env = new Environment(
          new TestDruidLeaderSelector(),
          serverInventoryView,
          segmentManager,
          ruleManager,
          coordinatorConfig
      );

      // Build the coordinator
      final DruidCoordinator coordinator = new DruidCoordinator(
          env.coordinatorConfig,
          null,
          env.jacksonConfigManager,
          env.segmentManager,
          env.historicalInventoryView,
          env.ruleManager,
          () -> null,
          env.serviceEmitter,
          env.executorFactory,
          null,
          env.loadQueueTaskMaster,
          new ServiceAnnouncer.Noop(),
          null,
          Collections.emptySet(),
          null,
          new CoordinatorCustomDutyGroups(Collections.emptySet()),
          balancerStrategyFactory != null ? balancerStrategyFactory
                                          : buildCachingCostBalancerStrategy(env),
          env.lookupCoordinatorManager,
          env.leaderSelector,
          OBJECT_MAPPER,
          ZkEnablementConfig.ENABLED
      );

      return new CoordinatorSimulationImpl(coordinator, env);
    }

    private BalancerStrategyFactory buildCachingCostBalancerStrategy(Environment env)
    {
      try {
        return new CachingCostBalancerStrategyFactory(
            env.coordinatorInventoryView,
            env.lifecycle,
            new CachingCostBalancerStrategyConfig()
        );
      }
      catch (Exception e) {
        throw new ISE(e, "Error building balancer strategy");
      }
    }
  }

  /**
   * Environment for a coordinator simulation.
   */
  private static class Environment
  {
    private final Lifecycle lifecycle = new Lifecycle("coord-sim");

    // Executors
    private final ExecutorFactory executorFactory;

    private final TestDruidLeaderSelector leaderSelector;
    private final TestSegmentsMetadataManager segmentManager;
    private final TestMetadataRuleManager ruleManager;
    private final TestServerInventoryView historicalInventoryView;

    private final LoadQueueTaskMaster loadQueueTaskMaster;
    private final StubServiceEmitter serviceEmitter
        = new StubServiceEmitter("coordinator", "coordinator");
    private final TestServerInventoryView coordinatorInventoryView;

    private final JacksonConfigManager jacksonConfigManager;
    private final LookupCoordinatorManager lookupCoordinatorManager;
    private final DruidCoordinatorConfig coordinatorConfig;

    private final List<Object> mocks = new ArrayList<>();

    private Environment(
        TestDruidLeaderSelector leaderSelector,
        TestServerInventoryView historicalInventoryView,
        TestSegmentsMetadataManager segmentManager,
        TestMetadataRuleManager ruleManager,
        DruidCoordinatorConfig coordinatorConfig
    )
    {
      this.leaderSelector = leaderSelector;
      this.historicalInventoryView = historicalInventoryView;
      this.segmentManager = segmentManager;
      this.ruleManager = ruleManager;
      this.coordinatorConfig = coordinatorConfig;

      this.executorFactory = new ExecutorFactory();
      this.coordinatorInventoryView = new TestServerInventoryView();
      HttpClient httpClient = new TestSegmentLoadingHttpClient(
          OBJECT_MAPPER,
          historicalInventoryView::getChangeHandlerForHost,
          executorFactory.create(1, ExecutorFactory.HISTORICAL_LOADER)
      );

      this.loadQueueTaskMaster = new LoadQueueTaskMaster(
          null,
          OBJECT_MAPPER,
          executorFactory.create(1, ExecutorFactory.LOAD_QUEUE_EXECUTOR),
          executorFactory.create(1, ExecutorFactory.LOAD_CALLBACK_EXECUTOR),
          coordinatorConfig,
          httpClient,
          null
      );

      this.jacksonConfigManager = mockConfigManager();
      this.lookupCoordinatorManager = EasyMock.createNiceMock(LookupCoordinatorManager.class);
      mocks.add(jacksonConfigManager);
      mocks.add(lookupCoordinatorManager);
    }

    private void setUp() throws Exception
    {
      EmittingLogger.registerEmitter(serviceEmitter);
      historicalInventoryView.setUp();
      coordinatorInventoryView.sync(historicalInventoryView);
      coordinatorInventoryView.setUp();
      lifecycle.start();
      executorFactory.setUp();
      leaderSelector.becomeLeader();
      EasyMock.replay(mocks.toArray());
    }

    private void tearDown()
    {
      EasyMock.verify(mocks.toArray());
      lifecycle.stop();
    }

    private JacksonConfigManager mockConfigManager()
    {
      final CoordinatorDynamicConfig dynamicConfig =
          CoordinatorDynamicConfig
              .builder()
              .withMaxSegmentsToMove(100)
              .withMaxSegmentsInNodeLoadingQueue(0)
              .withReplicationThrottleLimit(100000)
              .build();

      final JacksonConfigManager jacksonConfigManager
          = EasyMock.createMock(JacksonConfigManager.class);
      EasyMock.expect(
          jacksonConfigManager.watch(
              EasyMock.eq(CoordinatorDynamicConfig.CONFIG_KEY),
              EasyMock.eq(CoordinatorDynamicConfig.class),
              EasyMock.anyObject()
          )
      ).andReturn(new AtomicReference<>(dynamicConfig)).anyTimes();

      EasyMock.expect(
          jacksonConfigManager.watch(
              EasyMock.eq(CoordinatorCompactionConfig.CONFIG_KEY),
              EasyMock.eq(CoordinatorCompactionConfig.class),
              EasyMock.anyObject()
          )
      ).andReturn(new AtomicReference<>(CoordinatorCompactionConfig.empty())).anyTimes();

      return jacksonConfigManager;
    }
  }

  private static class ExecutorFactory implements ScheduledExecutorFactory
  {
    static final String HISTORICAL_LOADER = "historical-loader-%d";
    static final String LOAD_QUEUE_EXECUTOR = "load-queue-%d";
    static final String LOAD_CALLBACK_EXECUTOR = "load-callback-%d";
    static final String COORDINATOR_RUNNER = "Coordinator-Exec--%d";

    private final Map<String, BlockingExecutorService> blockingExecutors = new HashMap<>();

    private BlockingExecutorService historicalLoader;
    private BlockingExecutorService loadQueueExecutor;
    private BlockingExecutorService loadCallbackExecutor;
    private BlockingExecutorService coordinatorRunner;

    @Override
    public ScheduledExecutorService create(int corePoolSize, String nameFormat)
    {
      boolean isCoordinatorRunner = COORDINATOR_RUNNER.equals(nameFormat);
      return blockingExecutors.computeIfAbsent(
          nameFormat,
          k -> new BlockingExecutorService(k, !isCoordinatorRunner)
      );
    }

    private BlockingExecutorService findExecutor(String nameFormat)
    {
      return blockingExecutors.get(nameFormat);
    }

    private void setUp()
    {
      coordinatorRunner = findExecutor(COORDINATOR_RUNNER);
      historicalLoader = findExecutor(HISTORICAL_LOADER);
      loadQueueExecutor = findExecutor(LOAD_QUEUE_EXECUTOR);
      loadCallbackExecutor = findExecutor(LOAD_CALLBACK_EXECUTOR);
    }
  }

}
