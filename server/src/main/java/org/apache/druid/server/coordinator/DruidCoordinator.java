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

package org.apache.druid.server.coordinator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.CoordinatorIndexingServiceDuty;
import org.apache.druid.guice.annotations.CoordinatorMetadataStoreManagementDuty;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.server.coordinator.duty.CollectSegmentAndServerStats;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.duty.CompactionSegmentSearchPolicy;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.MarkOvershadowedSegmentsAsUnused;
import org.apache.druid.server.coordinator.duty.RunRules;
import org.apache.druid.server.coordinator.duty.UnloadUnusedSegments;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.SegmentLoadingConfig;
import org.apache.druid.server.coordinator.loading.SegmentReplicaCount;
import org.apache.druid.server.coordinator.loading.SegmentReplicationStatus;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class DruidCoordinator
{
  /**
   * Orders newest segments (i.e. segments with most recent intervals) first.
   * Used by:
   * <ul>
   * <li>{@link RunRules} duty to prioritize assignment of more recent segments.
   * The order of segments matters because the {@link CoordinatorDynamicConfig#replicationThrottleLimit}
   * might cause only a few segments to be picked for replication in a coordinator run.
   * </li>
   * <li>{@link LoadQueuePeon}s to prioritize load of more recent segments.</li>
   * </ul>
   * It is presumed that more recent segments are queried more often and contain
   * more important data for users. This ordering thus ensures that if the cluster
   * has availability or loading problems, the most recent segments are made
   * available as soon as possible.
   */
  public static final Ordering<DataSegment> SEGMENT_COMPARATOR_RECENT_FIRST = Ordering
      .from(Comparators.intervalsByEndThenStart())
      .onResultOf(DataSegment::getInterval)
      .compound(Ordering.<DataSegment>natural())
      .reverse();

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinator.class);

  private final Object lock = new Object();
  private final DruidCoordinatorConfig config;
  private final JacksonConfigManager configManager;
  private final SegmentsMetadataManager segmentsMetadataManager;
  private final ServerInventoryView serverInventoryView;
  private final MetadataRuleManager metadataRuleManager;

  private final ServiceEmitter emitter;
  private final OverlordClient overlordClient;
  private final ScheduledExecutorService exec;
  private final LoadQueueTaskMaster taskMaster;
  private final ConcurrentHashMap<String, LoadQueuePeon> loadManagementPeons = new ConcurrentHashMap<>();
  private final SegmentLoadQueueManager loadQueueManager;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DruidNode self;
  private final Set<CoordinatorDuty> indexingServiceDuties;
  private final Set<CoordinatorDuty> metadataStoreManagementDuties;
  private final CoordinatorCustomDutyGroups customDutyGroups;
  private final BalancerStrategyFactory balancerStrategyFactory;
  private final LookupCoordinatorManager lookupCoordinatorManager;
  private final DruidLeaderSelector coordLeaderSelector;
  private final CompactSegments compactSegments;

  private volatile boolean started = false;

  /**
   * Used to determine count of under-replicated or unavailable segments.
   * Updated in each coordinator run in the {@link UpdateReplicationStatus} duty.
   * <p>
   * This might have stale information if coordinator runs are delayed. But as
   * long as the {@link SegmentsMetadataManager} has the latest information of
   * used segments, we would only have false negatives and not false positives.
   * In other words, we might report some segments as under-replicated or
   * unavailable even if they are fully replicated. But if a segment is reported
   * as fully replicated, it is guaranteed to be so.
   */
  private volatile SegmentReplicationStatus segmentReplicationStatus = null;

  private int cachedBalancerThreadNumber;
  private ListeningExecutorService balancerExec;

  public static final String HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP = "HistoricalManagementDuties";
  private static final String METADATA_STORE_MANAGEMENT_DUTIES_DUTY_GROUP = "MetadataStoreManagementDuties";
  private static final String INDEXING_SERVICE_DUTIES_DUTY_GROUP = "IndexingServiceDuties";
  private static final String COMPACT_SEGMENTS_DUTIES_DUTY_GROUP = "CompactSegmentsDuties";

  @Inject
  public DruidCoordinator(
      DruidCoordinatorConfig config,
      JacksonConfigManager configManager,
      SegmentsMetadataManager segmentsMetadataManager,
      ServerInventoryView serverInventoryView,
      MetadataRuleManager metadataRuleManager,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      OverlordClient overlordClient,
      LoadQueueTaskMaster taskMaster,
      SegmentLoadQueueManager loadQueueManager,
      ServiceAnnouncer serviceAnnouncer,
      @Self DruidNode self,
      @CoordinatorMetadataStoreManagementDuty Set<CoordinatorDuty> metadataStoreManagementDuties,
      @CoordinatorIndexingServiceDuty Set<CoordinatorDuty> indexingServiceDuties,
      CoordinatorCustomDutyGroups customDutyGroups,
      BalancerStrategyFactory balancerStrategyFactory,
      LookupCoordinatorManager lookupCoordinatorManager,
      @Coordinator DruidLeaderSelector coordLeaderSelector,
      CompactionSegmentSearchPolicy compactionSegmentSearchPolicy
  )
  {
    this.config = config;
    this.configManager = configManager;

    this.segmentsMetadataManager = segmentsMetadataManager;
    this.serverInventoryView = serverInventoryView;
    this.metadataRuleManager = metadataRuleManager;
    this.emitter = emitter;
    this.overlordClient = overlordClient;
    this.taskMaster = taskMaster;
    this.serviceAnnouncer = serviceAnnouncer;
    this.self = self;
    this.indexingServiceDuties = indexingServiceDuties;
    this.metadataStoreManagementDuties = metadataStoreManagementDuties;
    this.customDutyGroups = customDutyGroups;

    this.exec = scheduledExecutorFactory.create(1, "Coordinator-Exec--%d");

    this.balancerStrategyFactory = balancerStrategyFactory;
    this.lookupCoordinatorManager = lookupCoordinatorManager;
    this.coordLeaderSelector = coordLeaderSelector;
    this.compactSegments = initializeCompactSegmentsDuty(compactionSegmentSearchPolicy);
    this.loadQueueManager = loadQueueManager;
  }

  public boolean isLeader()
  {
    return coordLeaderSelector.isLeader();
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicatedCount(boolean useClusterView)
  {
    final Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();
    return computeUnderReplicated(dataSegments, useClusterView);
  }

  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicatedCount(
      Iterable<DataSegment> dataSegments,
      boolean useClusterView
  )
  {
    return computeUnderReplicated(dataSegments, useClusterView);
  }

  public Object2IntMap<String> getDatasourceToUnavailableSegmentCount()
  {
    if (segmentReplicationStatus == null) {
      return Object2IntMaps.emptyMap();
    }

    final Object2IntOpenHashMap<String> datasourceToUnavailableSegments = new Object2IntOpenHashMap<>();

    final Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();
    for (DataSegment segment : dataSegments) {
      SegmentReplicaCount replicaCount = segmentReplicationStatus.getReplicaCountsInCluster(segment.getId());
      if (replicaCount != null && replicaCount.totalLoaded() > 0) {
        datasourceToUnavailableSegments.addTo(segment.getDataSource(), 0);
      } else {
        datasourceToUnavailableSegments.addTo(segment.getDataSource(), 1);
      }
    }

    return datasourceToUnavailableSegments;
  }

  public Map<String, Double> getDatasourceToLoadStatus()
  {
    final Map<String, Double> loadStatus = new HashMap<>();
    final Collection<ImmutableDruidDataSource> dataSources =
        segmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments();

    for (ImmutableDruidDataSource dataSource : dataSources) {
      final Set<DataSegment> segments = Sets.newHashSet(dataSource.getSegments());
      final int numPublishedSegments = segments.size();

      // remove loaded segments
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        final DruidDataSource loadedView = druidServer.getDataSource(dataSource.getName());
        if (loadedView != null) {
          // This does not use segments.removeAll(loadedView.getSegments()) for performance reasons.
          // Please see https://github.com/apache/druid/pull/5632 for more info.
          for (DataSegment serverSegment : loadedView.getSegments()) {
            segments.remove(serverSegment);
          }
        }
      }
      final int numUnavailableSegments = segments.size();
      loadStatus.put(
          dataSource.getName(),
          (numPublishedSegments - numUnavailableSegments) * 100.0 / numPublishedSegments
      );
    }

    return loadStatus;
  }

  @Nullable
  public Integer getReplicationFactor(SegmentId segmentId)
  {
    if (segmentReplicationStatus == null) {
      return null;
    }
    SegmentReplicaCount replicaCountsInCluster = segmentReplicationStatus.getReplicaCountsInCluster(segmentId);
    return replicaCountsInCluster == null ? null : replicaCountsInCluster.required();
  }

  @Nullable
  public Long getTotalSizeOfSegmentsAwaitingCompaction(String dataSource)
  {
    return compactSegments.getTotalSizeOfSegmentsAwaitingCompaction(dataSource);
  }

  @Nullable
  public AutoCompactionSnapshot getAutoCompactionSnapshotForDataSource(String dataSource)
  {
    return compactSegments.getAutoCompactionSnapshot(dataSource);
  }

  public Map<String, AutoCompactionSnapshot> getAutoCompactionSnapshot()
  {
    return compactSegments.getAutoCompactionSnapshot();
  }

  private CoordinatorDynamicConfig getDynamicConfigs()
  {
    return CoordinatorDynamicConfig.current(configManager);
  }

  private CoordinatorCompactionConfig getCompactionConfig()
  {
    return CoordinatorCompactionConfig.current(configManager);
  }

  public String getCurrentLeader()
  {
    return coordLeaderSelector.getCurrentLeader();
  }

  @VisibleForTesting
  public int getCachedBalancerThreadNumber()
  {
    return cachedBalancerThreadNumber;
  }

  @VisibleForTesting
  public ListeningExecutorService getBalancerExec()
  {
    return balancerExec;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }
      started = true;

      coordLeaderSelector.registerListener(
          new DruidLeaderSelector.Listener()
          {
            @Override
            public void becomeLeader()
            {
              DruidCoordinator.this.becomeLeader();
            }

            @Override
            public void stopBeingLeader()
            {
              DruidCoordinator.this.stopBeingLeader();
            }
          }
      );
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      coordLeaderSelector.unregisterListener();

      started = false;

      exec.shutdownNow();

      if (balancerExec != null) {
        balancerExec.shutdownNow();
      }
    }
  }

  public void runCompactSegmentsDuty()
  {
    final int startingLeaderCounter = coordLeaderSelector.localTerm();
    DutiesRunnable compactSegmentsDuty = new DutiesRunnable(
        makeCompactSegmentsDuty(),
        startingLeaderCounter,
        COMPACT_SEGMENTS_DUTIES_DUTY_GROUP,
        null
    );
    compactSegmentsDuty.run();
  }

  private Map<String, Object2LongMap<String>> computeUnderReplicated(
      Iterable<DataSegment> dataSegments,
      boolean computeUsingClusterView
  )
  {
    if (segmentReplicationStatus == null) {
      return Collections.emptyMap();
    } else {
      return segmentReplicationStatus.getTierToDatasourceToUnderReplicated(dataSegments, !computeUsingClusterView);
    }
  }

  private void becomeLeader()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      log.info(
          "I am the leader of the coordinators, all must bow! Starting coordination in [%s].",
          config.getCoordinatorStartDelay()
      );

      segmentsMetadataManager.startPollingDatabasePeriodically();
      metadataRuleManager.start();
      lookupCoordinatorManager.start();
      serviceAnnouncer.announce(self);
      final int startingLeaderCounter = coordLeaderSelector.localTerm();

      final List<DutiesRunnable> dutiesRunnables = new ArrayList<>();
      dutiesRunnables.add(
          new DutiesRunnable(
              makeHistoricalManagementDuties(),
              startingLeaderCounter,
              HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP,
              config.getCoordinatorPeriod()
          )
      );
      if (overlordClient != null) {
        dutiesRunnables.add(
            new DutiesRunnable(
                makeIndexingServiceDuties(),
                startingLeaderCounter,
                INDEXING_SERVICE_DUTIES_DUTY_GROUP,
                config.getCoordinatorIndexingPeriod()
            )
        );
      }
      dutiesRunnables.add(
          new DutiesRunnable(
              makeMetadataStoreManagementDuties(),
              startingLeaderCounter,
              METADATA_STORE_MANAGEMENT_DUTIES_DUTY_GROUP,
              config.getCoordinatorMetadataStoreManagementPeriod()
          )
      );

      for (CoordinatorCustomDutyGroup customDutyGroup : customDutyGroups.getCoordinatorCustomDutyGroups()) {
        dutiesRunnables.add(
            new DutiesRunnable(
                customDutyGroup.getCustomDutyList(),
                startingLeaderCounter,
                customDutyGroup.getName(),
                customDutyGroup.getPeriod()
            )
        );
        log.info(
            "Done making custom coordinator duties [%s] for group [%s]",
            customDutyGroup.getCustomDutyList().stream()
                           .map(duty -> duty.getClass().getName()).collect(Collectors.toList()),
            customDutyGroup.getName()
        );
      }

      for (final DutiesRunnable dutiesRunnable : dutiesRunnables) {
        // CompactSegmentsDuty can takes a non trival amount of time to complete.
        // Hence, we schedule at fixed rate to make sure the other tasks still run at approximately every
        // config.getCoordinatorIndexingPeriod() period. Note that cautious should be taken
        // if setting config.getCoordinatorIndexingPeriod() lower than the default value.
        ScheduledExecutors.scheduleAtFixedRate(
            exec,
            config.getCoordinatorStartDelay(),
            dutiesRunnable.getPeriod(),
            () -> {
              if (coordLeaderSelector.isLeader()
                  && startingLeaderCounter == coordLeaderSelector.localTerm()) {
                dutiesRunnable.run();
              }

              // Check if we are still leader before re-scheduling
              if (coordLeaderSelector.isLeader()
                  && startingLeaderCounter == coordLeaderSelector.localTerm()) {
                return ScheduledExecutors.Signal.REPEAT;
              } else {
                return ScheduledExecutors.Signal.STOP;
              }
            }
        );
      }
    }
  }

  private void stopBeingLeader()
  {
    synchronized (lock) {

      log.info("I am no longer the leader...");

      for (String server : loadManagementPeons.keySet()) {
        LoadQueuePeon peon = loadManagementPeons.remove(server);
        peon.stop();
      }
      loadManagementPeons.clear();

      serviceAnnouncer.unannounce(self);
      lookupCoordinatorManager.stop();
      metadataRuleManager.stop();
      segmentsMetadataManager.stopPollingDatabasePeriodically();

      if (balancerExec != null) {
        balancerExec.shutdownNow();
        balancerExec = null;
      }
    }
  }

  @VisibleForTesting
  protected void initBalancerExecutor()
  {
    final int currentNumber = getDynamicConfigs().getBalancerComputeThreads();

    if (balancerExec == null) {
      balancerExec = createNewBalancerExecutor(currentNumber);
    } else if (cachedBalancerThreadNumber != currentNumber) {
      log.info(
          "balancerComputeThreads has changed from [%d] to [%d], recreating the thread pool.",
          cachedBalancerThreadNumber,
          currentNumber
      );
      balancerExec.shutdownNow();
      balancerExec = createNewBalancerExecutor(currentNumber);
    }
  }

  private ListeningExecutorService createNewBalancerExecutor(int numThreads)
  {
    cachedBalancerThreadNumber = numThreads;
    return MoreExecutors.listeningDecorator(
        Execs.multiThreaded(numThreads, "coordinator-cost-balancer-%s")
    );
  }

  private List<CoordinatorDuty> makeHistoricalManagementDuties()
  {
    return ImmutableList.of(
        new UpdateCoordinatorStateAndPrepareCluster(),
        new RunRules(segmentsMetadataManager::markSegmentsAsUnused),
        new UpdateReplicationStatus(),
        new UnloadUnusedSegments(loadQueueManager),
        new MarkOvershadowedSegmentsAsUnused(segmentsMetadataManager::markSegmentsAsUnused),
        new BalanceSegments(),
        new CollectSegmentAndServerStats(DruidCoordinator.this)
    );
  }

  @VisibleForTesting
  List<CoordinatorDuty> makeIndexingServiceDuties()
  {
    final List<CoordinatorDuty> duties = new ArrayList<>(indexingServiceDuties);
    // CompactSegmentsDuty should be the last duty as it can take a long time to complete
    // We do not have to add compactSegments if it is already enabled in the custom duty group
    if (getCompactSegmentsDutyFromCustomGroups().isEmpty()) {
      duties.addAll(makeCompactSegmentsDuty());
    }
    log.debug(
        "Initialized indexing service duties [%s].",
        duties.stream().map(duty -> duty.getClass().getName()).collect(Collectors.toList())
    );
    return ImmutableList.copyOf(duties);
  }

  private List<CoordinatorDuty> makeMetadataStoreManagementDuties()
  {
    List<CoordinatorDuty> duties = ImmutableList.copyOf(metadataStoreManagementDuties);
    log.debug(
        "Initialized metadata store management duties [%s].",
        duties.stream().map(duty -> duty.getClass().getName()).collect(Collectors.toList())
    );
    return duties;
  }

  @VisibleForTesting
  CompactSegments initializeCompactSegmentsDuty(CompactionSegmentSearchPolicy compactionSegmentSearchPolicy)
  {
    List<CompactSegments> compactSegmentsDutyFromCustomGroups = getCompactSegmentsDutyFromCustomGroups();
    if (compactSegmentsDutyFromCustomGroups.isEmpty()) {
      return new CompactSegments(config, compactionSegmentSearchPolicy, overlordClient);
    } else {
      if (compactSegmentsDutyFromCustomGroups.size() > 1) {
        log.warn(
            "More than one compactSegments duty is configured in the Coordinator Custom Duty Group."
            + " The first duty will be picked up."
        );
      }
      return compactSegmentsDutyFromCustomGroups.get(0);
    }
  }

  @VisibleForTesting
  List<CompactSegments> getCompactSegmentsDutyFromCustomGroups()
  {
    return customDutyGroups.getCoordinatorCustomDutyGroups()
                           .stream()
                           .flatMap(coordinatorCustomDutyGroup ->
                                        coordinatorCustomDutyGroup.getCustomDutyList().stream())
                           .filter(duty -> duty instanceof CompactSegments)
                           .map(duty -> (CompactSegments) duty)
                           .collect(Collectors.toList());
  }

  private List<CoordinatorDuty> makeCompactSegmentsDuty()
  {
    return ImmutableList.of(compactSegments);
  }

  private class DutiesRunnable implements Runnable
  {
    private final DateTime coordinatorStartTime = DateTimes.nowUtc();
    private final List<? extends CoordinatorDuty> duties;
    private final int startingLeaderCounter;
    private final String dutyGroupName;
    private final Duration period;

    DutiesRunnable(
        List<? extends CoordinatorDuty> duties,
        final int startingLeaderCounter,
        String alias,
        Duration period
    )
    {
      this.duties = duties;
      this.startingLeaderCounter = startingLeaderCounter;
      this.dutyGroupName = alias;
      this.period = period;
    }

    @Override
    public void run()
    {
      try {
        log.info("Starting coordinator run for group [%s]", dutyGroupName);
        final Stopwatch groupRunTime = Stopwatch.createStarted();

        synchronized (lock) {
          if (!coordLeaderSelector.isLeader()) {
            log.info("LEGGO MY EGGO. [%s] is leader.", coordLeaderSelector.getCurrentLeader());
            stopBeingLeader();
            return;
          }
        }

        List<Boolean> allStarted = Arrays.asList(
            segmentsMetadataManager.isPollingDatabasePeriodically(),
            serverInventoryView.isStarted()
        );
        for (Boolean aBoolean : allStarted) {
          if (!aBoolean) {
            log.error("InventoryManagers not started[%s]", allStarted);
            stopBeingLeader();
            return;
          }
        }

        // Do coordinator stuff.
        DataSourcesSnapshot dataSourcesSnapshot = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments();

        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams
                .newBuilder(coordinatorStartTime)
                .withDatabaseRuleManager(metadataRuleManager)
                .withSnapshotOfDataSourcesWithAllUsedSegments(dataSourcesSnapshot)
                .withDynamicConfigs(getDynamicConfigs())
                .withCompactionConfig(getCompactionConfig())
                .withEmitter(emitter)
                .build();
        log.info(
            "Initialized run params for group [%s] with [%,d] used segments in [%d] datasources.",
            dutyGroupName, params.getUsedSegments().size(), dataSourcesSnapshot.getDataSourcesMap().size()
        );

        boolean coordinationPaused = getDynamicConfigs().getPauseCoordination();
        if (coordinationPaused
            && coordLeaderSelector.isLeader()
            && startingLeaderCounter == coordLeaderSelector.localTerm()) {

          log.info("Coordination has been paused. Duties will not run until coordination is resumed.");
        }

        final Stopwatch dutyRunTime = Stopwatch.createUnstarted();
        for (CoordinatorDuty duty : duties) {
          // Don't read state and run state in the same duty otherwise racy conditions may exist
          if (!coordinationPaused
              && coordLeaderSelector.isLeader()
              && startingLeaderCounter == coordLeaderSelector.localTerm()) {

            dutyRunTime.reset().start();
            params = duty.run(params);
            dutyRunTime.stop();

            final String dutyName = duty.getClass().getName();
            if (params == null) {
              log.info("Stopping run for group [%s] on request of duty [%s].", dutyGroupName, dutyName);
              return;
            } else {
              final RowKey rowKey = RowKey.of(Dimension.DUTY, dutyName);
              final long dutyRunMillis = dutyRunTime.elapsed(TimeUnit.MILLISECONDS);
              params.getCoordinatorStats().add(Stats.CoordinatorRun.DUTY_RUN_TIME, rowKey, dutyRunMillis);
            }
          }
        }

        // Emit stats collected from all duties
        final CoordinatorRunStats allStats = params.getCoordinatorStats();
        if (allStats.rowCount() > 0) {
          final AtomicInteger emittedCount = new AtomicInteger();
          allStats.forEachStat(
              (stat, dimensions, value) -> {
                if (stat.shouldEmit()) {
                  emitStat(stat, dimensions.getValues(), value);
                  emittedCount.incrementAndGet();
                }
              }
          );

          log.info(
              "Emitted [%d] stats for group [%s]. All collected stats:%s\n",
              emittedCount.get(), dutyGroupName, allStats.buildStatsTable()
          );
        }

        // Emit the runtime of the full DutiesRunnable
        final long runMillis = groupRunTime.stop().elapsed(TimeUnit.MILLISECONDS);
        emitStat(Stats.CoordinatorRun.GROUP_RUN_TIME, Collections.emptyMap(), runMillis);
        log.info("Finished coordinator run for group [%s] in [%d] ms", dutyGroupName, runMillis);
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
      }
    }

    private void emitStat(CoordinatorStat stat, Map<Dimension, String> dimensionValues, long value)
    {
      ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder()
          .setDimension(Dimension.DUTY_GROUP.reportedName(), dutyGroupName);
      dimensionValues.forEach(
          (dim, dimValue) -> eventBuilder.setDimension(dim.reportedName(), dimValue)
      );
      emitter.emit(eventBuilder.build(stat.getMetricName(), value));
    }

    Duration getPeriod()
    {
      return period;
    }

    @Override
    public String toString()
    {
      return "DutiesRunnable{group='" + dutyGroupName + '\'' + '}';
    }
  }

  /**
   * This duty does the following:
   * <ul>
   *   <li>Prepares an immutable {@link DruidCluster} consisting of {@link ServerHolder}s
   *   which represent the current state of the servers in the cluster.</li>
   *   <li>Starts and stops load peons for new and disappeared servers respectively.</li>
   *   <li>Cancels in-progress loads on all decommissioning servers. This is done
   *   here to ensure that under-replicated segments are assigned to active servers
   *   in the {@link RunRules} duty after this.</li>
   *   <li>Initializes the {@link BalancerStrategy} for the run.</li>
   * </ul>
   *
   * @see #makeHistoricalManagementDuties() for the order of duties
   */
  private class UpdateCoordinatorStateAndPrepareCluster implements CoordinatorDuty
  {
    @Nullable
    @Override
    public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
    {
      List<ImmutableDruidServer> currentServers = prepareCurrentServers();

      startPeonsForNewServers(currentServers);
      stopPeonsForDisappearedServers(currentServers);

      final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
      final SegmentLoadingConfig segmentLoadingConfig = params.getSegmentLoadingConfig();

      final DruidCluster cluster = prepareCluster(dynamicConfig, segmentLoadingConfig, currentServers);
      cancelLoadsOnDecommissioningServers(cluster);

      initBalancerExecutor();
      final BalancerStrategy balancerStrategy = balancerStrategyFactory.createBalancerStrategy(balancerExec);
      log.info(
          "Using balancer strategy [%s] with round-robin assignment [%s] and debug dimensions [%s].",
          balancerStrategy.getClass().getSimpleName(),
          segmentLoadingConfig.isUseRoundRobinSegmentAssignment(),
          dynamicConfig.getDebugDimensions()
      );

      return params.buildFromExisting()
                   .withDruidCluster(cluster)
                   .withBalancerStrategy(balancerStrategy)
                   .withSegmentAssignerUsing(loadQueueManager)
                   .build();
    }

    /**
     * Cancels all load/move operations on decommissioning servers. This should
     * be done before initializing the SegmentReplicantLookup so that
     * under-replicated segments can be assigned in the current run itself.
     */
    private void cancelLoadsOnDecommissioningServers(DruidCluster cluster)
    {
      final AtomicInteger cancelledCount = new AtomicInteger(0);
      final List<ServerHolder> decommissioningServers
          = cluster.getAllServers().stream()
                   .filter(ServerHolder::isDecommissioning)
                   .collect(Collectors.toList());

      for (ServerHolder server : decommissioningServers) {
        server.getQueuedSegments().forEach(
            (segment, action) -> {
              // Cancel the operation if it is a type of load
              if (action.isLoad() && server.cancelOperation(action, segment)) {
                cancelledCount.incrementAndGet();
              }
            }
        );
      }

      if (cancelledCount.get() > 0) {
        log.info(
            "Cancelled [%d] load/move operations on [%d] decommissioning servers.",
            cancelledCount.get(), decommissioningServers.size()
        );
      }
    }

    List<ImmutableDruidServer> prepareCurrentServers()
    {
      List<ImmutableDruidServer> currentServers = serverInventoryView
          .getInventory()
          .stream()
          .filter(DruidServer::isSegmentReplicationOrBroadcastTarget)
          .map(DruidServer::toImmutableDruidServer)
          .collect(Collectors.toList());

      if (log.isDebugEnabled()) {
        // Display info about all segment-replicatable (historical and bridge) servers
        log.debug("Servers");
        for (ImmutableDruidServer druidServer : currentServers) {
          log.debug("  %s", druidServer);
          log.debug("    -- DataSources");
          for (ImmutableDruidDataSource druidDataSource : druidServer.getDataSources()) {
            log.debug("    %s", druidDataSource);
          }
        }
      }
      return currentServers;
    }

    void startPeonsForNewServers(List<ImmutableDruidServer> currentServers)
    {
      for (ImmutableDruidServer server : currentServers) {
        loadManagementPeons.computeIfAbsent(server.getName(), serverName -> {
          LoadQueuePeon loadQueuePeon = taskMaster.giveMePeon(server);
          loadQueuePeon.start();
          log.debug("Created LoadQueuePeon for server[%s].", server.getName());
          return loadQueuePeon;
        });
      }
    }

    DruidCluster prepareCluster(
        CoordinatorDynamicConfig dynamicConfig,
        SegmentLoadingConfig segmentLoadingConfig,
        List<ImmutableDruidServer> currentServers
    )
    {
      final Set<String> decommissioningServers = dynamicConfig.getDecommissioningNodes();
      final DruidCluster.Builder cluster = DruidCluster.builder();
      for (ImmutableDruidServer server : currentServers) {
        cluster.add(
            new ServerHolder(
                server,
                loadManagementPeons.get(server.getName()),
                decommissioningServers.contains(server.getHost()),
                segmentLoadingConfig.getMaxSegmentsInLoadQueue(),
                segmentLoadingConfig.getMaxLifetimeInLoadQueue()
            )
        );
      }
      return cluster.build();
    }

    void stopPeonsForDisappearedServers(List<ImmutableDruidServer> servers)
    {
      final Set<String> disappeared = Sets.newHashSet(loadManagementPeons.keySet());
      for (ImmutableDruidServer server : servers) {
        disappeared.remove(server.getName());
      }
      for (String name : disappeared) {
        log.debug("Removing listener for server[%s] which is no longer there.", name);
        LoadQueuePeon peon = loadManagementPeons.remove(name);
        peon.stop();
      }
    }
  }

  /**
   * Updates replication status of all used segments. This duty must run after
   * {@link RunRules} so that the number of required replicas for all segments
   * has been determined.
   */
  private class UpdateReplicationStatus implements CoordinatorDuty
  {

    @Override
    public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
    {
      segmentReplicationStatus = params.getSegmentReplicationStatus();
      return params;
    }

  }
}

