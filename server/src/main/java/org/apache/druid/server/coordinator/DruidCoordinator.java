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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.compact.CompactionSegmentSearchPolicy;
import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.server.coordinator.duty.CollectSegmentAndServerStats;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.KillAuditLog;
import org.apache.druid.server.coordinator.duty.KillCompactionConfig;
import org.apache.druid.server.coordinator.duty.KillDatasourceMetadata;
import org.apache.druid.server.coordinator.duty.KillRules;
import org.apache.druid.server.coordinator.duty.KillStalePendingSegments;
import org.apache.druid.server.coordinator.duty.KillSupervisors;
import org.apache.druid.server.coordinator.duty.KillUnusedSegments;
import org.apache.druid.server.coordinator.duty.MarkEternityTombstonesAsUnused;
import org.apache.druid.server.coordinator.duty.MarkOvershadowedSegmentsAsUnused;
import org.apache.druid.server.coordinator.duty.PrepareBalancerAndLoadQueues;
import org.apache.druid.server.coordinator.duty.RunRules;
import org.apache.druid.server.coordinator.duty.UnloadUnusedSegments;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
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
import java.util.concurrent.ScheduledExecutorService;
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
  private final MetadataManager metadataManager;
  private final ServerInventoryView serverInventoryView;

  private final ServiceEmitter emitter;
  private final OverlordClient overlordClient;
  private final ScheduledExecutorFactory executorFactory;
  private final Map<String, ScheduledExecutorService> dutyGroupExecutors = new HashMap<>();
  private final LoadQueueTaskMaster taskMaster;
  private final SegmentLoadQueueManager loadQueueManager;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DruidNode self;
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

  public static final String HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP = "HistoricalManagementDuties";
  private static final String METADATA_STORE_MANAGEMENT_DUTIES_DUTY_GROUP = "MetadataStoreManagementDuties";
  private static final String INDEXING_SERVICE_DUTIES_DUTY_GROUP = "IndexingServiceDuties";
  private static final String COMPACT_SEGMENTS_DUTIES_DUTY_GROUP = "CompactSegmentsDuties";

  @Inject
  public DruidCoordinator(
      DruidCoordinatorConfig config,
      MetadataManager metadataManager,
      ServerInventoryView serverInventoryView,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      OverlordClient overlordClient,
      LoadQueueTaskMaster taskMaster,
      SegmentLoadQueueManager loadQueueManager,
      ServiceAnnouncer serviceAnnouncer,
      @Self DruidNode self,
      CoordinatorCustomDutyGroups customDutyGroups,
      BalancerStrategyFactory balancerStrategyFactory,
      LookupCoordinatorManager lookupCoordinatorManager,
      @Coordinator DruidLeaderSelector coordLeaderSelector,
      CompactionSegmentSearchPolicy compactionSegmentSearchPolicy
  )
  {
    this.config = config;
    this.metadataManager = metadataManager;
    this.serverInventoryView = serverInventoryView;
    this.emitter = emitter;
    this.overlordClient = overlordClient;
    this.taskMaster = taskMaster;
    this.serviceAnnouncer = serviceAnnouncer;
    this.self = self;
    this.customDutyGroups = customDutyGroups;

    this.executorFactory = scheduledExecutorFactory;

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
    return taskMaster.getAllPeons();
  }

  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicatedCount(boolean useClusterView)
  {
    final Iterable<DataSegment> dataSegments = metadataManager.segments().iterateAllUsedSegments();
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

    final Iterable<DataSegment> dataSegments = metadataManager.segments().iterateAllUsedSegments();
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
        metadataManager.segments().getImmutableDataSourcesWithAllUsedSegments();

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

  public String getCurrentLeader()
  {
    return coordLeaderSelector.getCurrentLeader();
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

      stopAllDutyGroupExecutors();
      balancerStrategyFactory.stopExecutor();
    }
  }

  public void runCompactSegmentsDuty()
  {
    final int startingLeaderCounter = coordLeaderSelector.localTerm();
    DutiesRunnable compactSegmentsDuty = new DutiesRunnable(
        ImmutableList.of(compactSegments),
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

      metadataManager.onLeaderStart();
      taskMaster.onLeaderStart();
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
        // Several coordinator duties can take a non trival amount of time to complete.
        // Hence, we schedule each duty group on a dedicated executor
        ScheduledExecutors.scheduleAtFixedRate(
            getOrCreateDutyGroupExecutor(dutiesRunnable.dutyGroupName),
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

      taskMaster.onLeaderStop();
      serviceAnnouncer.unannounce(self);
      lookupCoordinatorManager.stop();
      metadataManager.onLeaderStop();
      balancerStrategyFactory.stopExecutor();
    }
  }

  @GuardedBy("lock")
  private ScheduledExecutorService getOrCreateDutyGroupExecutor(String dutyGroup)
  {
    return dutyGroupExecutors.computeIfAbsent(
        dutyGroup,
        group -> executorFactory.create(1, "Coordinator-Exec-" + dutyGroup + "-%d")
    );
  }

  @GuardedBy("lock")
  private void stopAllDutyGroupExecutors()
  {
    dutyGroupExecutors.values().forEach(ScheduledExecutorService::shutdownNow);
    dutyGroupExecutors.clear();
  }

  private List<CoordinatorDuty> makeHistoricalManagementDuties()
  {
    return ImmutableList.of(
        new PrepareBalancerAndLoadQueues(
            taskMaster,
            loadQueueManager,
            balancerStrategyFactory,
            serverInventoryView
        ),
        new RunRules(segments -> metadataManager.segments().markSegmentsAsUnused(segments)),
        new UpdateReplicationStatus(),
        new UnloadUnusedSegments(loadQueueManager),
        new MarkOvershadowedSegmentsAsUnused(segments -> metadataManager.segments().markSegmentsAsUnused(segments)),
        new MarkEternityTombstonesAsUnused(segments -> metadataManager.segments().markSegmentsAsUnused(segments)),
        new BalanceSegments(config.getCoordinatorPeriod()),
        new CollectSegmentAndServerStats(taskMaster)
    );
  }

  @VisibleForTesting
  List<CoordinatorDuty> makeIndexingServiceDuties()
  {
    final List<CoordinatorDuty> duties = new ArrayList<>();
    if (config.isKillUnusedSegmentsEnabled()) {
      duties.add(new KillUnusedSegments(metadataManager.segments(), overlordClient, config));
    }
    if (config.isKillPendingSegmentsEnabled()) {
      duties.add(new KillStalePendingSegments(overlordClient));
    }

    // CompactSegmentsDuty should be the last duty as it can take a long time to complete
    // We do not have to add compactSegments if it is already enabled in the custom duty group
    if (getCompactSegmentsDutyFromCustomGroups().isEmpty()) {
      duties.add(compactSegments);
    }
    log.debug(
        "Initialized indexing service duties [%s].",
        duties.stream().map(duty -> duty.getClass().getName()).collect(Collectors.toList())
    );
    return ImmutableList.copyOf(duties);
  }

  private List<CoordinatorDuty> makeMetadataStoreManagementDuties()
  {
    return Arrays.asList(
        new KillSupervisors(config, metadataManager.supervisors()),
        new KillAuditLog(config, metadataManager.audit()),
        new KillRules(config, metadataManager.rules()),
        new KillDatasourceMetadata(config, metadataManager.indexer(), metadataManager.supervisors()),
        new KillCompactionConfig(config, metadataManager.segments(), metadataManager.configs())
    );
  }

  @VisibleForTesting
  CompactSegments initializeCompactSegmentsDuty(CompactionSegmentSearchPolicy compactionSegmentSearchPolicy)
  {
    List<CompactSegments> compactSegmentsDutyFromCustomGroups = getCompactSegmentsDutyFromCustomGroups();
    if (compactSegmentsDutyFromCustomGroups.isEmpty()) {
      return new CompactSegments(compactionSegmentSearchPolicy, overlordClient);
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
            metadataManager.isStarted(),
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
        DataSourcesSnapshot dataSourcesSnapshot
            = metadataManager.segments().getSnapshotOfDataSourcesWithAllUsedSegments();

        final CoordinatorDynamicConfig dynamicConfig = metadataManager.configs().getCurrentDynamicConfig();
        final CoordinatorCompactionConfig compactionConfig = metadataManager.configs().getCurrentCompactionConfig();
        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams
                .newBuilder(coordinatorStartTime)
                .withDatabaseRuleManager(metadataManager.rules())
                .withDataSourcesSnapshot(dataSourcesSnapshot)
                .withDynamicConfigs(dynamicConfig)
                .withCompactionConfig(compactionConfig)
                .build();
        log.info(
            "Initialized run params for group [%s] with [%,d] used segments in [%d] datasources.",
            dutyGroupName, params.getUsedSegments().size(), dataSourcesSnapshot.getDataSourcesMap().size()
        );

        boolean coordinationPaused = dynamicConfig.getPauseCoordination();
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

            dutyRunTime.restart();
            params = duty.run(params);
            dutyRunTime.stop();

            final String dutyName = duty.getClass().getName();
            if (params == null) {
              log.info("Stopping run for group [%s] on request of duty [%s].", dutyGroupName, dutyName);
              return;
            } else {
              final RowKey rowKey = RowKey.of(Dimension.DUTY, dutyName);
              final long dutyRunMillis = dutyRunTime.millisElapsed();
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
              "Emitted [%d] stats for group [%s]. All collected stats:%s",
              emittedCount.get(), dutyGroupName, allStats.buildStatsTable()
          );
        }

        // Emit the runtime of the full DutiesRunnable
        groupRunTime.stop();
        final long runMillis = groupRunTime.millisElapsed();
        emitStat(Stats.CoordinatorRun.GROUP_RUN_TIME, Collections.emptyMap(), runMillis);
        log.info("Finished coordinator run for group [%s] in [%d] ms.%n", dutyGroupName, runMillis);
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
      emitter.emit(eventBuilder.setMetric(stat.getMetricName(), value));
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

      // Collect stats for unavailable and under-replicated segments
      final CoordinatorRunStats stats = params.getCoordinatorStats();
      getDatasourceToUnavailableSegmentCount().forEach(
          (dataSource, numUnavailable) -> stats.add(
              Stats.Segments.UNAVAILABLE,
              RowKey.of(Dimension.DATASOURCE, dataSource),
              numUnavailable
          )
      );
      getTierToDatasourceToUnderReplicatedCount(false).forEach(
          (tier, countsPerDatasource) -> countsPerDatasource.forEach(
              (dataSource, underReplicatedCount) ->
                  stats.addToSegmentStat(Stats.Segments.UNDER_REPLICATED, tier, dataSource, underReplicatedCount)
          )
      );

      return params;
    }

  }
}

