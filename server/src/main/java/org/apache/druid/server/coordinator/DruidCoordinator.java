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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.compaction.CompactionRunSimulator;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.config.CoordinatorKillConfigs;
import org.apache.druid.server.coordinator.config.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.config.KillUnusedSegmentsConfig;
import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.CoordinatorDutyGroup;
import org.apache.druid.server.coordinator.duty.DutyGroupStatus;
import org.apache.druid.server.coordinator.duty.KillAuditLog;
import org.apache.druid.server.coordinator.duty.KillCompactionConfig;
import org.apache.druid.server.coordinator.duty.KillDatasourceMetadata;
import org.apache.druid.server.coordinator.duty.KillRules;
import org.apache.druid.server.coordinator.duty.KillStalePendingSegments;
import org.apache.druid.server.coordinator.duty.KillSupervisors;
import org.apache.druid.server.coordinator.duty.KillUnreferencedSegmentSchema;
import org.apache.druid.server.coordinator.duty.KillUnusedSegments;
import org.apache.druid.server.coordinator.duty.MarkEternityTombstonesAsUnused;
import org.apache.druid.server.coordinator.duty.MarkOvershadowedSegmentsAsUnused;
import org.apache.druid.server.coordinator.duty.MetadataAction;
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
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class DruidCoordinator
{
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinator.class);

  private final Object lock = new Object();
  private final DruidCoordinatorConfig config;
  private final MetadataManager metadataManager;
  private final ServerInventoryView serverInventoryView;

  private final ServiceEmitter emitter;
  private final OverlordClient overlordClient;
  private final ScheduledExecutorFactory executorFactory;
  private final List<DutiesRunnable> dutiesRunnables = new ArrayList<>();
  private final LoadQueueTaskMaster taskMaster;
  private final SegmentLoadQueueManager loadQueueManager;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DruidNode self;
  private final CoordinatorCustomDutyGroups customDutyGroups;
  private final BalancerStrategyFactory balancerStrategyFactory;
  private final LookupCoordinatorManager lookupCoordinatorManager;
  private final DruidLeaderSelector coordLeaderSelector;
  private final CompactionStatusTracker compactionStatusTracker;
  private final CompactSegments compactSegments;
  @Nullable
  private final CoordinatorSegmentMetadataCache coordinatorSegmentMetadataCache;
  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

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

  /**
   * Set of broadcast segments determined in the latest coordinator run of the {@link RunRules} duty.
   * This might contain stale information if the Coordinator duties haven't run or are delayed.
   */
  private volatile Set<DataSegment> broadcastSegments = null;

  private static final String HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP = "HistoricalManagementDuties";
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
      LookupCoordinatorManager lookupCoordinatorManager,
      @Coordinator DruidLeaderSelector coordLeaderSelector,
      @Nullable CoordinatorSegmentMetadataCache coordinatorSegmentMetadataCache,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig,
      CompactionStatusTracker compactionStatusTracker
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

    this.balancerStrategyFactory = config.getBalancerStrategyFactory();
    this.lookupCoordinatorManager = lookupCoordinatorManager;
    this.coordLeaderSelector = coordLeaderSelector;
    this.compactionStatusTracker = compactionStatusTracker;
    this.compactSegments = initializeCompactSegmentsDuty(this.compactionStatusTracker);
    this.loadQueueManager = loadQueueManager;
    this.coordinatorSegmentMetadataCache = coordinatorSegmentMetadataCache;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
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
      if (replicaCount != null && (replicaCount.totalLoaded() > 0 || replicaCount.required() == 0)) {
        datasourceToUnavailableSegments.addTo(segment.getDataSource(), 0);
      } else {
        datasourceToUnavailableSegments.addTo(segment.getDataSource(), 1);
      }
    }

    return datasourceToUnavailableSegments;
  }

  public Object2IntMap<String> getDatasourceToDeepStorageQueryOnlySegmentCount()
  {
    if (segmentReplicationStatus == null) {
      return Object2IntMaps.emptyMap();
    }

    final Object2IntOpenHashMap<String> datasourceToDeepStorageOnlySegments = new Object2IntOpenHashMap<>();

    final Iterable<DataSegment> dataSegments = metadataManager.segments().iterateAllUsedSegments();
    for (DataSegment segment : dataSegments) {
      SegmentReplicaCount replicaCount = segmentReplicationStatus.getReplicaCountsInCluster(segment.getId());
      if (replicaCount != null && replicaCount.totalLoaded() == 0 && replicaCount.required() == 0) {
        datasourceToDeepStorageOnlySegments.addTo(segment.getDataSource(), 1);
      }
    }

    return datasourceToDeepStorageOnlySegments;
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

  /**
   * @return Set of broadcast segments determined by the latest run of the {@link RunRules} duty.
   * If the coordinator runs haven't triggered or are delayed, this information may be stale.
   */
  @Nullable
  public Set<DataSegment> getBroadcastSegments()
  {
    return broadcastSegments;
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
  public AutoCompactionSnapshot getAutoCompactionSnapshotForDataSource(String dataSource)
  {
    return compactSegments.getAutoCompactionSnapshot(dataSource);
  }

  public Map<String, AutoCompactionSnapshot> getAutoCompactionSnapshot()
  {
    return compactSegments.getAutoCompactionSnapshot();
  }

  public CompactionSimulateResult simulateRunWithConfigUpdate(ClusterCompactionConfig updateRequest)
  {
    return new CompactionRunSimulator(compactionStatusTracker, overlordClient).simulateRunWithConfig(
        metadataManager.configs().getCurrentCompactionConfig().withClusterConfig(updateRequest),
        metadataManager.segments()
                       .getSnapshotOfDataSourcesWithAllUsedSegments(),
        CompactionEngine.NATIVE
    );
  }

  public String getCurrentLeader()
  {
    return coordLeaderSelector.getCurrentLeader();
  }

  public List<DutyGroupStatus> getStatusOfDuties()
  {
    return dutiesRunnables.stream().map(r -> r.dutyGroup.getStatus()).collect(Collectors.toList());
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
      stopAllDutyGroups();
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
      if (coordinatorSegmentMetadataCache != null) {
        coordinatorSegmentMetadataCache.onLeaderStart();
      }
      final int startingLeaderCounter = coordLeaderSelector.localTerm();

      dutiesRunnables.add(
          new DutiesRunnable(
              makeHistoricalManagementDuties(),
              startingLeaderCounter,
              HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP,
              config.getCoordinatorPeriod()
          )
      );
      dutiesRunnables.add(
          new DutiesRunnable(
              makeIndexingServiceDuties(),
              startingLeaderCounter,
              INDEXING_SERVICE_DUTIES_DUTY_GROUP,
              config.getCoordinatorIndexingPeriod()
          )
      );
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
                new ArrayList<>(customDutyGroup.getCustomDutyList()),
                startingLeaderCounter,
                customDutyGroup.getName(),
                customDutyGroup.getPeriod()
            )
        );
      }

      log.warn(
          "Created [%d] duty groups. DUTY RUNS WILL NOT BE LOGGED."
          + " Use API '/druid/coordinator/v1/duties' to get current status.",
          dutiesRunnables.size()
      );

      for (final DutiesRunnable dutiesRunnable : dutiesRunnables) {
        // Several coordinator duties can take a non trival amount of time to complete.
        // Hence, we schedule each duty group on a dedicated executor
        ScheduledExecutors.scheduleAtFixedRate(
            dutiesRunnable.executor,
            config.getCoordinatorStartDelay(),
            dutiesRunnable.dutyGroup.getPeriod(),
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

      if (coordinatorSegmentMetadataCache != null) {
        coordinatorSegmentMetadataCache.onLeaderStop();
      }
      compactionStatusTracker.stop();
      taskMaster.onLeaderStop();
      serviceAnnouncer.unannounce(self);
      lookupCoordinatorManager.stop();
      metadataManager.onLeaderStop();
      stopAllDutyGroups();
    }
  }

  @GuardedBy("lock")
  private void stopAllDutyGroups()
  {
    balancerStrategyFactory.stopExecutor();
    dutiesRunnables.forEach(group -> group.executor.shutdownNow());
    dutiesRunnables.clear();
  }

  private List<CoordinatorDuty> makeHistoricalManagementDuties()
  {
    final MetadataAction.DeleteSegments deleteSegments = this::markSegmentsAsUnused;
    final MetadataAction.GetDatasourceRules getRules
        = dataSource -> metadataManager.rules().getRulesWithDefault(dataSource);

    return ImmutableList.of(
        new PrepareBalancerAndLoadQueues(
            taskMaster,
            loadQueueManager,
            balancerStrategyFactory,
            serverInventoryView
        ),
        new RunRules(deleteSegments, getRules),
        new UpdateReplicationStatus(),
        new CollectSegmentStats(),
        new UnloadUnusedSegments(loadQueueManager, getRules),
        new MarkOvershadowedSegmentsAsUnused(deleteSegments),
        new MarkEternityTombstonesAsUnused(deleteSegments),
        new BalanceSegments(config.getCoordinatorPeriod()),
        new CollectLoadQueueStats()
    );
  }

  private List<CoordinatorDuty> makeIndexingServiceDuties()
  {
    final List<CoordinatorDuty> duties = new ArrayList<>();
    final KillUnusedSegmentsConfig killUnusedConfig = config.getKillConfigs().unusedSegments(
        config.getCoordinatorIndexingPeriod()
    );
    if (killUnusedConfig.isCleanupEnabled()) {
      duties.add(new KillUnusedSegments(metadataManager.segments(), overlordClient, killUnusedConfig));
    }
    if (config.getKillConfigs().pendingSegments().isCleanupEnabled()) {
      duties.add(new KillStalePendingSegments(overlordClient));
    }

    // Do not add compactSegments if it is already added in any of the custom duty groups
    if (getCompactSegmentsDutyFromCustomGroups().isEmpty()) {
      duties.add(compactSegments);
    }
    return ImmutableList.copyOf(duties);
  }

  private List<CoordinatorDuty> makeMetadataStoreManagementDuties()
  {
    final CoordinatorKillConfigs killConfigs = config.getKillConfigs();
    final List<CoordinatorDuty> duties = new ArrayList<>();
    duties.add(new KillSupervisors(killConfigs.supervisors(), metadataManager.supervisors()));
    duties.add(new KillAuditLog(killConfigs.auditLogs(), metadataManager.audit()));
    duties.add(new KillRules(killConfigs.rules(), metadataManager.rules()));
    duties.add(
        new KillDatasourceMetadata(
            killConfigs.datasources(),
            metadataManager.indexer(),
            metadataManager.supervisors()
        )
    );
    duties.add(
        new KillCompactionConfig(killConfigs.compactionConfigs(), metadataManager.segments(), metadataManager.configs())
    );
    if (centralizedDatasourceSchemaConfig.isEnabled()) {
      duties.add(new KillUnreferencedSegmentSchema(killConfigs.segmentSchemas(), metadataManager.schemas()));
    }
    return duties;
  }

  private CompactSegments initializeCompactSegmentsDuty(CompactionStatusTracker statusTracker)
  {
    List<CompactSegments> compactSegmentsDutyFromCustomGroups = getCompactSegmentsDutyFromCustomGroups();
    if (compactSegmentsDutyFromCustomGroups.isEmpty()) {
      return new CompactSegments(statusTracker, overlordClient);
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

  private List<CompactSegments> getCompactSegmentsDutyFromCustomGroups()
  {
    return customDutyGroups.getCoordinatorCustomDutyGroups()
                           .stream()
                           .flatMap(coordinatorCustomDutyGroup ->
                                        coordinatorCustomDutyGroup.getCustomDutyList().stream())
                           .filter(duty -> duty instanceof CompactSegments)
                           .map(duty -> (CompactSegments) duty)
                           .collect(Collectors.toList());
  }

  /**
   * Makes an API call to Overlord to mark segments of a datasource as unused.
   *
   * @return Number of segments updated.
   */
  private int markSegmentsAsUnused(String datasource, Set<SegmentId> segmentIds)
  {
    try {
      final Set<String> segmentIdsToUpdate
          = segmentIds.stream().map(SegmentId::toString).collect(Collectors.toSet());
      final SegmentsToUpdateFilter filter
          = new SegmentsToUpdateFilter(null, segmentIdsToUpdate, null);
      SegmentUpdateResponse response = FutureUtils.getUnchecked(
          overlordClient.markSegmentsAsUnused(datasource, filter),
          true
      );
      return response.getNumChangedSegments();
    }
    catch (Exception e) {
      final Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof HttpResponseException) {
        HttpResponseStatus status = ((HttpResponseException) rootCause).getResponse().getStatus();
        if (status.getCode() == 404) {
          log.info("Could not update segments via Overlord API. Updating metadata store directly.");
          return metadataManager.segments().markSegmentsAsUnused(segmentIds);
        }
      }

      log.error(e, "Could not mark segments as unused for datasource[%s].", datasource);
      return 0;
    }
  }

  /**
   * Used by {@link CoordinatorDutyGroup} to check leadership and emit stats.
   */
  public interface DutyGroupHelper
  {
    boolean isLeader();
    void emitStat(CoordinatorStat stat, RowKey rowKey, long value);
  }

  /**
   * Container for a single {@link CoordinatorDutyGroup} that runs on a dedicated executor.
   */
  private class DutiesRunnable implements Runnable, DutyGroupHelper
  {
    private final int startingLeaderCounter;
    private final ScheduledExecutorService executor;
    private final CoordinatorDutyGroup dutyGroup;

    DutiesRunnable(
        List<CoordinatorDuty> duties,
        final int startingLeaderCounter,
        String alias,
        Duration period
    )
    {
      this.startingLeaderCounter = startingLeaderCounter;
      this.dutyGroup = new CoordinatorDutyGroup(alias, duties, period, this);
      this.executor = executorFactory.create(1, "Coordinator-Exec-" + alias + "-%d");
    }

    @Override
    public void run()
    {
      try {
        synchronized (lock) {
          if (!coordLeaderSelector.isLeader()) {
            stopBeingLeader();
            return;
          }
        }

        if (metadataManager.isStarted() && serverInventoryView.isStarted()) {
          final DataSourcesSnapshot dataSourcesSnapshot;
          if (dutyGroup.getName().equals(COMPACT_SEGMENTS_DUTIES_DUTY_GROUP)) {
            // If this is a compact segments duty group triggered by IT,
            // use a future snapshotTime to ensure that compaction always runs
            dataSourcesSnapshot = DataSourcesSnapshot.fromUsedSegments(
                metadataManager.segments()
                               .getSnapshotOfDataSourcesWithAllUsedSegments()
                               .iterateAllUsedSegmentsInSnapshot(),
                DateTimes.nowUtc().plusMinutes(60)
            );
          } else {
            dataSourcesSnapshot = metadataManager.segments().getSnapshotOfDataSourcesWithAllUsedSegments();
          }

          final DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
              .builder()
              .withDataSourcesSnapshot(dataSourcesSnapshot)
              .withDynamicConfigs(metadataManager.configs().getCurrentDynamicConfig())
              .withCompactionConfig(metadataManager.configs().getCurrentCompactionConfig())
              .build();
          dutyGroup.run(params);
        } else {
          log.error("Inventory view not initialized yet. Skipping run of duty group[%s].", dutyGroup.getName());
          stopBeingLeader();
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
      }
    }

    @Override
    public void emitStat(CoordinatorStat stat, RowKey rowKey, long value)
    {
      ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder()
          .setDimension(Dimension.DUTY_GROUP.reportedName(), dutyGroup.getName());
      rowKey.getValues().forEach(
          (dim, dimValue) -> eventBuilder.setDimension(dim.reportedName(), dimValue)
      );
      emitter.emit(eventBuilder.setMetric(stat.getMetricName(), value));
    }

    @Override
    public boolean isLeader()
    {
      return coordLeaderSelector.isLeader()
             && startingLeaderCounter == coordLeaderSelector.localTerm();
    }
  }

  // Duties that read/update the state in the DruidCoordinator class

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
      broadcastSegments = params.getBroadcastSegments();
      segmentReplicationStatus = params.getSegmentReplicationStatus();
      if (coordinatorSegmentMetadataCache != null) {
        coordinatorSegmentMetadataCache.updateSegmentReplicationStatus(segmentReplicationStatus);
      }
      return params;
    }
  }

  /**
   * Collects stats for unavailable and under-replicated segments.
   */
  private class CollectSegmentStats implements CoordinatorDuty
  {
    @Override
    public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
    {
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
      getDatasourceToDeepStorageQueryOnlySegmentCount().forEach(
          (dataSource, numDeepStorageOnly) -> stats.add(
              Stats.Segments.DEEP_STORAGE_ONLY,
              RowKey.of(Dimension.DATASOURCE, dataSource),
              numDeepStorageOnly
          )
      );

      return params;
    }
  }

  /**
   * Collects load queue stats.
   */
  private class CollectLoadQueueStats implements CoordinatorDuty
  {
    @Override
    public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
    {
      final CoordinatorRunStats stats = params.getCoordinatorStats();
      taskMaster.getAllPeons().forEach((serverName, queuePeon) -> {
        final RowKey rowKey = RowKey.of(Dimension.SERVER, serverName);
        stats.add(Stats.SegmentQueue.BYTES_TO_LOAD, rowKey, queuePeon.getSizeOfSegmentsToLoad());
        stats.add(Stats.SegmentQueue.NUM_TO_LOAD, rowKey, queuePeon.getSegmentsToLoad().size());
        stats.add(Stats.SegmentQueue.NUM_TO_DROP, rowKey, queuePeon.getSegmentsToDrop().size());
        stats.updateMax(Stats.SegmentQueue.LOAD_RATE_KBPS, rowKey, queuePeon.getLoadRateKbps());

        queuePeon.getAndResetStats().forEachStat(
            (stat, key, statValue) ->
                stats.add(stat, createRowKeyForServer(serverName, key.getValues()), statValue)
        );
      });
      return params;
    }

    private RowKey createRowKeyForServer(String serverName, Map<Dimension, String> dimensionValues)
    {
      final RowKey.Builder builder = RowKey.with(Dimension.SERVER, serverName);
      dimensionValues.forEach(builder::with);
      return builder.build();
    }
  }
}
