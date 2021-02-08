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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Provider;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.curator.ZkEnablementConfig;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.CoordinatorIndexingServiceDuty;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
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
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.EmitClusterStatsAndMetrics;
import org.apache.druid.server.coordinator.duty.LogUsedSegments;
import org.apache.druid.server.coordinator.duty.MarkAsUnusedOvershadowedSegments;
import org.apache.druid.server.coordinator.duty.RunRules;
import org.apache.druid.server.coordinator.duty.UnloadUnusedSegments;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class DruidCoordinator
{
  /**
   * This comparator orders "freshest" segments first, i. e. segments with most recent intervals.
   *
   * It is used in historical nodes' {@link LoadQueuePeon}s to make historicals load more recent segment first.
   *
   * It is also used in {@link DruidCoordinatorRuntimeParams} for {@link
   * DruidCoordinatorRuntimeParams#getUsedSegments()} - a collection of segments to be considered during some
   * coordinator run for different {@link CoordinatorDuty}s. The order matters only for {@link
   * RunRules}, which tries to apply the rules while iterating the segments in the order imposed by
   * this comparator. In {@link LoadRule} the throttling limit may be hit (via {@link ReplicationThrottler}; see
   * {@link CoordinatorDynamicConfig#getReplicationThrottleLimit()}). So before we potentially hit this limit, we want
   * to schedule loading the more recent segments (among all of those that need to be loaded).
   *
   * In both {@link LoadQueuePeon}s and {@link RunRules}, we want to load more recent segments first
   * because presumably they are queried more often and contain are more important data for users, so if the Druid
   * cluster has availability problems and struggling to make all segments available immediately, at least we try to
   * make more "important" (more recent) segments available as soon as possible.
   */
  static final Comparator<DataSegment> SEGMENT_COMPARATOR_RECENT_FIRST = Ordering
      .from(Comparators.intervalsByEndThenStart())
      .onResultOf(DataSegment::getInterval)
      .compound(Ordering.<DataSegment>natural())
      .reverse();

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinator.class);

  private final Object lock = new Object();
  private final DruidCoordinatorConfig config;
  private final ZkPathsConfig zkPaths;
  private final JacksonConfigManager configManager;
  private final SegmentsMetadataManager segmentsMetadataManager;
  private final ServerInventoryView serverInventoryView;
  private final MetadataRuleManager metadataRuleManager;

  @Nullable // Null if zk is disabled
  private final CuratorFramework curator;

  private final ServiceEmitter emitter;
  private final IndexingServiceClient indexingServiceClient;
  private final ScheduledExecutorService exec;
  private final LoadQueueTaskMaster taskMaster;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DruidNode self;
  private final Set<CoordinatorDuty> indexingServiceDuties;
  private final BalancerStrategyFactory factory;
  private final LookupCoordinatorManager lookupCoordinatorManager;
  private final DruidLeaderSelector coordLeaderSelector;

  private final CompactSegments compactSegments;

  private volatile boolean started = false;
  private volatile SegmentReplicantLookup segmentReplicantLookup = null;

  private int cachedBalancerThreadNumber;
  private ListeningExecutorService balancerExec;

  private static final String HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP = "HistoricalManagementDuties";
  private static final String INDEXING_SERVICE_DUTIES_DUTY_GROUP = "IndexingServiceDuties";
  private static final String COMPACT_SEGMENTS_DUTIES_DUTY_GROUP = "CompactSegmentsDuties";

  @Inject
  public DruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      SegmentsMetadataManager segmentsMetadataManager,
      ServerInventoryView serverInventoryView,
      MetadataRuleManager metadataRuleManager,
      Provider<CuratorFramework> curatorProvider,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      ServiceAnnouncer serviceAnnouncer,
      @Self DruidNode self,
      @CoordinatorIndexingServiceDuty Set<CoordinatorDuty> indexingServiceDuties,
      BalancerStrategyFactory factory,
      LookupCoordinatorManager lookupCoordinatorManager,
      @Coordinator DruidLeaderSelector coordLeaderSelector,
      CompactSegments compactSegments,
      ZkEnablementConfig zkEnablementConfig
  )
  {
    this(
        config,
        zkPaths,
        configManager,
        segmentsMetadataManager,
        serverInventoryView,
        metadataRuleManager,
        curatorProvider,
        emitter,
        scheduledExecutorFactory,
        indexingServiceClient,
        taskMaster,
        serviceAnnouncer,
        self,
        new ConcurrentHashMap<>(),
        indexingServiceDuties,
        factory,
        lookupCoordinatorManager,
        coordLeaderSelector,
        compactSegments,
        zkEnablementConfig
    );
  }

  DruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      SegmentsMetadataManager segmentsMetadataManager,
      ServerInventoryView serverInventoryView,
      MetadataRuleManager metadataRuleManager,
      Provider<CuratorFramework> curatorProvider,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      ServiceAnnouncer serviceAnnouncer,
      DruidNode self,
      ConcurrentMap<String, LoadQueuePeon> loadQueuePeonMap,
      Set<CoordinatorDuty> indexingServiceDuties,
      BalancerStrategyFactory factory,
      LookupCoordinatorManager lookupCoordinatorManager,
      DruidLeaderSelector coordLeaderSelector,
      CompactSegments compactSegments,
      ZkEnablementConfig zkEnablementConfig
  )
  {
    this.config = config;
    this.zkPaths = zkPaths;
    this.configManager = configManager;

    this.segmentsMetadataManager = segmentsMetadataManager;
    this.serverInventoryView = serverInventoryView;
    this.metadataRuleManager = metadataRuleManager;
    if (zkEnablementConfig.isEnabled()) {
      this.curator = curatorProvider.get();
    } else {
      this.curator = null;
    }
    this.emitter = emitter;
    this.indexingServiceClient = indexingServiceClient;
    this.taskMaster = taskMaster;
    this.serviceAnnouncer = serviceAnnouncer;
    this.self = self;
    this.indexingServiceDuties = indexingServiceDuties;

    this.exec = scheduledExecutorFactory.create(1, "Coordinator-Exec--%d");

    this.loadManagementPeons = loadQueuePeonMap;
    this.factory = factory;
    this.lookupCoordinatorManager = lookupCoordinatorManager;
    this.coordLeaderSelector = coordLeaderSelector;

    this.compactSegments = compactSegments;
  }

  public boolean isLeader()
  {
    return coordLeaderSelector.isLeader();
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  /**
   * @return tier -> { dataSource -> underReplicationCount } map
   */
  public Map<String, Object2LongMap<String>> computeUnderReplicationCountsPerDataSourcePerTier()
  {
    final Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();
    return computeUnderReplicationCountsPerDataSourcePerTierForSegments(dataSegments);
  }

  /**
   * segmentReplicantLookup use in this method could potentially be stale since it is only updated on coordinator runs.
   * However, this is ok as long as the {@param dataSegments} is refreshed/latest as this would at least still ensure
   * that the stale data in segmentReplicantLookup would be under counting replication levels,
   * rather than potentially falsely reporting that everything is available.
   *
   * @return tier -> { dataSource -> underReplicationCount } map
   */
  public Map<String, Object2LongMap<String>> computeUnderReplicationCountsPerDataSourcePerTierForSegments(
      Iterable<DataSegment> dataSegments
  )
  {
    final Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier = new HashMap<>();

    if (segmentReplicantLookup == null) {
      return underReplicationCountsPerDataSourcePerTier;
    }

    final DateTime now = DateTimes.nowUtc();

    for (final DataSegment segment : dataSegments) {
      final List<Rule> rules = metadataRuleManager.getRulesWithDefault(segment.getDataSource());

      for (final Rule rule : rules) {
        if (!rule.appliesTo(segment, now)) {
          // Rule did not match. Continue to the next Rule.
          continue;
        }
        if (!rule.canLoadSegments()) {
          // Rule matched but rule does not and cannot load segments.
          // Hence, there is no need to update underReplicationCountsPerDataSourcePerTier map
          break;
        }

        rule.updateUnderReplicated(underReplicationCountsPerDataSourcePerTier, segmentReplicantLookup, segment);

        // Only the first matching rule applies. This is because the Coordinator cycle through all used segments
        // and match each segment with the first rule that applies. Each segment may only match a single rule.
        break;
      }
    }

    return underReplicationCountsPerDataSourcePerTier;
  }

  public Object2IntMap<String> computeNumsUnavailableUsedSegmentsPerDataSource()
  {
    if (segmentReplicantLookup == null) {
      return Object2IntMaps.emptyMap();
    }

    final Object2IntOpenHashMap<String> numsUnavailableUsedSegmentsPerDataSource = new Object2IntOpenHashMap<>();

    final Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();

    for (DataSegment segment : dataSegments) {
      if (segmentReplicantLookup.getLoadedReplicants(segment.getId()) == 0) {
        numsUnavailableUsedSegmentsPerDataSource.addTo(segment.getDataSource(), 1);
      } else {
        numsUnavailableUsedSegmentsPerDataSource.addTo(segment.getDataSource(), 0);
      }
    }

    return numsUnavailableUsedSegmentsPerDataSource;
  }

  public Map<String, Double> getLoadStatus()
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
          // Please see https://github.com/apache/druid/pull/5632 and LoadStatusBenchmark for more info.
          for (DataSegment serverSegment : loadedView.getSegments()) {
            segments.remove(serverSegment);
          }
        }
      }
      final int numUnavailableSegments = segments.size();
      loadStatus.put(
          dataSource.getName(),
          100 * ((double) (numPublishedSegments - numUnavailableSegments) / (double) numPublishedSegments)
      );
    }

    return loadStatus;
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

  public CoordinatorDynamicConfig getDynamicConfigs()
  {
    return CoordinatorDynamicConfig.current(configManager);
  }

  public CoordinatorCompactionConfig getCompactionConfig()
  {
    return CoordinatorCompactionConfig.current(configManager);
  }

  public void markSegmentAsUnused(DataSegment segment)
  {
    log.debug("Marking segment[%s] as unused", segment.getId());
    segmentsMetadataManager.markSegmentAsUnused(segment.getId().toString());
  }

  public String getCurrentLeader()
  {
    return coordLeaderSelector.getCurrentLeader();
  }

  public void moveSegment(
      DruidCoordinatorRuntimeParams params,
      ImmutableDruidServer fromServer,
      ImmutableDruidServer toServer,
      DataSegment segment,
      final LoadPeonCallback callback
  )
  {
    if (segment == null) {
      log.makeAlert(new IAE("Can not move null DataSegment"), "Exception moving null segment").emit();
      if (callback != null) {
        callback.execute();
      }
      throw new ISE("Cannot move null DataSegment");
    }
    SegmentId segmentId = segment.getId();
    try {
      if (fromServer.getMetadata().equals(toServer.getMetadata())) {
        throw new IAE("Cannot move [%s] to and from the same server [%s]", segmentId, fromServer.getName());
      }

      ImmutableDruidDataSource dataSource = params.getDataSourcesSnapshot().getDataSource(segment.getDataSource());
      if (dataSource == null) {
        throw new IAE("Unable to find dataSource for segment [%s] in metadata", segmentId);
      }

      // get segment information from SegmentsMetadataManager instead of getting it from fromServer's.
      // This is useful when SegmentsMetadataManager and fromServer DataSegment's are different for same
      // identifier (say loadSpec differs because of deep storage migration).
      final DataSegment segmentToLoad = dataSource.getSegment(segment.getId());
      if (segmentToLoad == null) {
        throw new IAE("No segment metadata found for segment Id [%s]", segment.getId());
      }
      final LoadQueuePeon loadPeon = loadManagementPeons.get(toServer.getName());
      if (loadPeon == null) {
        throw new IAE("LoadQueuePeon hasn't been created yet for path [%s]", toServer.getName());
      }

      final LoadQueuePeon dropPeon = loadManagementPeons.get(fromServer.getName());
      if (dropPeon == null) {
        throw new IAE("LoadQueuePeon hasn't been created yet for path [%s]", fromServer.getName());
      }

      final ServerHolder toHolder = new ServerHolder(toServer, loadPeon);
      if (toHolder.getAvailableSize() < segmentToLoad.getSize()) {
        throw new IAE(
            "Not enough capacity on server [%s] for segment [%s]. Required: %,d, available: %,d.",
            toServer.getName(),
            segmentToLoad,
            segmentToLoad.getSize(),
            toHolder.getAvailableSize()
        );
      }

      final String toLoadQueueSegPath =
          ZKPaths.makePath(zkPaths.getLoadQueuePath(), toServer.getName(), segmentId.toString());

      final LoadPeonCallback loadPeonCallback = () -> {
        dropPeon.unmarkSegmentToDrop(segmentToLoad);
        if (callback != null) {
          callback.execute();
        }
      };

      // mark segment to drop before it is actually loaded on server
      // to be able to account this information in DruidBalancerStrategy immediately
      dropPeon.markSegmentToDrop(segmentToLoad);
      try {
        loadPeon.loadSegment(
            segmentToLoad,
            () -> {
              try {
                if (serverInventoryView.isSegmentLoadedByServer(toServer.getName(), segment) &&
                    (curator == null || curator.checkExists().forPath(toLoadQueueSegPath) == null) &&
                    !dropPeon.getSegmentsToDrop().contains(segment)) {
                  dropPeon.dropSegment(segment, loadPeonCallback);
                } else {
                  loadPeonCallback.execute();
                }
              }
              catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
        );
      }
      catch (Exception e) {
        dropPeon.unmarkSegmentToDrop(segmentToLoad);
        throw new RuntimeException(e);
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception moving segment %s", segmentId).emit();
      if (callback != null) {
        callback.execute();
      }
    }
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
    DutiesRunnable compactSegmentsDuty = new DutiesRunnable(makeCompactSegmentsDuty(), startingLeaderCounter, COMPACT_SEGMENTS_DUTIES_DUTY_GROUP);
    compactSegmentsDuty.run();
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

      final List<Pair<? extends DutiesRunnable, Duration>> dutiesRunnables = new ArrayList<>();
      dutiesRunnables.add(
          Pair.of(
              new DutiesRunnable(makeHistoricalManagementDuties(), startingLeaderCounter, HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP),
              config.getCoordinatorPeriod()
          )
      );
      if (indexingServiceClient != null) {
        dutiesRunnables.add(
            Pair.of(
                new DutiesRunnable(makeIndexingServiceDuties(), startingLeaderCounter, INDEXING_SERVICE_DUTIES_DUTY_GROUP),
                config.getCoordinatorIndexingPeriod()
            )
        );
      }

      for (final Pair<? extends DutiesRunnable, Duration> dutiesRunnable : dutiesRunnables) {
        // CompactSegmentsDuty can takes a non trival amount of time to complete.
        // Hence, we schedule at fixed rate to make sure the other tasks still run at approximately every
        // config.getCoordinatorIndexingPeriod() period. Note that cautious should be taken
        // if setting config.getCoordinatorIndexingPeriod() lower than the default value.
        ScheduledExecutors.scheduleAtFixedRate(
            exec,
            config.getCoordinatorStartDelay(),
            dutiesRunnable.rhs,
            new Callable<ScheduledExecutors.Signal>()
            {
              private final DutiesRunnable theRunnable = dutiesRunnable.lhs;

              @Override
              public ScheduledExecutors.Signal call()
              {
                if (coordLeaderSelector.isLeader() && startingLeaderCounter == coordLeaderSelector.localTerm()) {
                  theRunnable.run();
                }
                if (coordLeaderSelector.isLeader()
                    && startingLeaderCounter == coordLeaderSelector.localTerm()) { // (We might no longer be leader)
                  return ScheduledExecutors.Signal.REPEAT;
                } else {
                  return ScheduledExecutors.Signal.STOP;
                }
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

  private List<CoordinatorDuty> makeHistoricalManagementDuties()
  {
    return ImmutableList.of(
        new LogUsedSegments(),
        new UpdateCoordinatorStateAndPrepareCluster(),
        new RunRules(DruidCoordinator.this),
        new UnloadUnusedSegments(),
        new MarkAsUnusedOvershadowedSegments(DruidCoordinator.this),
        new BalanceSegments(DruidCoordinator.this),
        new EmitClusterStatsAndMetrics(DruidCoordinator.this)
    );
  }

  private List<CoordinatorDuty> makeIndexingServiceDuties()
  {
    List<CoordinatorDuty> duties = new ArrayList<>();
    duties.add(new LogUsedSegments());
    duties.addAll(indexingServiceDuties);
    // CompactSegmentsDuty should be the last duty as it can take a long time to complete
    duties.addAll(makeCompactSegmentsDuty());

    log.debug(
        "Done making indexing service duties %s",
        duties.stream().map(duty -> duty.getClass().getName()).collect(Collectors.toList())
    );
    return ImmutableList.copyOf(duties);
  }

  private List<CoordinatorDuty> makeCompactSegmentsDuty()
  {
    return ImmutableList.of(compactSegments);
  }

  @VisibleForTesting
  protected class DutiesRunnable implements Runnable
  {
    private final long startTimeNanos = System.nanoTime();
    private final List<CoordinatorDuty> duties;
    private final int startingLeaderCounter;
    private final String dutiesRunnableAlias;

    protected DutiesRunnable(List<CoordinatorDuty> duties, final int startingLeaderCounter, String alias)
    {
      this.duties = duties;
      this.startingLeaderCounter = startingLeaderCounter;
      this.dutiesRunnableAlias = alias;
    }

    @VisibleForTesting
    protected void initBalancerExecutor()
    {
      final int currentNumber = getDynamicConfigs().getBalancerComputeThreads();
      final String threadNameFormat = "coordinator-cost-balancer-%s";
      // fist time initialization
      if (balancerExec == null) {
        balancerExec = MoreExecutors.listeningDecorator(Execs.multiThreaded(
            currentNumber,
            threadNameFormat
        ));
        cachedBalancerThreadNumber = currentNumber;
        return;
      }

      if (cachedBalancerThreadNumber != currentNumber) {
        log.info(
            "balancerComputeThreads has been changed from [%s] to [%s], recreating the thread pool.",
            cachedBalancerThreadNumber,
            currentNumber
        );
        balancerExec.shutdownNow();
        balancerExec = MoreExecutors.listeningDecorator(Execs.multiThreaded(
            currentNumber,
            threadNameFormat
        ));
        cachedBalancerThreadNumber = currentNumber;
      }
    }

    @Override
    public void run()
    {
      try {
        final long globalStart = System.nanoTime();
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

        initBalancerExecutor();
        BalancerStrategy balancerStrategy = factory.createBalancerStrategy(balancerExec);

        // Do coordinator stuff.
        DataSourcesSnapshot dataSourcesSnapshot = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments();

        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams
                .newBuilder()
                .withDatabaseRuleManager(metadataRuleManager)
                .withStartTimeNanos(startTimeNanos)
                .withSnapshotOfDataSourcesWithAllUsedSegments(dataSourcesSnapshot)
                .withDynamicConfigs(getDynamicConfigs())
                .withCompactionConfig(getCompactionConfig())
                .withEmitter(emitter)
                .withBalancerStrategy(balancerStrategy)
                .build();

        boolean coordinationPaused = getDynamicConfigs().getPauseCoordination();
        if (coordinationPaused
            && coordLeaderSelector.isLeader()
            && startingLeaderCounter == coordLeaderSelector.localTerm()) {

          log.debug(
              "Coordination is paused via dynamic configs! I will not be running Coordination Duties at this time"
          );
        }

        for (CoordinatorDuty duty : duties) {
          // Don't read state and run state in the same duty otherwise racy conditions may exist
          if (!coordinationPaused
              && coordLeaderSelector.isLeader()
              && startingLeaderCounter == coordLeaderSelector.localTerm()) {

            final long start = System.nanoTime();
            params = duty.run(params);
            final long end = System.nanoTime();

            if (params == null) {
              // This duty wanted to cancel the run. No log message, since the duty should have logged a reason.
              return;
            } else {
              params.getCoordinatorStats().addToDutyStat("runtime", duty.getClass().getName(), TimeUnit.NANOSECONDS.toMillis(end - start));
            }
          }
        }
        // Emit the runtime of the full DutiesRunnable
        params.getEmitter().emit(
            new ServiceMetricEvent.Builder()
                .setDimension(DruidMetrics.DUTY_GROUP, dutiesRunnableAlias)
                .build("coordinator/global/time", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - globalStart))
        );
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
      }
    }
  }

  /**
   * Updates the enclosing {@link DruidCoordinator}'s state and prepares an immutable view of the cluster state (which
   * consists of {@link DruidCluster} and {@link SegmentReplicantLookup}) and feeds it into {@link
   * DruidCoordinatorRuntimeParams} for use in subsequent {@link CoordinatorDuty}s (see the order in {@link
   * #makeHistoricalManagementDuties()}).
   */
  private class UpdateCoordinatorStateAndPrepareCluster implements CoordinatorDuty
  {
    @Nullable
    @Override
    public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
    {
      List<ImmutableDruidServer> currentServers = prepareCurrentServers();

      startPeonsForNewServers(currentServers);

      final DruidCluster cluster = prepareCluster(params, currentServers);
      segmentReplicantLookup = SegmentReplicantLookup.make(cluster);

      stopPeonsForDisappearedServers(currentServers);

      return params.buildFromExisting()
                   .withDruidCluster(cluster)
                   .withLoadManagementPeons(loadManagementPeons)
                   .withSegmentReplicantLookup(segmentReplicantLookup)
                   .build();
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

    DruidCluster prepareCluster(DruidCoordinatorRuntimeParams params, List<ImmutableDruidServer> currentServers)
    {
      Set<String> decommissioningServers = params.getCoordinatorDynamicConfig().getDecommissioningNodes();
      final DruidCluster cluster = new DruidCluster();
      for (ImmutableDruidServer server : currentServers) {
        cluster.add(
            new ServerHolder(
                server,
                loadManagementPeons.get(server.getName()),
                decommissioningServers.contains(server.getHost())
            )
        );
      }
      return cluster;
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
}

