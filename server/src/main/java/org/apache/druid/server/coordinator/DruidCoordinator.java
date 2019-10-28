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
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
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
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.CoordinatorIndexingServiceHelper;
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
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.MetadataSegmentManager;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorCleanupOvershadowed;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorCleanupUnneeded;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorHelper;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorLogger;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorRuleRunner;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorSegmentCompactor;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorSegmentInfoLoader;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Duration;

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
   * coordinator run for different {@link DruidCoordinatorHelper}s. The order matters only for {@link
   * DruidCoordinatorRuleRunner}, which tries to apply the rules while iterating the segments in the order imposed by
   * this comparator. In {@link LoadRule} the throttling limit may be hit (via {@link ReplicationThrottler}; see
   * {@link CoordinatorDynamicConfig#getReplicationThrottleLimit()}). So before we potentially hit this limit, we want
   * to schedule loading the more recent segments (among all of those that need to be loaded).
   *
   * In both {@link LoadQueuePeon}s and {@link DruidCoordinatorRuleRunner}, we want to load more recent segments first
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
  private final MetadataSegmentManager segmentsMetadata;
  private final ServerInventoryView serverInventoryView;
  private final MetadataRuleManager metadataRuleManager;
  private final CuratorFramework curator;
  private final ServiceEmitter emitter;
  private final IndexingServiceClient indexingServiceClient;
  private final ScheduledExecutorService exec;
  private final LoadQueueTaskMaster taskMaster;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DruidNode self;
  private final Set<DruidCoordinatorHelper> indexingServiceHelpers;
  private final BalancerStrategyFactory factory;
  private final LookupCoordinatorManager lookupCoordinatorManager;
  private final DruidLeaderSelector coordLeaderSelector;

  private final DruidCoordinatorSegmentCompactor segmentCompactor;

  private volatile boolean started = false;
  private volatile SegmentReplicantLookup segmentReplicantLookup = null;

  @Inject
  public DruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      MetadataSegmentManager segmentsMetadata,
      ServerInventoryView serverInventoryView,
      MetadataRuleManager metadataRuleManager,
      CuratorFramework curator,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      ServiceAnnouncer serviceAnnouncer,
      @Self DruidNode self,
      @CoordinatorIndexingServiceHelper Set<DruidCoordinatorHelper> indexingServiceHelpers,
      BalancerStrategyFactory factory,
      LookupCoordinatorManager lookupCoordinatorManager,
      @Coordinator DruidLeaderSelector coordLeaderSelector,
      DruidCoordinatorSegmentCompactor segmentCompactor
  )
  {
    this(
        config,
        zkPaths,
        configManager,
        segmentsMetadata,
        serverInventoryView,
        metadataRuleManager,
        curator,
        emitter,
        scheduledExecutorFactory,
        indexingServiceClient,
        taskMaster,
        serviceAnnouncer,
        self,
        new ConcurrentHashMap<>(),
        indexingServiceHelpers,
        factory,
        lookupCoordinatorManager,
        coordLeaderSelector,
        segmentCompactor
    );
  }

  DruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      MetadataSegmentManager segmentsMetadata,
      ServerInventoryView serverInventoryView,
      MetadataRuleManager metadataRuleManager,
      CuratorFramework curator,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      ServiceAnnouncer serviceAnnouncer,
      DruidNode self,
      ConcurrentMap<String, LoadQueuePeon> loadQueuePeonMap,
      Set<DruidCoordinatorHelper> indexingServiceHelpers,
      BalancerStrategyFactory factory,
      LookupCoordinatorManager lookupCoordinatorManager,
      DruidLeaderSelector coordLeaderSelector,
      DruidCoordinatorSegmentCompactor segmentCompactor
  )
  {
    this.config = config;
    this.zkPaths = zkPaths;
    this.configManager = configManager;

    this.segmentsMetadata = segmentsMetadata;
    this.serverInventoryView = serverInventoryView;
    this.metadataRuleManager = metadataRuleManager;
    this.curator = curator;
    this.emitter = emitter;
    this.indexingServiceClient = indexingServiceClient;
    this.taskMaster = taskMaster;
    this.serviceAnnouncer = serviceAnnouncer;
    this.self = self;
    this.indexingServiceHelpers = indexingServiceHelpers;

    this.exec = scheduledExecutorFactory.create(1, "Coordinator-Exec--%d");

    this.loadManagementPeons = loadQueuePeonMap;
    this.factory = factory;
    this.lookupCoordinatorManager = lookupCoordinatorManager;
    this.coordLeaderSelector = coordLeaderSelector;

    this.segmentCompactor = segmentCompactor;
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
    final Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier = new HashMap<>();

    if (segmentReplicantLookup == null) {
      return underReplicationCountsPerDataSourcePerTier;
    }

    final Iterable<DataSegment> dataSegments = segmentsMetadata.iterateAllUsedSegments();

    final DateTime now = DateTimes.nowUtc();

    for (final DataSegment segment : dataSegments) {
      final List<Rule> rules = metadataRuleManager.getRulesWithDefault(segment.getDataSource());

      for (final Rule rule : rules) {
        if (!(rule instanceof LoadRule && rule.appliesTo(segment, now))) {
          continue;
        }

        ((LoadRule) rule)
            .getTieredReplicants()
            .forEach((final String tier, final Integer ruleReplicants) -> {
              int currentReplicants = segmentReplicantLookup.getLoadedReplicants(segment.getId(), tier);
              Object2LongMap<String> underReplicationPerDataSource = underReplicationCountsPerDataSourcePerTier
                  .computeIfAbsent(tier, ignored -> new Object2LongOpenHashMap<>());
              ((Object2LongOpenHashMap<String>) underReplicationPerDataSource)
                  .addTo(segment.getDataSource(), Math.max(ruleReplicants - currentReplicants, 0));
            });
        break; // only the first matching rule applies
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

    final Iterable<DataSegment> dataSegments = segmentsMetadata.iterateAllUsedSegments();

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
        segmentsMetadata.getImmutableDataSourcesWithAllUsedSegments();

    for (ImmutableDruidDataSource dataSource : dataSources) {
      final Set<DataSegment> segments = Sets.newHashSet(dataSource.getSegments());
      final int numUsedSegments = segments.size();

      // remove loaded segments
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        final DruidDataSource loadedView = druidServer.getDataSource(dataSource.getName());
        if (loadedView != null) {
          // This does not use segments.removeAll(loadedView.getSegments()) for performance reasons.
          // Please see https://github.com/apache/incubator-druid/pull/5632 and LoadStatusBenchmark for more info.
          for (DataSegment serverSegment : loadedView.getSegments()) {
            segments.remove(serverSegment);
          }
        }
      }
      final int numUnloadedSegments = segments.size();
      loadStatus.put(
          dataSource.getName(),
          100 * ((double) (numUsedSegments - numUnloadedSegments) / (double) numUsedSegments)
      );
    }

    return loadStatus;
  }

  public long remainingSegmentSizeBytesForCompaction(String dataSource)
  {
    return segmentCompactor.getRemainingSegmentSizeBytes(dataSource);
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
    log.info("Marking segment[%s] as unused", segment.getId());
    segmentsMetadata.markSegmentAsUnused(segment.getId().toString());
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

      // get segment information from MetadataSegmentManager instead of getting it from fromServer's.
      // This is useful when MetadataSegmentManager and fromServer DataSegment's are different for same
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
                    curator.checkExists().forPath(toLoadQueueSegPath) == null &&
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
    }
  }

  private void becomeLeader()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      log.info("I am the leader of the coordinators, all must bow!");
      log.info("Starting coordination in [%s]", config.getCoordinatorStartDelay());

      segmentsMetadata.startPollingDatabasePeriodically();
      metadataRuleManager.start();
      lookupCoordinatorManager.start();
      serviceAnnouncer.announce(self);
      final int startingLeaderCounter = coordLeaderSelector.localTerm();

      final List<Pair<? extends CoordinatorRunnable, Duration>> coordinatorRunnables = new ArrayList<>();
      coordinatorRunnables.add(
          Pair.of(
              new CoordinatorHistoricalManagerRunnable(startingLeaderCounter),
              config.getCoordinatorPeriod()
          )
      );
      if (indexingServiceClient != null) {
        coordinatorRunnables.add(
            Pair.of(
                new CoordinatorIndexingServiceRunnable(
                    makeIndexingServiceHelpers(),
                    startingLeaderCounter
                ),
                config.getCoordinatorIndexingPeriod()
            )
        );
      }

      for (final Pair<? extends CoordinatorRunnable, Duration> coordinatorRunnable : coordinatorRunnables) {
        ScheduledExecutors.scheduleWithFixedDelay(
            exec,
            config.getCoordinatorStartDelay(),
            coordinatorRunnable.rhs,
            new Callable<ScheduledExecutors.Signal>()
            {
              private final CoordinatorRunnable theRunnable = coordinatorRunnable.lhs;

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
      segmentsMetadata.stopPollingDatabasePeriodically();
    }
  }

  private List<DruidCoordinatorHelper> makeIndexingServiceHelpers()
  {
    List<DruidCoordinatorHelper> helpers = new ArrayList<>();
    helpers.add(new DruidCoordinatorSegmentInfoLoader(DruidCoordinator.this));
    helpers.add(segmentCompactor);
    helpers.addAll(indexingServiceHelpers);

    log.info(
        "Done making indexing service helpers [%s]",
        helpers.stream().map(helper -> helper.getClass().getName()).collect(Collectors.toList())
    );
    return ImmutableList.copyOf(helpers);
  }

  public abstract class CoordinatorRunnable implements Runnable
  {
    private final long startTimeNanos = System.nanoTime();
    private final List<DruidCoordinatorHelper> helpers;
    private final int startingLeaderCounter;

    protected CoordinatorRunnable(List<DruidCoordinatorHelper> helpers, final int startingLeaderCounter)
    {
      this.helpers = helpers;
      this.startingLeaderCounter = startingLeaderCounter;
    }

    @Override
    public void run()
    {
      ListeningExecutorService balancerExec = null;
      try {
        synchronized (lock) {
          if (!coordLeaderSelector.isLeader()) {
            log.info("LEGGO MY EGGO. [%s] is leader.", coordLeaderSelector.getCurrentLeader());
            stopBeingLeader();
            return;
          }
        }

        List<Boolean> allStarted = Arrays.asList(
            segmentsMetadata.isPollingDatabasePeriodically(),
            serverInventoryView.isStarted()
        );
        for (Boolean aBoolean : allStarted) {
          if (!aBoolean) {
            log.error("InventoryManagers not started[%s]", allStarted);
            stopBeingLeader();
            return;
          }
        }

        balancerExec = MoreExecutors.listeningDecorator(Execs.multiThreaded(
            getDynamicConfigs().getBalancerComputeThreads(),
            "coordinator-cost-balancer-%s"
        ));
        BalancerStrategy balancerStrategy = factory.createBalancerStrategy(balancerExec);

        // Do coordinator stuff.
        DataSourcesSnapshot dataSourcesSnapshot = segmentsMetadata.getSnapshotOfDataSourcesWithAllUsedSegments();

        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams
                .newBuilder()
                .withStartTimeNanos(startTimeNanos)
                .withSnapshotOfDataSourcesWithAllUsedSegments(dataSourcesSnapshot)
                .withDynamicConfigs(getDynamicConfigs())
                .withCompactionConfig(getCompactionConfig())
                .withEmitter(emitter)
                .withBalancerStrategy(balancerStrategy)
                .build();

        for (DruidCoordinatorHelper helper : helpers) {
          // Don't read state and run state in the same helper otherwise racy conditions may exist
          if (coordLeaderSelector.isLeader() && startingLeaderCounter == coordLeaderSelector.localTerm()) {
            params = helper.run(params);

            if (params == null) {
              // This helper wanted to cancel the run. No log message, since the helper should have logged a reason.
              return;
            }
          }
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
      }
      finally {
        if (balancerExec != null) {
          balancerExec.shutdownNow();
        }
      }
    }
  }

  private class CoordinatorHistoricalManagerRunnable extends CoordinatorRunnable
  {
    public CoordinatorHistoricalManagerRunnable(final int startingLeaderCounter)
    {
      super(
          ImmutableList.of(
              new DruidCoordinatorSegmentInfoLoader(DruidCoordinator.this),
              params -> {
                List<ImmutableDruidServer> servers = serverInventoryView
                    .getInventory()
                    .stream()
                    .filter(DruidServer::segmentReplicatable)
                    .map(DruidServer::toImmutableDruidServer)
                    .collect(Collectors.toList());

                if (log.isDebugEnabled()) {
                  // Display info about all historical servers
                  log.debug("Servers");
                  for (ImmutableDruidServer druidServer : servers) {
                    log.debug("  %s", druidServer);
                    log.debug("    -- DataSources");
                    for (ImmutableDruidDataSource druidDataSource : druidServer.getDataSources()) {
                      log.debug("    %s", druidDataSource);
                    }
                  }
                }

                // Find all historical servers, group them by subType and sort by ascending usage
                Set<String> decommissioningServers = params.getCoordinatorDynamicConfig().getDecommissioningNodes();
                final DruidCluster cluster = new DruidCluster();
                for (ImmutableDruidServer server : servers) {
                  if (!loadManagementPeons.containsKey(server.getName())) {
                    LoadQueuePeon loadQueuePeon = taskMaster.giveMePeon(server);
                    loadQueuePeon.start();
                    log.info("Created LoadQueuePeon for server[%s].", server.getName());

                    loadManagementPeons.put(server.getName(), loadQueuePeon);
                  }

                  cluster.add(
                      new ServerHolder(
                          server,
                          loadManagementPeons.get(server.getName()),
                          decommissioningServers.contains(server.getHost())
                      )
                  );
                }

                segmentReplicantLookup = SegmentReplicantLookup.make(cluster);

                // Stop peons for servers that aren't there anymore.
                final Set<String> disappeared = Sets.newHashSet(loadManagementPeons.keySet());
                for (ImmutableDruidServer server : servers) {
                  disappeared.remove(server.getName());
                }
                for (String name : disappeared) {
                  log.info("Removing listener for server[%s] which is no longer there.", name);
                  LoadQueuePeon peon = loadManagementPeons.remove(name);
                  peon.stop();
                }

                return params.buildFromExisting()
                             .withDruidCluster(cluster)
                             .withDatabaseRuleManager(metadataRuleManager)
                             .withLoadManagementPeons(loadManagementPeons)
                             .withSegmentReplicantLookup(segmentReplicantLookup)
                             .withBalancerReferenceTimestamp(DateTimes.nowUtc())
                             .build();
              },
              new DruidCoordinatorRuleRunner(DruidCoordinator.this),
              new DruidCoordinatorCleanupUnneeded(),
              new DruidCoordinatorCleanupOvershadowed(DruidCoordinator.this),
              new DruidCoordinatorBalancer(DruidCoordinator.this),
              new DruidCoordinatorLogger(DruidCoordinator.this)
          ),
          startingLeaderCounter
      );
    }
  }

  private class CoordinatorIndexingServiceRunnable extends CoordinatorRunnable
  {
    public CoordinatorIndexingServiceRunnable(List<DruidCoordinatorHelper> helpers, final int startingLeaderCounter)
    {
      super(helpers, startingLeaderCounter);
    }
  }
}

