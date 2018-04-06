/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.ServerInventoryView;
import io.druid.client.coordinator.Coordinator;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.config.JacksonConfigManager;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.discovery.DruidLeaderSelector;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.CoordinatorIndexingServiceHelper;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.DruidNode;
import io.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import io.druid.server.coordinator.helper.DruidCoordinatorCleanupOvershadowed;
import io.druid.server.coordinator.helper.DruidCoordinatorCleanupUnneeded;
import io.druid.server.coordinator.helper.DruidCoordinatorHelper;
import io.druid.server.coordinator.helper.DruidCoordinatorLogger;
import io.druid.server.coordinator.helper.DruidCoordinatorRuleRunner;
import io.druid.server.coordinator.helper.DruidCoordinatorSegmentInfoLoader;
import io.druid.server.coordinator.rules.LoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.lookup.cache.LookupCoordinatorManager;
import io.druid.timeline.DataSegment;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 */
@ManageLifecycle
public class DruidCoordinator
{
  public static Comparator<DataSegment> SEGMENT_COMPARATOR = Ordering.from(Comparators.intervalsByEndThenStart())
                                                                     .onResultOf(
                                                                         (Function<DataSegment, Interval>) segment -> segment
                                                                             .getInterval())
                                                                     .compound(Ordering.<DataSegment>natural())
                                                                     .reverse();

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinator.class);
  private final Object lock = new Object();
  private final DruidCoordinatorConfig config;
  private final ZkPathsConfig zkPaths;
  private final JacksonConfigManager configManager;
  private final MetadataSegmentManager metadataSegmentManager;
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
  private volatile boolean started = false;
  private volatile SegmentReplicantLookup segmentReplicantLookup = null;
  private final BalancerStrategyFactory factory;
  private final LookupCoordinatorManager lookupCoordinatorManager;
  private final DruidLeaderSelector coordLeaderSelector;

  @Inject
  public DruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      MetadataSegmentManager metadataSegmentManager,
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
      @Coordinator DruidLeaderSelector coordLeaderSelector
  )
  {
    this(
        config,
        zkPaths,
        configManager,
        metadataSegmentManager,
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
        coordLeaderSelector
    );
  }

  DruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      MetadataSegmentManager metadataSegmentManager,
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
      DruidLeaderSelector coordLeaderSelector
  )
  {
    this.config = config;
    this.zkPaths = zkPaths;
    this.configManager = configManager;

    this.metadataSegmentManager = metadataSegmentManager;
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
  }

  public boolean isLeader()
  {
    return coordLeaderSelector.isLeader();
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  public Map<String, ? extends Object2LongMap<String>> getReplicationStatus()
  {
    final Map<String, Object2LongOpenHashMap<String>> retVal = Maps.newHashMap();

    if (segmentReplicantLookup == null) {
      return retVal;
    }

    final DateTime now = DateTimes.nowUtc();

    for (final DataSegment segment : getAvailableDataSegments()) {
      final List<Rule> rules = metadataRuleManager.getRulesWithDefault(segment.getDataSource());

      for (final Rule rule : rules) {
        if (!(rule instanceof LoadRule && rule.appliesTo(segment, now))) {
          continue;
        }

        ((LoadRule) rule)
            .getTieredReplicants()
            .forEach((final String tier, final Integer ruleReplicants) -> {
              int currentReplicants = segmentReplicantLookup.getLoadedReplicants(segment.getIdentifier(), tier);
              retVal
                  .computeIfAbsent(tier, ignored -> new Object2LongOpenHashMap<>())
                  .addTo(segment.getDataSource(), Math.max(ruleReplicants - currentReplicants, 0));
            });
        break; // only the first matching rule applies
      }
    }

    return retVal;
  }

  public Object2LongMap<String> getSegmentAvailability()
  {
    final Object2LongOpenHashMap<String> retVal = new Object2LongOpenHashMap<>();

    if (segmentReplicantLookup == null) {
      return retVal;
    }

    for (DataSegment segment : getAvailableDataSegments()) {
      if (segmentReplicantLookup.getLoadedReplicants(segment.getIdentifier()) == 0) {
        retVal.addTo(segment.getDataSource(), 1);
      } else {
        retVal.addTo(segment.getDataSource(), 0);
      }
    }

    return retVal;
  }

  boolean hasLoadPending(final String dataSource)
  {
    return loadManagementPeons
        .values()
        .stream()
        .flatMap((final LoadQueuePeon peon) -> peon.getSegmentsToLoad().stream())
        .anyMatch((final DataSegment segment) -> segment.getDataSource().equals(dataSource));
  }

  public Map<String, Double> getLoadStatus()
  {
    Map<String, Double> loadStatus = Maps.newHashMap();
    for (ImmutableDruidDataSource dataSource : metadataSegmentManager.getInventory()) {
      final Set<DataSegment> segments = Sets.newHashSet(dataSource.getSegments());
      final int availableSegmentSize = segments.size();

      // remove loaded segments
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        final DruidDataSource loadedView = druidServer.getDataSource(dataSource.getName());
        if (loadedView != null) {
          // This does not use segments.removeAll(loadedView.getSegments()) for performance reasons.
          // Please see https://github.com/druid-io/druid/pull/5632 and LoadStatusBenchmark for more info.
          for (DataSegment serverSegment : loadedView.getSegments()) {
            segments.remove(serverSegment);
          }
        }
      }
      final int unloadedSegmentSize = segments.size();
      loadStatus.put(
          dataSource.getName(),
          100 * ((double) (availableSegmentSize - unloadedSegmentSize) / (double) availableSegmentSize)
      );
    }

    return loadStatus;
  }

  public CoordinatorDynamicConfig getDynamicConfigs()
  {
    return configManager.watch(
        CoordinatorDynamicConfig.CONFIG_KEY,
        CoordinatorDynamicConfig.class,
        new CoordinatorDynamicConfig.Builder().build()
    ).get();
  }

  public void removeSegment(DataSegment segment)
  {
    log.info("Removing Segment[%s]", segment);
    metadataSegmentManager.removeSegment(segment.getDataSource(), segment.getIdentifier());
  }

  public String getCurrentLeader()
  {
    return coordLeaderSelector.getCurrentLeader();
  }

  public void moveSegment(
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
    String segmentName = segment.getIdentifier();
    try {
      if (fromServer.getMetadata().equals(toServer.getMetadata())) {
        throw new IAE("Cannot move [%s] to and from the same server [%s]", segmentName, fromServer.getName());
      }

      ImmutableDruidDataSource dataSource = metadataSegmentManager.getInventoryValue(segment.getDataSource());
      if (dataSource == null) {
        throw new IAE("Unable to find dataSource for segment [%s] in metadata", segmentName);
      }

      // get segment information from MetadataSegmentManager instead of getting it from fromServer's.
      // This is useful when MetadataSegmentManager and fromServer DataSegment's are different for same
      // identifier (say loadSpec differs because of deep storage migration).
      final DataSegment segmentToLoad = dataSource.getSegment(segment.getIdentifier());
      if (segmentToLoad == null) {
        throw new IAE("No segment metadata found for segment Id [%s]", segment.getIdentifier());
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

      final String toLoadQueueSegPath = ZKPaths.makePath(
          ZKPaths.makePath(
              zkPaths.getLoadQueuePath(),
              toServer.getName()
          ), segmentName
      );

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
                throw Throwables.propagate(e);
              }
            }
        );
      }
      catch (Exception e) {
        dropPeon.unmarkSegmentToDrop(segmentToLoad);
        Throwables.propagate(e);
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception moving segment %s", segmentName).emit();
      if (callback != null) {
        callback.execute();
      }
    }
  }

  public Set<DataSegment> getOrderedAvailableDataSegments()
  {
    Set<DataSegment> availableSegments = Sets.newTreeSet(SEGMENT_COMPARATOR);

    Iterable<DataSegment> dataSegments = getAvailableDataSegments();

    for (DataSegment dataSegment : dataSegments) {
      if (dataSegment.getSize() < 0) {
        log.makeAlert("No size on Segment, wtf?")
           .addData("segment", dataSegment)
           .emit();
      }
      availableSegments.add(dataSegment);
    }

    return availableSegments;
  }

  private List<DataSegment> getAvailableDataSegments()
  {
    return metadataSegmentManager.getInventory()
                                 .stream()
                                 .flatMap(source -> source.getSegments().stream())
                                 .collect(Collectors.toList());
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

      metadataSegmentManager.start();
      metadataRuleManager.start();
      serviceAnnouncer.announce(self);
      final int startingLeaderCounter = coordLeaderSelector.localTerm();

      final List<Pair<? extends CoordinatorRunnable, Duration>> coordinatorRunnables = Lists.newArrayList();
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

      lookupCoordinatorManager.start();
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
      metadataRuleManager.stop();
      metadataSegmentManager.stop();
      lookupCoordinatorManager.stop();
    }
  }

  private List<DruidCoordinatorHelper> makeIndexingServiceHelpers()
  {
    List<DruidCoordinatorHelper> helpers = Lists.newArrayList();
    helpers.add(new DruidCoordinatorSegmentInfoLoader(DruidCoordinator.this));
    helpers.addAll(indexingServiceHelpers);

    log.info("Done making indexing service helpers [%s]", helpers);
    return ImmutableList.copyOf(helpers);
  }

  public abstract class CoordinatorRunnable implements Runnable
  {
    private final long startTime = System.currentTimeMillis();
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
            metadataSegmentManager.isStarted(),
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
        DruidCoordinatorRuntimeParams params =
                DruidCoordinatorRuntimeParams.newBuilder()
                        .withStartTime(startTime)
                        .withDatasources(metadataSegmentManager.getInventory())
                        .withDynamicConfigs(getDynamicConfigs())
                        .withEmitter(emitter)
                        .withBalancerStrategy(balancerStrategy)
                        .build();
        for (DruidCoordinatorHelper helper : helpers) {
          // Don't read state and run state in the same helper otherwise racy conditions may exist
          if (coordLeaderSelector.isLeader() && startingLeaderCounter == coordLeaderSelector.localTerm()) {
            params = helper.run(params);
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
              new DruidCoordinatorHelper()
              {
                @Override
                public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
                {
                  // Display info about all historical servers
                  Iterable<ImmutableDruidServer> servers = FunctionalIterable
                      .create(serverInventoryView.getInventory())
                      .filter(DruidServer::segmentReplicatable)
                      .transform(DruidServer::toImmutableDruidServer);

                  if (log.isDebugEnabled()) {
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
                  final DruidCluster cluster = new DruidCluster();
                  for (ImmutableDruidServer server : servers) {
                    if (!loadManagementPeons.containsKey(server.getName())) {
                      LoadQueuePeon loadQueuePeon = taskMaster.giveMePeon(server);
                      loadQueuePeon.start();
                      log.info("Created LoadQueuePeon for server[%s].", server.getName());

                      loadManagementPeons.put(server.getName(), loadQueuePeon);
                    }

                    cluster.add(new ServerHolder(server, loadManagementPeons.get(server.getName())));
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
                }
              },
              new DruidCoordinatorRuleRunner(DruidCoordinator.this),
              new DruidCoordinatorCleanupUnneeded(DruidCoordinator.this),
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

