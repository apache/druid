/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Comparators;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.ServerInventoryView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.collections.CountingMap;
import io.druid.common.config.JacksonConfigManager;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.segment.IndexIO;
import io.druid.server.DruidNode;
import io.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import io.druid.server.coordinator.helper.DruidCoordinatorCleanupOvershadowed;
import io.druid.server.coordinator.helper.DruidCoordinatorCleanupUnneeded;
import io.druid.server.coordinator.helper.DruidCoordinatorHelper;
import io.druid.server.coordinator.helper.DruidCoordinatorLogger;
import io.druid.server.coordinator.helper.DruidCoordinatorRuleRunner;
import io.druid.server.coordinator.helper.DruidCoordinatorSegmentInfoLoader;
import io.druid.server.coordinator.helper.DruidCoordinatorSegmentMerger;
import io.druid.server.coordinator.rules.LoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@ManageLifecycle
public class DruidCoordinator
{
  public static final String COORDINATOR_OWNER_NODE = "_COORDINATOR";
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinator.class);
  private final Object lock = new Object();
  private final DruidCoordinatorConfig config;
  private final ZkPathsConfig zkPaths;
  private final JacksonConfigManager configManager;
  private final MetadataSegmentManager metadataSegmentManager;
  private final ServerInventoryView<Object> serverInventoryView;
  private final MetadataRuleManager metadataRuleManager;
  private final CuratorFramework curator;
  private final ServiceEmitter emitter;
  private final IndexingServiceClient indexingServiceClient;
  private final ScheduledExecutorService exec;
  private final LoadQueueTaskMaster taskMaster;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final AtomicReference<LeaderLatch> leaderLatch;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DruidNode self;
  private volatile boolean started = false;
  private volatile int leaderCounter = 0;
  private volatile boolean leader = false;
  private volatile SegmentReplicantLookup segmentReplicantLookup = null;


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
      @Self DruidNode self
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
        Maps.<String, LoadQueuePeon>newConcurrentMap()
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
      ConcurrentMap<String, LoadQueuePeon> loadQueuePeonMap
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

    this.exec = scheduledExecutorFactory.create(1, "Coordinator-Exec--%d");

    this.leaderLatch = new AtomicReference<>(null);
    this.loadManagementPeons = loadQueuePeonMap;
  }

  public boolean isLeader()
  {
    return leader;
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  public Map<String, CountingMap<String>> getReplicationStatus()
  {
    final Map<String, CountingMap<String>> retVal = Maps.newHashMap();

    if (segmentReplicantLookup == null) {
      return retVal;
    }

    final DateTime now = new DateTime();
    for (DataSegment segment : getAvailableDataSegments()) {
      List<Rule> rules = metadataRuleManager.getRulesWithDefault(segment.getDataSource());
      for (Rule rule : rules) {
        if (rule instanceof LoadRule && rule.appliesTo(segment, now)) {
          for (Map.Entry<String, Integer> entry : ((LoadRule) rule).getTieredReplicants().entrySet()) {
            CountingMap<String> dataSourceMap = retVal.get(entry.getKey());
            if (dataSourceMap == null) {
              dataSourceMap = new CountingMap<>();
              retVal.put(entry.getKey(), dataSourceMap);
            }

            int diff = Math.max(
                entry.getValue() - segmentReplicantLookup.getTotalReplicants(segment.getIdentifier(), entry.getKey()),
                0
            );
            dataSourceMap.add(segment.getDataSource(), diff);
          }
          break;
        }
      }
    }

    return retVal;
  }

  public CountingMap<String> getSegmentAvailability()
  {
    final CountingMap<String> retVal = new CountingMap<>();

    if (segmentReplicantLookup == null) {
      return retVal;
    }

    for (DataSegment segment : getAvailableDataSegments()) {
      int available = (segmentReplicantLookup.getTotalReplicants(segment.getIdentifier()) == 0) ? 0 : 1;
      retVal.add(segment.getDataSource(), 1 - available);
    }

    return retVal;
  }

  public Map<String, Double> getLoadStatus()
  {
    Map<String, Double> loadStatus = Maps.newHashMap();
    for (DruidDataSource dataSource : metadataSegmentManager.getInventory()) {
      final Set<DataSegment> segments = Sets.newHashSet(dataSource.getSegments());
      final int availableSegmentSize = segments.size();

      // remove loaded segments
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        final DruidDataSource loadedView = druidServer.getDataSource(dataSource.getName());
        if (loadedView != null) {
          segments.removeAll(loadedView.getSegments());
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

  public void removeDatasource(String ds)
  {
    metadataSegmentManager.removeDatasource(ds);
  }

  public void enableDatasource(String ds)
  {
    metadataSegmentManager.enableDatasource(ds);
  }

  public String getCurrentLeader()
  {
    try {
      final LeaderLatch latch = leaderLatch.get();

      if (latch == null) {
        return null;
      }

      Participant participant = latch.getLeader();
      if (participant.isLeader()) {
        return participant.getId();
      }

      return null;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void moveSegment(
      ImmutableDruidServer fromServer,
      ImmutableDruidServer toServer,
      String segmentName,
      final LoadPeonCallback callback
  )
  {
    try {
      if (fromServer.getMetadata().equals(toServer.getMetadata())) {
        throw new IAE("Cannot move [%s] to and from the same server [%s]", segmentName, fromServer.getName());
      }

      final DataSegment segment = fromServer.getSegment(segmentName);
      if (segment == null) {
        throw new IAE("Unable to find segment [%s] on server [%s]", segmentName, fromServer.getName());
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
      if (toHolder.getAvailableSize() < segment.getSize()) {
        throw new IAE(
            "Not enough capacity on server [%s] for segment [%s]. Required: %,d, available: %,d.",
            toServer.getName(),
            segment,
            segment.getSize(),
            toHolder.getAvailableSize()
        );
      }

      final String toLoadQueueSegPath = ZKPaths.makePath(
          ZKPaths.makePath(
              zkPaths.getLoadQueuePath(),
              toServer.getName()
          ), segmentName
      );
      final String toServedSegPath = ZKPaths.makePath(
          ZKPaths.makePath(serverInventoryView.getInventoryManagerConfig().getInventoryPath(), toServer.getName()),
          segmentName
      );

      loadPeon.loadSegment(
          segment,
          new LoadPeonCallback()
          {
            @Override
            public void execute()
            {
              try {
                if (curator.checkExists().forPath(toServedSegPath) != null &&
                    curator.checkExists().forPath(toLoadQueueSegPath) == null &&
                    !dropPeon.getSegmentsToDrop().contains(segment)) {
                  dropPeon.dropSegment(segment, callback);
                } else if (callback != null) {
                  callback.execute();
                }
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            }
          }
      );
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
    Set<DataSegment> availableSegments = Sets.newTreeSet(Comparators.inverse(DataSegment.bucketMonthComparator()));

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

  public Iterable<DataSegment> getAvailableDataSegments()
  {
    return Iterables.concat(
        Iterables.transform(
            metadataSegmentManager.getInventory(),
            new Function<DruidDataSource, Iterable<DataSegment>>()
            {
              @Override
              public Iterable<DataSegment> apply(DruidDataSource input)
              {
                return input.getSegments();
              }
            }
        )
    );
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }
      started = true;

      createNewLeaderLatch();
      try {
        leaderLatch.get().start();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private LeaderLatch createNewLeaderLatch()
  {
    final LeaderLatch newLeaderLatch = new LeaderLatch(
        curator, ZKPaths.makePath(zkPaths.getCoordinatorPath(), COORDINATOR_OWNER_NODE), self.getHostAndPort()
    );

    newLeaderLatch.addListener(
        new LeaderLatchListener()
        {
          @Override
          public void isLeader()
          {
            DruidCoordinator.this.becomeLeader();
          }

          @Override
          public void notLeader()
          {
            DruidCoordinator.this.stopBeingLeader();
          }
        },
        Execs.singleThreaded("CoordinatorLeader-%s")
    );

    return leaderLatch.getAndSet(newLeaderLatch);
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      stopBeingLeader();

      try {
        leaderLatch.get().close();
      }
      catch (IOException e) {
        log.warn(e, "Unable to close leaderLatch, ignoring");
      }

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
      try {
        leaderCounter++;
        leader = true;
        metadataSegmentManager.start();
        metadataRuleManager.start();
        serverInventoryView.start();
        serviceAnnouncer.announce(self);
        final int startingLeaderCounter = leaderCounter;

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
                      makeIndexingServiceHelpers(
                          configManager.watch(
                              DatasourceWhitelist.CONFIG_KEY,
                              DatasourceWhitelist.class
                          )
                      ),
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
                  if (leader && startingLeaderCounter == leaderCounter) {
                    theRunnable.run();
                  }
                  if (leader && startingLeaderCounter == leaderCounter) { // (We might no longer be leader)
                    return ScheduledExecutors.Signal.REPEAT;
                  } else {
                    return ScheduledExecutors.Signal.STOP;
                  }
                }
              }
          );
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to become leader")
           .emit();
        final LeaderLatch oldLatch = createNewLeaderLatch();
        CloseQuietly.close(oldLatch);
        try {
          leaderLatch.get().start();
        }
        catch (Exception e1) {
          // If an exception gets thrown out here, then the coordinator will zombie out 'cause it won't be looking for
          // the latch anymore.  I don't believe it's actually possible for an Exception to throw out here, but
          // Curator likes to have "throws Exception" on methods so it might happen...
          log.makeAlert(e1, "I am a zombie")
             .emit();
        }
      }
    }
  }

  private void stopBeingLeader()
  {
    synchronized (lock) {
      try {
        leaderCounter++;

        log.info("I am no longer the leader...");

        for (String server : loadManagementPeons.keySet()) {
          LoadQueuePeon peon = loadManagementPeons.remove(server);
          peon.stop();
        }
        loadManagementPeons.clear();

        serviceAnnouncer.unannounce(self);
        serverInventoryView.stop();
        metadataRuleManager.stop();
        metadataSegmentManager.stop();
        leader = false;
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to stopBeingLeader").emit();
      }
    }
  }

  private List<DruidCoordinatorHelper> makeIndexingServiceHelpers(final AtomicReference<DatasourceWhitelist> whitelistRef)
  {
    List<DruidCoordinatorHelper> helpers = Lists.newArrayList();

    helpers.add(new DruidCoordinatorSegmentInfoLoader(DruidCoordinator.this));

    if (config.isConvertSegments()) {
      helpers.add(new DruidCoordinatorVersionConverter(indexingServiceClient, whitelistRef));
    }
    if (config.isMergeSegments()) {
      helpers.add(new DruidCoordinatorSegmentMerger(indexingServiceClient, whitelistRef));
      helpers.add(
          new DruidCoordinatorHelper()
          {
            @Override
            public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
            {
              CoordinatorStats stats = params.getCoordinatorStats();
              log.info("Issued merge requests for %s segments", stats.getGlobalStats().get("mergedCount").get());

              params.getEmitter().emit(
                  new ServiceMetricEvent.Builder().build(
                      "coordinator/merge/count", stats.getGlobalStats().get("mergedCount")
                  )
              );

              return params;
            }
          }
      );
    }

    return ImmutableList.copyOf(helpers);
  }

  public static class DruidCoordinatorVersionConverter implements DruidCoordinatorHelper
  {
    private final IndexingServiceClient indexingServiceClient;
    private final AtomicReference<DatasourceWhitelist> whitelistRef;

    public DruidCoordinatorVersionConverter(
        IndexingServiceClient indexingServiceClient,
        AtomicReference<DatasourceWhitelist> whitelistRef
    )
    {
      this.indexingServiceClient = indexingServiceClient;
      this.whitelistRef = whitelistRef;
    }

    @Override
    public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
    {
      DatasourceWhitelist whitelist = whitelistRef.get();

      for (DataSegment dataSegment : params.getAvailableSegments()) {
        if (whitelist == null || whitelist.contains(dataSegment.getDataSource())) {
          final Integer binaryVersion = dataSegment.getBinaryVersion();

          if (binaryVersion == null || binaryVersion < IndexIO.CURRENT_VERSION_ID) {
            log.info("Upgrading version on segment[%s]", dataSegment.getIdentifier());
            indexingServiceClient.upgradeSegment(dataSegment);
          }
        }
      }

      return params;
    }
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
      try {
        synchronized (lock) {
          final LeaderLatch latch = leaderLatch.get();
          if (latch == null || !latch.hasLeadership()) {
            log.info("LEGGO MY EGGO. [%s] is leader.", latch == null ? null : latch.getLeader().getId());
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

        BalancerStrategyFactory factory =
            new CostBalancerStrategyFactory(getDynamicConfigs().getBalancerComputeThreads());

        // Do coordinator stuff.
        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams.newBuilder()
                                         .withStartTime(startTime)
                                         .withDatasources(metadataSegmentManager.getInventory())
                                         .withDynamicConfigs(getDynamicConfigs())
                                         .withEmitter(emitter)
                                         .withBalancerStrategyFactory(factory)
                                         .build();

        for (DruidCoordinatorHelper helper : helpers) {
          // Don't read state and run state in the same helper otherwise racy conditions may exist
          if (leader && startingLeaderCounter == leaderCounter) {
            params = helper.run(params);
          }
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
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
                      .filter(
                          new Predicate<DruidServer>()
                          {
                            @Override
                            public boolean apply(
                                DruidServer input
                            )
                            {
                              return input.isAssignable();
                            }
                          }
                      ).transform(
                          new Function<DruidServer, ImmutableDruidServer>()
                          {
                            @Override
                            public ImmutableDruidServer apply(DruidServer input)
                            {
                              return input.toImmutableDruidServer();
                            }
                          }
                      );

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
                      String basePath = ZKPaths.makePath(zkPaths.getLoadQueuePath(), server.getName());
                      LoadQueuePeon loadQueuePeon = taskMaster.giveMePeon(basePath);
                      log.info("Creating LoadQueuePeon for server[%s] at path[%s]", server.getName(), basePath);

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
                               .withBalancerReferenceTimestamp(DateTime.now())
                               .build();
                }
              },
              new DruidCoordinatorRuleRunner(DruidCoordinator.this),
              new DruidCoordinatorCleanupUnneeded(DruidCoordinator.this),
              new DruidCoordinatorCleanupOvershadowed(DruidCoordinator.this),
              new DruidCoordinatorBalancer(DruidCoordinator.this),
              new DruidCoordinatorLogger()
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

