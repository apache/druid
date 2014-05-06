/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.Comparators;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ServerInventoryView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.collections.CountingMap;
import io.druid.common.config.JacksonConfigManager;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.db.DatabaseRuleManager;
import io.druid.db.DatabaseSegmentManager;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.segment.IndexIO;
import io.druid.server.DruidNode;
import io.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import io.druid.server.coordinator.helper.DruidCoordinatorCleanup;
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
  private final DatabaseSegmentManager databaseSegmentManager;
  private final ServerInventoryView<Object> serverInventoryView;
  private final DatabaseRuleManager databaseRuleManager;
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
      DatabaseSegmentManager databaseSegmentManager,
      ServerInventoryView serverInventoryView,
      DatabaseRuleManager databaseRuleManager,
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
        databaseSegmentManager,
        serverInventoryView,
        databaseRuleManager,
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
      DatabaseSegmentManager databaseSegmentManager,
      ServerInventoryView serverInventoryView,
      DatabaseRuleManager databaseRuleManager,
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

    this.databaseSegmentManager = databaseSegmentManager;
    this.serverInventoryView = serverInventoryView;
    this.databaseRuleManager = databaseRuleManager;
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
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(segment.getDataSource());
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
    // find available segments
    Map<String, Set<DataSegment>> availableSegments = Maps.newHashMap();
    for (DataSegment dataSegment : getAvailableDataSegments()) {
      Set<DataSegment> segments = availableSegments.get(dataSegment.getDataSource());
      if (segments == null) {
        segments = Sets.newHashSet();
        availableSegments.put(dataSegment.getDataSource(), segments);
      }
      segments.add(dataSegment);
    }

    // find segments currently loaded
    Map<String, Set<DataSegment>> segmentsInCluster = Maps.newHashMap();
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      for (DruidDataSource druidDataSource : druidServer.getDataSources()) {
        Set<DataSegment> segments = segmentsInCluster.get(druidDataSource.getName());
        if (segments == null) {
          segments = Sets.newHashSet();
          segmentsInCluster.put(druidDataSource.getName(), segments);
        }
        segments.addAll(druidDataSource.getSegments());
      }
    }

    // compare available segments with currently loaded
    Map<String, Double> loadStatus = Maps.newHashMap();
    for (Map.Entry<String, Set<DataSegment>> entry : availableSegments.entrySet()) {
      String dataSource = entry.getKey();
      Set<DataSegment> segmentsAvailable = entry.getValue();
      Set<DataSegment> loadedSegments = segmentsInCluster.get(dataSource);
      if (loadedSegments == null) {
        loadedSegments = Sets.newHashSet();
      }
      Set<DataSegment> unloadedSegments = Sets.difference(segmentsAvailable, loadedSegments);
      loadStatus.put(
          dataSource,
          100 * ((double) (segmentsAvailable.size() - unloadedSegments.size()) / (double) segmentsAvailable.size())
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
    databaseSegmentManager.removeSegment(segment.getDataSource(), segment.getIdentifier());
  }

  public void removeDatasource(String ds)
  {
    databaseSegmentManager.removeDatasource(ds);
  }

  public void enableDatasource(String ds)
  {
    databaseSegmentManager.enableDatasource(ds);
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

  public void moveSegment(String from, String to, String segmentName, final LoadPeonCallback callback)
  {
    try {
      final DruidServer fromServer = serverInventoryView.getInventoryValue(from);
      if (fromServer == null) {
        throw new IAE("Unable to find server [%s]", from);
      }

      final DruidServer toServer = serverInventoryView.getInventoryValue(to);
      if (toServer == null) {
        throw new IAE("Unable to find server [%s]", to);
      }

      if (to.equalsIgnoreCase(from)) {
        throw new IAE("Redundant command to move segment [%s] from [%s] to [%s]", segmentName, from, to);
      }

      final DataSegment segment = fromServer.getSegment(segmentName);
      if (segment == null) {
        throw new IAE("Unable to find segment [%s] on server [%s]", segmentName, from);
      }

      final LoadQueuePeon loadPeon = loadManagementPeons.get(to);
      if (loadPeon == null) {
        throw new IAE("LoadQueuePeon hasn't been created yet for path [%s]", to);
      }

      final LoadQueuePeon dropPeon = loadManagementPeons.get(from);
      if (dropPeon == null) {
        throw new IAE("LoadQueuePeon hasn't been created yet for path [%s]", from);
      }

      final ServerHolder toHolder = new ServerHolder(toServer, loadPeon);
      if (toHolder.getAvailableSize() < segment.getSize()) {
        throw new IAE(
            "Not enough capacity on server [%s] for segment [%s]. Required: %,d, available: %,d.",
            to,
            segment,
            segment.getSize(),
            toHolder.getAvailableSize()
        );
      }

      final String toLoadQueueSegPath = ZKPaths.makePath(ZKPaths.makePath(zkPaths.getLoadQueuePath(), to), segmentName);
      final String toServedSegPath = ZKPaths.makePath(
          ZKPaths.makePath(serverInventoryView.getInventoryManagerConfig().getInventoryPath(), to), segmentName
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
      callback.execute();
    }
  }

  public void dropSegment(String from, String segmentName, final LoadPeonCallback callback)
  {
    try {
      final DruidServer fromServer = serverInventoryView.getInventoryValue(from);
      if (fromServer == null) {
        throw new IAE("Unable to find server [%s]", from);
      }

      final DataSegment segment = fromServer.getSegment(segmentName);
      if (segment == null) {
        throw new IAE("Unable to find segment [%s] on server [%s]", segmentName, from);
      }

      final LoadQueuePeon dropPeon = loadManagementPeons.get(from);
      if (dropPeon == null) {
        throw new IAE("LoadQueuePeon hasn't been created yet for path [%s]", from);
      }

      if (!dropPeon.getSegmentsToDrop().contains(segment)) {
        dropPeon.dropSegment(segment, callback);
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception dropping segment %s", segmentName).emit();
      callback.execute();
    }
  }

  public Set<DataSegment> getAvailableDataSegments()
  {
    Set<DataSegment> availableSegments = Sets.newTreeSet(Comparators.inverse(DataSegment.bucketMonthComparator()));

    Iterable<DataSegment> dataSegments = Iterables.concat(
        Iterables.transform(
            databaseSegmentManager.getInventory(),
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
        curator, ZKPaths.makePath(zkPaths.getCoordinatorPath(), COORDINATOR_OWNER_NODE), config.getHost()
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
      try {
        leaderCounter++;
        leader = true;
        databaseSegmentManager.start();
        databaseRuleManager.start();
        serverInventoryView.start();
        serviceAnnouncer.announce(self);

        final List<Pair<? extends CoordinatorRunnable, Duration>> coordinatorRunnables = Lists.newArrayList();
        coordinatorRunnables.add(Pair.of(new CoordinatorHistoricalManagerRunnable(), config.getCoordinatorPeriod()));
        if (indexingServiceClient != null) {
          coordinatorRunnables.add(
              Pair.of(
                  new CoordinatorIndexingServiceRunnable(
                      makeIndexingServiceHelpers(
                          configManager.watch(
                              DatasourceWhitelist.CONFIG_KEY,
                              DatasourceWhitelist.class
                          )
                      )
                  ),
                  config.getCoordinatorIndexingPeriod()
              )
          );
        }

        final int startingLeaderCounter = leaderCounter;
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
        Closeables.closeQuietly(oldLatch);
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
        databaseRuleManager.stop();
        databaseSegmentManager.stop();
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

    protected CoordinatorRunnable(List<DruidCoordinatorHelper> helpers)
    {
      this.helpers = helpers;
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
            databaseSegmentManager.isStarted(),
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
        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams.newBuilder()
                                         .withStartTime(startTime)
                                         .withDatasources(databaseSegmentManager.getInventory())
                                         .withDynamicConfigs(getDynamicConfigs())
                                         .withEmitter(emitter)
                                         .build();

        for (DruidCoordinatorHelper helper : helpers) {
          params = helper.run(params);
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
      }
    }
  }

  private class CoordinatorHistoricalManagerRunnable extends CoordinatorRunnable
  {
    private CoordinatorHistoricalManagerRunnable()
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
                  Iterable<DruidServer> servers = FunctionalIterable
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
                      );

                  if (log.isDebugEnabled()) {
                    log.debug("Servers");
                    for (DruidServer druidServer : servers) {
                      log.debug("  %s", druidServer);
                      log.debug("    -- DataSources");
                      for (DruidDataSource druidDataSource : druidServer.getDataSources()) {
                        log.debug("    %s", druidDataSource);
                      }
                    }
                  }

                  // Find all historical servers, group them by subType and sort by ascending usage
                  final DruidCluster cluster = new DruidCluster();
                  for (DruidServer server : servers) {
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
                  for (DruidServer server : servers) {
                    disappeared.remove(server.getName());
                  }
                  for (String name : disappeared) {
                    log.info("Removing listener for server[%s] which is no longer there.", name);
                    LoadQueuePeon peon = loadManagementPeons.remove(name);
                    peon.stop();
                  }

                  return params.buildFromExisting()
                               .withDruidCluster(cluster)
                               .withDatabaseRuleManager(databaseRuleManager)
                               .withLoadManagementPeons(loadManagementPeons)
                               .withSegmentReplicantLookup(segmentReplicantLookup)
                               .withBalancerReferenceTimestamp(DateTime.now())
                               .build();
                }
              },
              new DruidCoordinatorRuleRunner(DruidCoordinator.this),
              new DruidCoordinatorCleanup(DruidCoordinator.this),
              new DruidCoordinatorBalancer(DruidCoordinator.this),
              new DruidCoordinatorLogger()
          )
      );
    }
  }

  private class CoordinatorIndexingServiceRunnable extends CoordinatorRunnable
  {
    private CoordinatorIndexingServiceRunnable(List<DruidCoordinatorHelper> helpers)
    {
      super(helpers);
    }
  }
}

