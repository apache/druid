/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.master;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.Comparators;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.ServerInventoryView;
import com.metamx.druid.client.indexing.IndexingServiceClient;
import com.metamx.druid.concurrent.Execs;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.initialization.ZkPathsConfig;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
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
public class DruidMaster
{
  public static final String MASTER_OWNER_NODE = "_MASTER";

  private static final EmittingLogger log = new EmittingLogger(DruidMaster.class);

  private final Object lock = new Object();

  private volatile boolean started = false;
  private volatile boolean master = false;

  private final DruidMasterConfig config;
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
  private AtomicReference<MasterSegmentSettings> segmentSettingsAtomicReference;

  public DruidMaster(
      DruidMasterConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      DatabaseSegmentManager databaseSegmentManager,
      ServerInventoryView serverInventoryView,
      DatabaseRuleManager databaseRuleManager,
      CuratorFramework curator,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster
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
        Maps.<String, LoadQueuePeon>newConcurrentMap()
    );
  }

  DruidMaster(
      DruidMasterConfig config,
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

    this.exec = scheduledExecutorFactory.create(1, "Master-Exec--%d");

    this.leaderLatch = new AtomicReference<LeaderLatch>(null);
    this.segmentSettingsAtomicReference= new AtomicReference<MasterSegmentSettings>(null);
    this.loadManagementPeons = loadQueuePeonMap;
  }

  public boolean isClusterMaster()
  {
    return master;
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

  public int lookupSegmentLifetime(DataSegment segment)
  {
    return serverInventoryView.lookupSegmentLifetime(segment);
  }

  public void decrementRemovedSegmentsLifetime()
  {
    serverInventoryView.decrementRemovedSegmentsLifetime();
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

  public String getCurrentMaster()
  {
    try {
      final LeaderLatch latch = leaderLatch.get();
      return latch == null ? null : latch.getLeader().getId();
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
            protected void execute()
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
        curator, ZKPaths.makePath(zkPaths.getMasterPath(), MASTER_OWNER_NODE), config.getHost()
    );

    newLeaderLatch.addListener(
        new LeaderLatchListener()
        {
          @Override
          public void isLeader()
          {
            DruidMaster.this.becomeMaster();
          }

          @Override
          public void notLeader()
          {
            DruidMaster.this.stopBeingMaster();
          }
        },
        Execs.singleThreaded("MasterLeader-%s")
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

      stopBeingMaster();

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

  private void becomeMaster()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      log.info("I am the master, all must bow!");
      try {
        master = true;
        databaseSegmentManager.start();
        databaseRuleManager.start();
        serverInventoryView.start();

        final List<Pair<? extends MasterRunnable, Duration>> masterRunnables = Lists.newArrayList();
        segmentSettingsAtomicReference = configManager.watch(MasterSegmentSettings.CONFIG_KEY, MasterSegmentSettings.class,(new MasterSegmentSettings.Builder()).build());
        masterRunnables.add(Pair.of(new MasterComputeManagerRunnable(), config.getMasterPeriod()));
        if (indexingServiceClient != null) {

          masterRunnables.add(
              Pair.of(
                  new MasterIndexingServiceRunnable(
                      makeIndexingServiceHelpers(configManager.watch(MergerWhitelist.CONFIG_KEY, MergerWhitelist.class))
                  ),
                  config.getMasterSegmentMergerPeriod()
              )
          );
        }

        for (final Pair<? extends MasterRunnable, Duration> masterRunnable : masterRunnables) {
          ScheduledExecutors.scheduleWithFixedDelay(
              exec,
              config.getMasterStartDelay(),
              masterRunnable.rhs,
              new Callable<ScheduledExecutors.Signal>()
              {
                private final MasterRunnable theRunnable = masterRunnable.lhs;

                @Override
                public ScheduledExecutors.Signal call()
                {
                  if (master) {
                    theRunnable.run();
                  }
                  if (master) { // (We might no longer be master)
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
        log.makeAlert(e, "Unable to become master")
           .emit();
        final LeaderLatch oldLatch = createNewLeaderLatch();
        Closeables.closeQuietly(oldLatch);
        try {
          leaderLatch.get().start();
        }
        catch (Exception e1) {
          // If an exception gets thrown out here, then the master will zombie out 'cause it won't be looking for
          // the latch anymore.  I don't believe it's actually possible for an Exception to throw out here, but
          // Curator likes to have "throws Exception" on methods so it might happen...
          log.makeAlert(e1, "I am a zombie")
             .emit();
        }
      }
    }
  }

  private void stopBeingMaster()
  {
    synchronized (lock) {
      try {
        log.info("I am no longer the master...");

        for (String server : loadManagementPeons.keySet()) {
          LoadQueuePeon peon = loadManagementPeons.remove(server);
          peon.stop();
        }
        loadManagementPeons.clear();

        databaseSegmentManager.stop();
        serverInventoryView.stop();
        master = false;
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to stopBeingMaster").emit();
      }
    }
  }

  private List<DruidMasterHelper> makeIndexingServiceHelpers(final AtomicReference<MergerWhitelist> whitelistRef)
  {
    List<DruidMasterHelper> helpers = Lists.newArrayList();

    helpers.add(new DruidMasterSegmentInfoLoader(DruidMaster.this));

    if (config.isConvertSegments()) {
      helpers.add(new DruidMasterVersionConverter(indexingServiceClient, whitelistRef));
    }
    if (config.isMergeSegments()) {
      helpers.add(new DruidMasterSegmentMerger(indexingServiceClient, whitelistRef));
      helpers.add(
          new DruidMasterHelper()
          {
            @Override
            public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
            {
              MasterStats stats = params.getMasterStats();
              log.info("Issued merge requests for %s segments", stats.getGlobalStats().get("mergedCount").get());

              params.getEmitter().emit(
                  new ServiceMetricEvent.Builder().build(
                      "master/merge/count", stats.getGlobalStats().get("mergedCount")
                  )
              );

              return params;
            }
          }
      );
    }

    return ImmutableList.copyOf(helpers);
  }

  public static class DruidMasterVersionConverter implements DruidMasterHelper
  {
    private final IndexingServiceClient indexingServiceClient;
    private final AtomicReference<MergerWhitelist> whitelistRef;

    public DruidMasterVersionConverter(
        IndexingServiceClient indexingServiceClient,
        AtomicReference<MergerWhitelist> whitelistRef
    )
    {
      this.indexingServiceClient = indexingServiceClient;
      this.whitelistRef = whitelistRef;
    }

    @Override
    public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
    {
      MergerWhitelist whitelist = whitelistRef.get();

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

  public abstract class MasterRunnable implements Runnable
  {
    private final long startTime = System.currentTimeMillis();
    private final List<DruidMasterHelper> helpers;

    protected MasterRunnable(List<DruidMasterHelper> helpers)
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
            log.info("LEGGO MY EGGO. [%s] is master.", latch == null ? null : latch.getLeader().getId());
            stopBeingMaster();
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
            stopBeingMaster();
            return;
          }
        }

        // Do master stuff.
        DruidMasterRuntimeParams params =
            DruidMasterRuntimeParams.newBuilder()
                                    .withStartTime(startTime)
                                    .withDatasources(databaseSegmentManager.getInventory())
                                    .withMasterSegmentSettings(segmentSettingsAtomicReference.get())
                                    .withEmitter(emitter)
                                    .build();


        for (DruidMasterHelper helper : helpers) {
          params = helper.run(params);
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
      }
    }
  }

  private class MasterComputeManagerRunnable extends MasterRunnable
  {
    private MasterComputeManagerRunnable()
    {
      super(
          ImmutableList.of(
              new DruidMasterSegmentInfoLoader(DruidMaster.this),
              new DruidMasterHelper()
              {
                @Override
                public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
                {
                  // Display info about all historical servers
                  Iterable<DruidServer> servers = FunctionalIterable
                      .create(serverInventoryView.getInventory())
                      .filter(
                          new Predicate<DruidServer>()
                          {
                            @Override
                            public boolean apply(
                                @Nullable DruidServer input
                            )
                            {
                              return input.getType().equalsIgnoreCase("historical");
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

                  SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(cluster);

                  // Stop peons for servers that aren't there anymore.
                  for (String name : Sets.difference(
                      Sets.newHashSet(
                          Iterables.transform(
                              servers,
                              new Function<DruidServer, String>()
                              {
                                @Override
                                public String apply(@Nullable DruidServer input)
                                {
                                  return input.getName();
                                }
                              }
                          )
                      ), loadManagementPeons.keySet()
                  )) {
                    log.info("Removing listener for server[%s] which is no longer there.", name);
                    LoadQueuePeon peon = loadManagementPeons.remove(name);
                    peon.stop();
                  }

                  decrementRemovedSegmentsLifetime();

                  return params.buildFromExisting()
                               .withDruidCluster(cluster)
                               .withDatabaseRuleManager(databaseRuleManager)
                               .withLoadManagementPeons(loadManagementPeons)
                               .withSegmentReplicantLookup(segmentReplicantLookup)
                               .withBalancerReferenceTimestamp(DateTime.now())
                               .withMasterSegmentSettings(
                                   segmentSettingsAtomicReference.get()
                               )
                               .build();
                }
              },
              new DruidMasterRuleRunner(
                  DruidMaster.this,
                  config.getReplicantLifetime(),
                  config.getReplicantThrottleLimit()
              ),
              new DruidMasterCleanup(DruidMaster.this),
              new DruidMasterBalancer(DruidMaster.this),
              new DruidMasterLogger()
          )
      );
    }
  }

  private class MasterIndexingServiceRunnable extends MasterRunnable
  {
    private MasterIndexingServiceRunnable(List<DruidMasterHelper> helpers)
    {
      super(helpers);
    }
  }
}

