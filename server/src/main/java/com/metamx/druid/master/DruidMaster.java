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
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.Comparators;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.ServerInventoryManager;
import com.metamx.druid.coordination.DruidClusterInfo;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;
import com.netflix.curator.x.discovery.ServiceProvider;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

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
  private final DruidClusterInfo clusterInfo;
  private final DatabaseSegmentManager databaseSegmentManager;
  private final ServerInventoryManager serverInventoryManager;
  private final DatabaseRuleManager databaseRuleManager;
  private final PhoneBook yp;
  private final ServiceEmitter emitter;
  private final ScheduledExecutorService exec;
  private final ScheduledExecutorService peonExec;
  private final PhoneBookPeon masterPeon;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceProvider serviceProvider;

  private final ObjectMapper jsonMapper;

  public DruidMaster(
      DruidMasterConfig config,
      DruidClusterInfo clusterInfo,
      ObjectMapper jsonMapper,
      DatabaseSegmentManager databaseSegmentManager,
      ServerInventoryManager serverInventoryManager,
      DatabaseRuleManager databaseRuleManager,
      PhoneBook zkPhoneBook,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ServiceProvider serviceProvider
  )
  {
    this.config = config;
    this.clusterInfo = clusterInfo;

    this.jsonMapper = jsonMapper;

    this.databaseSegmentManager = databaseSegmentManager;
    this.serverInventoryManager = serverInventoryManager;
    this.databaseRuleManager = databaseRuleManager;
    this.yp = zkPhoneBook;
    this.emitter = emitter;

    this.masterPeon = new MasterListeningPeon();
    this.exec = scheduledExecutorFactory.create(1, "Master-Exec--%d");
    this.peonExec = scheduledExecutorFactory.create(1, "Master-PeonExec--%d");

    this.loadManagementPeons = loadManagementPeons;

    this.serviceProvider = serviceProvider;
  }

  public boolean isClusterMaster()
  {
    return master;
  }

  public Map<String, Double> getLoadStatus()
  {
    Map<String, Integer> availableSegmentMap = Maps.newHashMap();

    for (DataSegment segment : getAvailableDataSegments()) {
      Integer count = availableSegmentMap.get(segment.getDataSource());
      int newCount = (count == null) ? 0 : count.intValue();
      availableSegmentMap.put(segment.getDataSource(), ++newCount);
    }

    Map<String, Set<DataSegment>> loadedDataSources = Maps.newHashMap();
    for (DruidServer server : serverInventoryManager.getInventory()) {
      for (DruidDataSource dataSource : server.getDataSources()) {
        if (!loadedDataSources.containsKey(dataSource.getName())) {
          TreeSet<DataSegment> setToAdd = Sets.newTreeSet(DataSegment.bucketMonthComparator());
          setToAdd.addAll(dataSource.getSegments());
          loadedDataSources.put(dataSource.getName(), setToAdd);
        } else {
          loadedDataSources.get(dataSource.getName()).addAll(dataSource.getSegments());
        }
      }
    }

    Map<String, Integer> loadedSegmentMap = Maps.newHashMap();
    for (Map.Entry<String, Set<DataSegment>> entry : loadedDataSources.entrySet()) {
      loadedSegmentMap.put(entry.getKey(), entry.getValue().size());
    }

    Map<String, Double> retVal = Maps.newHashMap();

    for (Map.Entry<String, Integer> entry : availableSegmentMap.entrySet()) {
      String key = entry.getKey();
      if (!loadedSegmentMap.containsKey(key) || entry.getValue().doubleValue() == 0.0) {
        retVal.put(key, 0.0);
      } else {
        retVal.put(key, 100 * loadedSegmentMap.get(key).doubleValue() / entry.getValue().doubleValue());
      }
    }

    return retVal;
  }

  public int lookupSegmentLifetime(DataSegment segment)
  {
    return serverInventoryManager.lookupSegmentLifetime(segment);
  }

  public void decrementRemovedSegmentsLifetime()
  {
    serverInventoryManager.decrementRemovedSegmentsLifetime();
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

  public void moveSegment(String from, String to, String segmentName, final LoadPeonCallback callback)
  {
    final DruidServer fromServer = serverInventoryManager.getInventoryValue(from);
    if (fromServer == null) {
      throw new IllegalArgumentException(String.format("Unable to find server [%s]", from));
    }

    final DruidServer toServer = serverInventoryManager.getInventoryValue(to);
    if (toServer == null) {
      throw new IllegalArgumentException(String.format("Unable to find server [%s]", to));
    }

    if (to.equalsIgnoreCase(from)) {
      throw new IllegalArgumentException(
          String.format("Redundant command to move segment [%s] from [%s] to [%s]", segmentName, from, to)
      );
    }

    final DataSegment segment = fromServer.getSegment(segmentName);
    if (segment == null) {
      throw new IllegalArgumentException(
          String.format("Unable to find segment [%s] on server [%s]", segmentName, from)
      );
    }

    final LoadQueuePeon loadPeon = loadManagementPeons.get(to);
    if (loadPeon == null) {
      throw new IllegalArgumentException(String.format("LoadQueuePeon hasn't been created yet for path [%s]", to));
    }

    final LoadQueuePeon dropPeon = loadManagementPeons.get(from);
    if (dropPeon == null) {
      throw new IllegalArgumentException(String.format("LoadQueuePeon hasn't been created yet for path [%s]", from));
    }

    final ServerHolder toHolder = new ServerHolder(toServer, loadPeon);
    if (toHolder.getAvailableSize() < segment.getSize()) {
      throw new IllegalArgumentException(
          String.format(
              "Not enough capacity on server [%s] for segment [%s]. Required: %,d, available: %,d.",
              to,
              segment,
              segment.getSize(),
              toHolder.getAvailableSize()
          )
      );
    }

    final String toLoadQueueSegPath = yp.combineParts(Arrays.asList(config.getLoadQueuePath(), to, segmentName));
    final String toServedSegPath = yp.combineParts(Arrays.asList(config.getServedSegmentsLocation(), to, segmentName));

    loadPeon.loadSegment(
        segment,
        new LoadPeonCallback()
        {
          @Override
          protected void execute()
          {
            if ((yp.lookup(toServedSegPath, Object.class) != null) &&
                yp.lookup(toLoadQueueSegPath, Object.class) == null &&
                !dropPeon.getSegmentsToDrop().contains(segment)) {
              dropPeon.dropSegment(segment, callback);
            } else if (callback != null) {
              callback.execute();
            }
          }
        }
    );
  }

  public void cloneSegment(String from, String to, String segmentName, LoadPeonCallback callback)
  {
    final DruidServer fromServer = serverInventoryManager.getInventoryValue(from);
    if (fromServer == null) {
      throw new IllegalArgumentException(String.format("Unable to find server [%s]", from));
    }

    final DruidServer toServer = serverInventoryManager.getInventoryValue(to);
    if (toServer == null) {
      throw new IllegalArgumentException(String.format("Unable to find server [%s]", to));
    }

    final DataSegment segment = fromServer.getSegment(segmentName);
    if (segment == null) {
      throw new IllegalArgumentException(
          String.format("Unable to find segment [%s] on server [%s]", segmentName, from)
      );
    }

    final LoadQueuePeon loadPeon = loadManagementPeons.get(to);
    if (loadPeon == null) {
      throw new IllegalArgumentException(String.format("LoadQueuePeon hasn't been created yet for path [%s]", to));
    }

    final ServerHolder toHolder = new ServerHolder(toServer, loadPeon);
    if (toHolder.getAvailableSize() < segment.getSize()) {
      throw new IllegalArgumentException(
          String.format(
              "Not enough capacity on server [%s] for segment [%s]. Required: %,d, available: %,d.",
              to,
              segment,
              segment.getSize(),
              toHolder.getAvailableSize()
          )
      );
    }

    if (!loadPeon.getSegmentsToLoad().contains(segment)) {
      loadPeon.loadSegment(segment, callback);
    }
  }

  public void dropSegment(String from, String segmentName, final LoadPeonCallback callback)
  {
    final DruidServer fromServer = serverInventoryManager.getInventoryValue(from);
    if (fromServer == null) {
      throw new IllegalArgumentException(String.format("Unable to find server [%s]", from));
    }

    final DataSegment segment = fromServer.getSegment(segmentName);
    if (segment == null) {
      throw new IllegalArgumentException(
          String.format("Unable to find segment [%s] on server [%s]", segmentName, from)
      );
    }

    final LoadQueuePeon dropPeon = loadManagementPeons.get(from);
    if (dropPeon == null) {
      throw new IllegalArgumentException(String.format("LoadQueuePeon hasn't been created yet for path [%s]", from));
    }

    if (!dropPeon.getSegmentsToDrop().contains(segment)) {
      dropPeon.dropSegment(segment, callback);
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
        log.warn("No size on Segment[%s], wtf?", dataSegment);
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

      if (!yp.isStarted()) {
        throw new ISE("Master cannot perform without his yellow pages.");
      }

      becomeMaster();
      yp.registerListener(config.getBasePath(), masterPeon);
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      stopBeingMaster();
      yp.unregisterListener(config.getBasePath(), masterPeon);

      started = false;

      exec.shutdownNow();
      peonExec.shutdownNow();
    }
  }

  private void becomeMaster()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      boolean becameMaster = true;
      try {
        yp.announce(
            config.getBasePath(),
            MASTER_OWNER_NODE,
            ImmutableMap.of(
                "host", config.getHost()
            )
        );
      }
      catch (ZkNodeExistsException e) {
        log.info("Got ZkNodeExistsException, not becoming master.");
        becameMaster = false;
      }

      if (becameMaster) {
        log.info("I am the master, all must bow!");
        master = true;
        databaseSegmentManager.start();
        databaseRuleManager.start();
        serverInventoryManager.start();

        final List<Pair<? extends MasterRunnable, Duration>> masterRunnables = Lists.newArrayList();

        masterRunnables.add(Pair.of(new MasterComputeManagerRunnable(), config.getMasterPeriod()));
        if (config.isMergeSegments() && serviceProvider != null) {
          masterRunnables.add(Pair.of(new MasterSegmentMergerRunnable(), config.getMasterSegmentMergerPeriod()));
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
      } else {
        log.info(
            "FAILED to become master!!11!12  Wtfpwned by [%s]",
            clusterInfo.lookupCurrentLeader()
        );
      }
    }
  }

  private void stopBeingMaster()
  {
    synchronized (lock) {
      log.debug("I am %s the master", master ? "DEFINITELY" : "NOT");
      if (master) {
        log.info("I am no longer the master...");

        for (String server : loadManagementPeons.keySet()) {
          LoadQueuePeon peon = loadManagementPeons.remove(server);
          peon.stop();
          yp.unregisterListener(
              yp.combineParts(Arrays.asList(config.getLoadQueuePath(), server)),
              peon
          );
        }
        loadManagementPeons.clear();

        yp.unannounce(config.getBasePath(), MASTER_OWNER_NODE);

        databaseSegmentManager.stop();
        serverInventoryManager.stop();
        master = false;
      }
    }
  }

  private class MasterListeningPeon implements PhoneBookPeon<Map>
  {
    @Override
    public Class<Map> getObjectClazz()
    {
      return Map.class;
    }

    @Override
    public void newEntry(String name, Map properties)
    {
      if (MASTER_OWNER_NODE.equals(name)) {
        if (config.getHost().equals(properties.get("host"))) {
          log.info("I really am the master!");
        } else {
          log.info("[%s] is the real master...", properties);
        }
      }
    }

    @Override
    public void entryRemoved(String name)
    {
      if (MASTER_OWNER_NODE.equals(name)) {
        becomeMaster();
      }
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
          Map<String, String> currLeader = clusterInfo.lookupCurrentLeader();
          if (currLeader == null || !config.getHost().equals(currLeader.get("host"))) {
            log.info("I thought I was the master, but really [%s] is.  Phooey.", currLeader);
            stopBeingMaster();
            return;
          }
        }

        List<Boolean> allStarted = Arrays.asList(
            databaseSegmentManager.isStarted(),
            serverInventoryManager.isStarted()
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
                                    .withLoadManagementPeons(loadManagementPeons)
                                    .withMillisToWaitBeforeDeleting(config.getMillisToWaitBeforeDeleting())
                                    .withEmitter(emitter)
                                    .withMergeBytesLimit(config.getMergeBytesLimit())
                                    .withMergeSegmentsLimit(config.getMergeSegmentsLimit())
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
                  Iterable<DruidServer> servers =
                      FunctionalIterable.create(serverInventoryManager.getInventory())
                                        .filter(
                                            new Predicate<DruidServer>()
                                            {
                                              @Override
                                              public boolean apply(
                                                  @Nullable DruidServer input
                                              )
                                              {
                                                return input.getType()
                                                            .equalsIgnoreCase("historical");
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
                      String basePath = yp.combineParts(Arrays.asList(config.getLoadQueuePath(), server.getName()));
                      LoadQueuePeon loadQueuePeon = new LoadQueuePeon(yp, basePath, peonExec);
                      log.info("Creating LoadQueuePeon for server[%s] at path[%s]", server.getName(), basePath);

                      loadManagementPeons.put(
                          server.getName(),
                          loadQueuePeon
                      );
                      yp.registerListener(basePath, loadQueuePeon);
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

                    yp.unregisterListener(yp.combineParts(Arrays.asList(config.getLoadQueuePath(), name)), peon);
                  }

                  decrementRemovedSegmentsLifetime();

                  return params.buildFromExisting()
                               .withDruidCluster(cluster)
                               .withDatabaseRuleManager(databaseRuleManager)
                               .withSegmentReplicantLookup(segmentReplicantLookup)
                               .build();
                }
              },
              new DruidMasterRuleRunner(DruidMaster.this),
              new DruidMasterCleanup(DruidMaster.this),
              new DruidMasterBalancer(DruidMaster.this, new BalancerAnalyzer()),
              new DruidMasterLogger()
          )
      );
    }
  }

  private class MasterSegmentMergerRunnable extends MasterRunnable
  {
    private MasterSegmentMergerRunnable()
    {
      super(
          ImmutableList.of(
              new DruidMasterSegmentInfoLoader(DruidMaster.this),
              new DruidMasterSegmentMerger(jsonMapper, serviceProvider),
              new DruidMasterHelper()
              {
                @Override
                public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
                {
                  MasterStats stats = params.getMasterStats();
                  log.info("Issued merge requests for %s segments", stats.getGlobalStats().get("mergedCount").get());

                  params.getEmitter().emit(
                      new ServiceMetricEvent.Builder().build(
                          "master/merge/count",
                          stats.getGlobalStats().get("mergedCount")
                      )
                  );

                  return params;
                }
              }
          )
      );
    }
  }
}

