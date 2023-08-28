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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ServerView of coordinator for the state of segments being loaded in the cluster.
 */
@ManageLifecycle
public class TimelineAwareCoordinatorServerView implements CoordinatorServerViewInterface, TimelineServerView
{
  private static final Logger log = new Logger(TimelineAwareCoordinatorServerView.class);

  private final Object lock = new Object();

  private final Map<SegmentId, SegmentLoadInfo> segmentLoadInfos;
  private final ConcurrentMap<String, QueryableDruidServer> clients = new ConcurrentHashMap<>();
  private final Map<SegmentId, ServerSelector> selectors = new HashMap<>();
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines2 = new HashMap<>();
  private final Map<String, VersionedIntervalTimeline<String, SegmentLoadInfo>> timelines;

  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();

  private final ServerInventoryView baseView;
  private final CoordinatorSegmentWatcherConfig segmentWatcherConfig;

  private final CountDownLatch initialized = new CountDownLatch(1);
  private final ServiceEmitter emitter;

  private final TierSelectorStrategy tierSelectorStrategy;

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;

  @Inject
  public TimelineAwareCoordinatorServerView(
      final QueryToolChestWarehouse warehouse,
      final QueryWatcher queryWatcher,
      final @Smile ObjectMapper smileMapper,
      final @EscalatedClient HttpClient httpClient,
      ServerInventoryView baseView,
      CoordinatorSegmentWatcherConfig segmentWatcherConfig,
      ServiceEmitter emitter,
      TierSelectorStrategy tierSelectorStrategy
  )
  {
    this.baseView = baseView;
    this.segmentWatcherConfig = segmentWatcherConfig;
    this.emitter = emitter;
    this.segmentLoadInfos = new HashMap<>();
    this.timelines = new HashMap<>();
    this.tierSelectorStrategy = tierSelectorStrategy;
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;

    ExecutorService exec = Execs.singleThreaded("CoordinatorServerView-%s");
    baseView.registerSegmentCallback(
        exec,
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            serverAddedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, DataSegment segment)
          {
            serverRemovedSegment2(server, segment);
            serverRemovedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            initialized.countDown();
            runTimelineCallbacks(TimelineCallback::timelineInitialized);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    baseView.registerServerRemovedCallback(
        exec,
        new ServerView.ServerRemovedCallback()
        {
          @Override
          public ServerView.CallbackAction serverRemoved(DruidServer server)
          {
            removeServer(server);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    if (segmentWatcherConfig.isAwaitInitializationOnStart()) {
      final long startMillis = System.currentTimeMillis();
      log.info("%s waiting for initialization.", getClass().getSimpleName());
      initialized.await();
      final long endMillis = System.currentTimeMillis();
      log.info("%s initialized in [%,d] ms.", getClass().getSimpleName(), endMillis - startMillis);
      emitter.emit(ServiceMetricEvent.builder().build(
          "serverview/init/time",
          endMillis - startMillis
      ));
    }
  }

  private void removeServer(DruidServer server)
  {
    for (DataSegment segment : server.iterateAllSegments()) {
      serverAddedSegment2(server.getMetadata(), segment);
      serverRemovedSegment(server.getMetadata(), segment);
    }
  }

  private void serverAddedSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    SegmentId segmentId = segment.getId();
    synchronized (lock) {
      log.debug("Adding segment[%s] for server[%s]", segment, server);

      SegmentLoadInfo segmentLoadInfo = segmentLoadInfos.get(segmentId);
      if (segmentLoadInfo == null) {
        // servers escape the scope of this object so use ConcurrentSet
        segmentLoadInfo = new SegmentLoadInfo(segment);

        VersionedIntervalTimeline<String, SegmentLoadInfo> timeline = timelines.get(segment.getDataSource());
        if (timeline == null) {
          timeline = new VersionedIntervalTimeline<>(Ordering.natural());
          timelines.put(segment.getDataSource(), timeline);
        }

        timeline.add(
            segment.getInterval(),
            segment.getVersion(),
            segment.getShardSpec().createChunk(segmentLoadInfo)
        );
        segmentLoadInfos.put(segmentId, segmentLoadInfo);
      }
      segmentLoadInfo.addServer(server);
    }
    runTimelineCallbacks(callback -> callback.segmentAdded(server, segment));
  }

  private void serverAddedSegment2(final DruidServerMetadata server, final DataSegment segment)
  {
    final SegmentId segmentId = segment.getId();
    synchronized (lock) {
      // in theory we could probably just filter this to ensure we don't put ourselves in here, to make broker tree
      // query topologies, but for now just skip all brokers, so we don't create some sort of wild infinite query
      // loop...
      if (!server.getType().equals(ServerType.BROKER)) {
        log.debug("Adding segment[%s] for server[%s]", segment, server);
        ServerSelector selector = selectors.get(segmentId);
        if (selector == null) {
          selector = new ServerSelector(segment, tierSelectorStrategy);

          VersionedIntervalTimeline<String, ServerSelector> timeline = timelines2.get(segment.getDataSource());
          if (timeline == null) {
            // broker needs to skip tombstones
            timeline = new VersionedIntervalTimeline<>(Ordering.natural(), true);
            timelines2.put(segment.getDataSource(), timeline);
          }

          timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
          selectors.put(segmentId, selector);
        }

        QueryableDruidServer queryableDruidServer = clients.get(server.getName());
        if (queryableDruidServer == null) {
          DruidServer inventoryValue = baseView.getInventoryValue(server.getName());
          if (inventoryValue == null) {
            log.warn(
                "Could not find server[%s] in inventory. Skipping addition of segment[%s].",
                server.getName(),
                segmentId
            );
            return;
          } else {
            queryableDruidServer = addServer(inventoryValue);
          }
        }
        selector.addServerAndUpdateSegment(queryableDruidServer, segment);
      }
      // run the callbacks, even if the segment came from a broker, lets downstream watchers decide what to do with it
      runTimelineCallbacks(callback -> callback.segmentAdded(server, segment));
    }
  }

  private QueryableDruidServer addServer(DruidServer server)
  {
    QueryableDruidServer retVal = new QueryableDruidServer<>(server, makeDirectClient(server));
    QueryableDruidServer exists = clients.put(server.getName(), retVal);
    if (exists != null) {
      log.warn("QueryRunner for server[%s] already exists!? Well it's getting replaced", server);
    }

    return retVal;
  }

  private DirectDruidClient makeDirectClient(DruidServer server)
  {
    return new DirectDruidClient(
        warehouse,
        queryWatcher,
        smileMapper,
        httpClient,
        server.getScheme(),
        server.getHost(),
        emitter
    );
  }

  private void serverRemovedSegment(DruidServerMetadata server, DataSegment segment)
  {
    SegmentId segmentId = segment.getId();

    synchronized (lock) {
      log.debug("Removing segment[%s] from server[%s].", segmentId, server);

      final SegmentLoadInfo segmentLoadInfo = segmentLoadInfos.get(segmentId);
      if (segmentLoadInfo == null) {
        log.warn("Told to remove non-existant segment[%s]", segmentId);
        return;
      }
      if (segmentLoadInfo.removeServer(server)) {
        runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
      }
      if (segmentLoadInfo.isEmpty()) {
        VersionedIntervalTimeline<String, SegmentLoadInfo> timeline = timelines.get(segment.getDataSource());
        segmentLoadInfos.remove(segmentId);

        final PartitionChunk<SegmentLoadInfo> removedPartition = timeline.remove(
            segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(
                new SegmentLoadInfo(
                    segment
                )
            )
        );

        if (removedPartition == null) {
          log.warn(
              "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
              segment.getInterval(),
              segment.getVersion()
          );
        } else {
          runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
        }
      }
    }
  }

  private void serverRemovedSegment2(DruidServerMetadata server, DataSegment segment)
  {
    final SegmentId segmentId = segment.getId();
    final ServerSelector selector;

    synchronized (lock) {
      log.debug("Removing segment[%s] from server[%s].", segmentId, server);

      // we don't store broker segments here, but still run the callbacks for the segment being removed from the server
      // since the broker segments are not stored on the timeline, do not fire segmentRemoved event
      selector = selectors.get(segmentId);
      if (selector == null) {
        log.warn("Told to remove non-existant segment[%s]", segmentId);
        return;
      }

      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        log.warn(
            "Could not find server[%s] in inventory. Skipping removal of segment[%s].",
            server.getName(),
            segmentId
        );
      } else if (!selector.removeServer(queryableDruidServer)) {
        log.warn(
            "Asked to disassociate non-existant association between server[%s] and segment[%s]",
            server,
            segmentId
        );
      } else {
     //   runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
      }

      if (selector.isEmpty()) {
        VersionedIntervalTimeline<String, ServerSelector> timeline = timelines2.get(segment.getDataSource());
        selectors.remove(segmentId);

        final PartitionChunk<ServerSelector> removedPartition = timeline.remove(
            segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector)
        );

        if (removedPartition == null) {
          log.warn(
              "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
              segment.getInterval(),
              segment.getVersion()
          );
        } else {
         // runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
        }
      }
    }
  }

  @Override
  public VersionedIntervalTimeline<String, SegmentLoadInfo> getTimeline(DataSource dataSource)
  {
    String table = Iterables.getOnlyElement(dataSource.getTableNames());
    synchronized (lock) {
      // build a new timeline?
    }
  }

  @Override
  public Map<SegmentId, SegmentLoadInfo> getSegmentLoadInfos()
  {
    // map
  }

  public Set<SegmentId> getLoadedSegmentIds()
  {
    return segmentLoadInfos.keySet();
  }

  @Override
  public DruidServer getInventoryValue(String serverKey)
  {
    return baseView.getInventoryValue(serverKey);
  }

  @Override
  public Collection<DruidServer> getInventory()
  {
    return baseView.getInventory();
  }

  @Override
  public boolean isStarted()
  {
    return baseView.isStarted();
  }

  @Override
  public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
  {
    return baseView.isSegmentLoadedByServer(serverKey, segment);
  }

  private void runTimelineCallbacks(final Function<TimelineCallback, CallbackAction> function)
  {
    for (Map.Entry<TimelineCallback, Executor> entry : timelineCallbacks.entrySet()) {
      entry.getValue().execute(
          () -> {
            if (CallbackAction.UNREGISTER == function.apply(entry.getKey())) {
              timelineCallbacks.remove(entry.getKey());
            }
          }
      );
    }
  }

  @Override
  public void registerTimelineCallback(Executor exec, TimelineCallback callback)
  {
    timelineCallbacks.put(callback, exec);
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    baseView.registerSegmentCallback(exec, callback);
  }

  @Override
  public Optional<? extends TimelineLookup<String, ServerSelector>> getTimeline(DataSourceAnalysis analysis)
  {
    final TableDataSource table =
        analysis.getBaseTableDataSource()
                .orElseThrow(() -> new ISE("Cannot handle base datasource: %s", analysis.getBaseDataSource()));

    synchronized (lock) {
      return Optional.ofNullable(timelines2.get(table.getName()));
    }
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    return clients.values().stream()
                  .map(queryableDruidServer -> queryableDruidServer.getServer().toImmutableDruidServer())
                  .collect(Collectors.toList());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    synchronized (lock) {
      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        log.error("No QueryRunner found for server name[%s].", server.getName());
        return null;
      }
      return queryableDruidServer.getQueryRunner();
    }
  }
}
