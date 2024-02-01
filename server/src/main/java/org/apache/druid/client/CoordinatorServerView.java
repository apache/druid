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

import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.client.TimelineServerView.TimelineCallback;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * ServerView of coordinator for the state of segments being loaded in the cluster.
 */
@ManageLifecycle
public class CoordinatorServerView implements InventoryView
{
  private static final Logger log = new Logger(CoordinatorServerView.class);

  private final Object lock = new Object();
  private final Map<SegmentId, SegmentLoadInfo> segmentLoadInfos;
  private final Map<String, VersionedIntervalTimeline<String, SegmentLoadInfo>> timelines;

  // Map of server and QueryRunner. This is updated when a segment is added/removed.
  // In parallel, it is used by {@link org.apache.druid.segment.metadata.SegmentMetadataQuerySegmentWalker} to run queries.
  private final ConcurrentMap<String, QueryRunner> serverQueryRunners;
  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks;
  private final ServerInventoryView baseView;
  private final CoordinatorSegmentWatcherConfig segmentWatcherConfig;
  private final CountDownLatch initialized = new CountDownLatch(1);
  private final ServiceEmitter emitter;
  @Nullable
  private final DirectDruidClientFactory druidClientFactory;

  @Inject
  public CoordinatorServerView(
      final ServerInventoryView baseView,
      final CoordinatorSegmentWatcherConfig segmentWatcherConfig,
      final ServiceEmitter emitter,
      @Nullable final DirectDruidClientFactory druidClientFactory
  )
  {
    this.baseView = baseView;
    this.segmentWatcherConfig = segmentWatcherConfig;
    this.emitter = emitter;
    this.druidClientFactory = druidClientFactory;
    this.segmentLoadInfos = new HashMap<>();
    this.timelines = new HashMap<>();
    this.serverQueryRunners = new ConcurrentHashMap<>();
    this.timelineCallbacks = new ConcurrentHashMap<>();

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

          @Override
          public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
          {
            runTimelineCallbacks(callback -> callback.segmentSchemasAnnounced(segmentSchemas));
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
      emitter.emit(ServiceMetricEvent.builder().setMetric(
          "serverview/init/time",
          endMillis - startMillis
      ));
    }
  }

  private void removeServer(DruidServer server)
  {
    for (DataSegment segment : server.iterateAllSegments()) {
      serverRemovedSegment(server.getMetadata(), segment);
    }
    // remove QueryRunner for the server
    serverQueryRunners.remove(server.getName());
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

      if (druidClientFactory != null) {
        QueryRunner queryRunner = serverQueryRunners.get(server.getName());
        if (queryRunner == null) {
          DruidServer inventoryValue = baseView.getInventoryValue(server.getName());
          if (inventoryValue == null) {
            log.warn(
                "Could not find server[%s] in inventory. Skipping addition of segment[%s].",
                server.getName(),
                segmentId
            );
            return;
          } else {
            serverQueryRunners.put(server.getName(), druidClientFactory.makeDirectClient(inventoryValue));
          }
        }
      }

      segmentLoadInfo.addServer(server);

      // segment added notification
      runTimelineCallbacks(callback -> callback.segmentAdded(server, segment));
    }
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
        // server segment removed notification
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
          // segment removed notification
          runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
        }
      }
    }
  }

  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    timelineCallbacks.put(callback, exec);
  }

  private void runTimelineCallbacks(final Function<TimelineCallback, ServerView.CallbackAction> function)
  {
    for (Map.Entry<TimelineCallback, Executor> entry : timelineCallbacks.entrySet()) {
      entry.getValue().execute(
          () -> {
            if (ServerView.CallbackAction.UNREGISTER == function.apply(entry.getKey())) {
              timelineCallbacks.remove(entry.getKey());
            }
          }
      );
    }
  }

  public QueryRunner getQueryRunner(String serverName)
  {
    return serverQueryRunners.get(serverName);
  }

  public VersionedIntervalTimeline<String, SegmentLoadInfo> getTimeline(DataSource dataSource)
  {
    String table = Iterables.getOnlyElement(dataSource.getTableNames());
    synchronized (lock) {
      return timelines.get(table);
    }
  }

  public Map<SegmentId, SegmentLoadInfo> getLoadInfoForAllSegments()
  {
    return segmentLoadInfos;
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
}
