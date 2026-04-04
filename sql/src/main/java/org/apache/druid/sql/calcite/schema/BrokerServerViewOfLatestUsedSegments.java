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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * A {@link TimelineServerView} that exposes a superset of {@link BrokerServerView}:
 * segments available on servers plus used segments that exist only in metadata.
 * <p>
 * This class maintains a merged per-datasource timeline updated incrementally from
 * {@link BrokerServerView} and {@link MetadataSegmentView} callbacks.
 */
@ManageLifecycle
public class BrokerServerViewOfLatestUsedSegments implements TimelineServerView
{
  private static final Logger log = new Logger(BrokerServerViewOfLatestUsedSegments.class);

  private final BrokerServerView brokerServerView;
  private final boolean isCacheEnabled;
  private final TierSelectorStrategy historicalTierSelectorStrategy;
  private final TierSelectorStrategy realtimeTierSelectorStrategy;
  private final BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig;

  // LOCK ORDERING: BsvCallback executes on the BrokerServerView callback thread, which holds
  // BrokerServerView.lock (via directExecutor). BsvCallback then acquires this.lock.
  // Therefore, this.lock must NEVER be held when calling into BrokerServerView (e.g.getBsvSelector),
  // as that would invert the order and risk deadlock.
  private final Object lock = new Object();
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> mergedTimelines = new HashMap<>();
  private final Map<SegmentId, ServerSelector> mergedSelectors = new HashMap<>();
  private final Map<SegmentId, DataSegment> metadataSegments = new HashMap<>();
  private final Set<SegmentId> metadataRemovedSegmentIds = new HashSet<>();

  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();
  private final CountDownLatch brokerViewInitLatch = new CountDownLatch(1);
  private final CountDownLatch metadataViewInitLatch = new CountDownLatch(1);
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  @Inject
  public BrokerServerViewOfLatestUsedSegments(
      final BrokerServerView brokerServerView,
      final MetadataSegmentView metadataSegmentView,
      final BrokerSegmentMetadataCacheConfig cacheConfig,
      final TierSelectorStrategy historicalTierSelectorStrategy,
      @Named(BrokerServerView.REALTIME_SELECTOR) final TierSelectorStrategy realtimeTierSelectorStrategy,
      final BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig
  )
  {
    this.brokerServerView = brokerServerView;
    this.isCacheEnabled = cacheConfig.isMetadataSegmentCacheEnable();
    this.historicalTierSelectorStrategy = historicalTierSelectorStrategy;
    this.realtimeTierSelectorStrategy = realtimeTierSelectorStrategy;
    this.brokerViewOfCoordinatorConfig = brokerViewOfCoordinatorConfig;

    brokerServerView.registerTimelineCallback(Execs.directExecutor(), new BsvCallback());
    metadataSegmentView.registerSegmentViewCallback(Execs.directExecutor(), new MsvCallback());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends TimelineLookup<String, ServerSelector>> Optional<T> getTimeline(final TableDataSource dataSource)
  {
    ensureCacheEnabled();

    final VersionedIntervalTimeline<String, ServerSelector> mergedTimeline;
    synchronized (lock) {
      mergedTimeline = mergedTimelines.get(dataSource.getName());
      if (mergedTimeline == null || mergedTimeline.isEmpty()) {
        return Optional.empty();
      }
    }

    return Optional.of((T) mergedTimeline);
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    ensureCacheEnabled();
    return brokerServerView.getDruidServers();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(final DruidServer server)
  {
    ensureCacheEnabled();
    return brokerServerView.getQueryRunner(server);
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    ensureCacheEnabled();
    timelineCallbacks.put(callback, exec);
  }

  @Override
  public void registerServerCallback(final Executor exec, final ServerCallback callback)
  {
    ensureCacheEnabled();
    brokerServerView.registerServerCallback(exec, callback);
  }

  @Override
  public void registerSegmentCallback(final Executor exec, final SegmentCallback callback)
  {
    ensureCacheEnabled();
    brokerServerView.registerSegmentCallback(exec, callback);
  }

  private class BsvCallback implements TimelineCallback
  {
    @Override
    public ServerView.CallbackAction timelineInitialized()
    {
      brokerViewInitLatch.countDown();
      maybeFireInitialized();
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentAdded(final DruidServerMetadata server, final DataSegment segment)
    {
      final ServerSelector selector = getBsvSelector(segment);
      final boolean visible;

      synchronized (lock) {
        visible = selector != null && !metadataRemovedSegmentIds.contains(segment.getId());
        if (visible) {
          upsertMergedSelector(selector);
        }
      }

      if (visible) {
        runTimelineCallbacks(cb -> cb.segmentAdded(server, segment));
      }
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentRemoved(final DataSegment segment)
    {
      final boolean shouldRemainVisibleAsUnavailable;
      final boolean shouldFireRemoved;

      synchronized (lock) {
        final SegmentId segmentId = segment.getId();
        final boolean wasMetadataRemoved = metadataRemovedSegmentIds.contains(segmentId);
        shouldRemainVisibleAsUnavailable = metadataSegments.containsKey(segmentId) && !wasMetadataRemoved;
        if (shouldRemainVisibleAsUnavailable) {
          upsertMergedSelector(createEmptySelector(segment));
        } else {
          removeMergedSelector(segmentId);
          // Both BSV and MSV now agree the segment is gone - clear the guard entry
          metadataRemovedSegmentIds.remove(segmentId);
        }
        shouldFireRemoved = !shouldRemainVisibleAsUnavailable && !wasMetadataRemoved;
      }

      if (shouldFireRemoved) {
        runTimelineCallbacks(cb -> cb.segmentRemoved(segment));
      }
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction serverSegmentRemoved(final DruidServerMetadata server, final DataSegment segment)
    {
      runTimelineCallbacks(cb -> cb.serverSegmentRemoved(server, segment));
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentSchemasAnnounced(final SegmentSchemas segmentSchemas)
    {
      runTimelineCallbacks(cb -> cb.segmentSchemasAnnounced(segmentSchemas));
      return ServerView.CallbackAction.CONTINUE;
    }
  }

  private class MsvCallback implements MetadataSegmentViewCallback
  {
    @Override
    public void timelineInitialized()
    {
      metadataViewInitLatch.countDown();
      maybeFireInitialized();
    }

    @Override
    public void segmentsAdded(final Collection<DataSegment> segments)
    {
      // Pre-compute BSV selectors outside lock to respect lock ordering.
      final Map<SegmentId, ServerSelector> bsvSelectors = new HashMap<>();
      for (DataSegment segment : segments) {
        bsvSelectors.put(segment.getId(), getBsvSelector(segment));
      }

      synchronized (lock) {
        for (DataSegment segment : segments) {
          final SegmentId segmentId = segment.getId();
          final ServerSelector bsvSelector = bsvSelectors.get(segmentId);

          metadataSegments.put(segmentId, segment);
          metadataRemovedSegmentIds.remove(segmentId);
          if (bsvSelector != null) {
            if (mergedSelectors.get(segmentId) != bsvSelector) {
              upsertMergedSelector(bsvSelector);
            }
          } else if (!mergedSelectors.containsKey(segmentId)) {
            upsertMergedSelector(createEmptySelector(segment));
          }
        }
      }
    }

    @Override
    public void segmentsRemoved(final Collection<SegmentId> segmentIds)
    {
      final List<DataSegment> toFireRemoved = new ArrayList<>();

      synchronized (lock) {
        for (SegmentId segmentId : segmentIds) {
          final DataSegment metadataSegment = metadataSegments.remove(segmentId);
          metadataRemovedSegmentIds.add(segmentId);

          final ServerSelector existingSelector = mergedSelectors.get(segmentId);
          if (existingSelector != null) {
            toFireRemoved.add(existingSelector.getSegment());
            removeMergedSelector(segmentId);
          }
        }
      }

      for (DataSegment segment : toFireRemoved) {
        runTimelineCallbacks(cb -> cb.segmentRemoved(segment));
      }
    }
  }

  private void ensureCacheEnabled()
  {
    if (!isCacheEnabled) {
      throw new ISE("metadataSegmentCacheEnable must be true to use BrokerServerViewOfLatestUsedSegments");
    }
  }

  private ServerSelector getBsvSelector(final DataSegment segment)
  {
    final PartitionChunk<ServerSelector> chunk = brokerServerView
        .getTimeline(TableDataSource.create(segment.getDataSource()))
        .map(t -> t.findChunk(segment.getInterval(), segment.getVersion(), segment.getShardSpec().getPartitionNum()))
        .orElse(null);
    return chunk == null ? null : chunk.getObject();
  }

  private void upsertMergedSelector(final ServerSelector selector)
  {
    final DataSegment segment = selector.getSegment();
    final ServerSelector oldSelector = mergedSelectors.get(segment.getId());
    final VersionedIntervalTimeline<String, ServerSelector> timeline = getOrCreateMergedTimeline(segment.getDataSource());

    if (oldSelector != null) {
      timeline.remove(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(oldSelector));
    }

    timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
    mergedSelectors.put(segment.getId(), selector);
  }

  private void removeMergedSelector(final SegmentId segmentId)
  {
    final ServerSelector selector = mergedSelectors.remove(segmentId);
    if (selector == null) {
      return;
    }

    final DataSegment segment = selector.getSegment();
    final VersionedIntervalTimeline<String, ServerSelector> timeline = mergedTimelines.get(segment.getDataSource());
    if (timeline != null) {
      timeline.remove(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
      if (timeline.isEmpty()) {
        mergedTimelines.remove(segment.getDataSource());
      }
    }
  }

  private ServerSelector createEmptySelector(final DataSegment segment)
  {
    return new ServerSelector(
        segment,
        historicalTierSelectorStrategy,
        realtimeTierSelectorStrategy,
        brokerViewOfCoordinatorConfig
    );
  }

  private VersionedIntervalTimeline<String, ServerSelector> getOrCreateMergedTimeline(final String dataSource)
  {
    return mergedTimelines.computeIfAbsent(
        dataSource,
        ds -> new VersionedIntervalTimeline<>(Ordering.natural(), true)
    );
  }

  private void maybeFireInitialized()
  {
    if (brokerViewInitLatch.getCount() == 0
        && metadataViewInitLatch.getCount() == 0
        && initialized.compareAndSet(false, true)) {
      log.info("BrokerServerViewOfLatestUsedSegments is initialized.");
      runTimelineCallbacks(TimelineCallback::timelineInitialized);
    }
  }

  private void runTimelineCallbacks(final Function<TimelineCallback, ServerView.CallbackAction> function)
  {
    for (Map.Entry<TimelineCallback, Executor> entry : timelineCallbacks.entrySet()) {
      entry.getValue().execute(() -> {
        if (ServerView.CallbackAction.UNREGISTER == function.apply(entry.getKey())) {
          timelineCallbacks.remove(entry.getKey());
        }
      });
    }
  }
}
