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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.sql.calcite.schema.SystemSchema.DerivedSegmentStatus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Duration;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Broker-side per-datasource rollup of segment stats, used to transparently accelerate the web
 * console's unfiltered {@code SELECT ... FROM sys.segments GROUP BY datasource} query (see
 * {@code SegmentsRollupRule}) without materializing every segment per request.
 *
 * <p>The rollup is derived from the SAME sources {@code sys.segments} reads - the published snapshot
 * held by {@link MetadataSegmentView} and the available-segment view in
 * {@link BrokerSegmentMetadataCache} - folded through the shared {@link DerivedSegmentStatus} so it
 * can never disagree with the row-level table on how a segment is classified. It is recomputed on a
 * fixed poll (same period as the metadata segment cache), so it is consistent with {@code sys.segments}
 * as of the last poll - the same "eventually consistent" contract the published side already has.
 */
@ManageLifecycle
public class SegmentsRollup
{
  private static final EmittingLogger log = new EmittingLogger(SegmentsRollup.class);
  private static final String SYNC_TIME_METRIC = "segment/rollup/sync/time";
  private static final String QUERY_COUNT_METRIC = "segment/rollup/query/count";

  private final MetadataSegmentView metadataView;
  private final BrokerSegmentMetadataCache availableSegmentCache;
  private final ServiceEmitter emitter;
  private final long pollPeriodInMs;
  private final boolean enabled;

  private final ScheduledExecutorService scheduledExec;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  /**
   * Sorted by datasource so the accelerated query yields rows in the same order the
   * {@code ORDER BY datasource} console query expects. Volatile: reassigned in {@link #poll()} and
   * read from query threads in {@link #getStatsIfReady()}. Null until the first poll completes (and on
   * processes where the poll is not lifecycle-started), which makes the accelerating rule fall back to
   * the normal scan.
   */
  @MonotonicNonNull
  private volatile ImmutableSortedMap<String, DatasourceSegmentStats> stats = null;

  @Inject
  public SegmentsRollup(
      final MetadataSegmentView metadataView,
      final BrokerSegmentMetadataCache availableSegmentCache,
      final BrokerSegmentMetadataCacheConfig config,
      final ServiceEmitter emitter
  )
  {
    this.metadataView = metadataView;
    this.availableSegmentCache = availableSegmentCache;
    this.emitter = emitter;
    this.pollPeriodInMs = config.getMetadataSegmentPollPeriod();
    this.enabled = config.isSegmentsRollupEnabled();
    this.scheduledExec = Execs.scheduledSingleThreaded("SegmentsRollup-Cache--%d");
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      if (enabled) {
        ScheduledExecutors.scheduleAtFixedRate(
            scheduledExec,
            Duration.millis(Math.min(pollPeriodInMs, 500L)),
            Duration.millis(pollPeriodInMs),
            this::poll
        );
        log.info("SegmentsRollup is started.");
      } else {
        log.info("SegmentsRollup is disabled (druid.sql.planner.segmentsRollupEnabled=false).");
      }
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }
    log.info("SegmentsRollup is stopping.");
    scheduledExec.shutdown();
    stats = null;
    log.info("SegmentsRollup is stopped.");
  }

  /**
   * The current per-datasource rollup sorted by datasource, or {@code null} if the first poll has not
   * completed (or the poll was never lifecycle-started on this process). Callers must treat {@code null}
   * as "not available" and fall back to scanning {@code sys.segments}.
   */
  @Nullable
  public ImmutableSortedMap<String, DatasourceSegmentStats> getStatsIfReady()
  {
    return stats;
  }

  /**
   * Records that a {@code GROUP BY datasource} query over sys.segments was served from this rollup
   * rather than a full segment scan, so operators can see how often the console query is accelerated.
   * Called by the rollup rel when it produces results.
   */
  public void emitAcceleratedQuery()
  {
    emitter.emit(ServiceMetricEvent.builder().setMetric(QUERY_COUNT_METRIC, 1));
  }

  @VisibleForTesting
  void poll()
  {
    log.debug("Recomputing per-datasource segment rollup.");
    final Stopwatch syncTime = Stopwatch.createStarted();

    final Map<String, DatasourceSegmentStats.Accumulator> acc = new HashMap<>();

    // Published (and, with centralized schema, realtime) segments, joined with available metadata.
    final Set<SegmentId> segmentsSeen = new HashSet<>();
    final Iterator<SegmentStatusInCluster> published = metadataView.getSegments(null);
    while (published.hasNext()) {
      final SegmentStatusInCluster val = published.next();
      final DataSegment segment = val.getDataSegment();
      final AvailableSegmentMetadata available =
          availableSegmentCache.getAvailableSegmentMetadata(segment.getDataSource(), segment.getId());
      segmentsSeen.add(segment.getId());
      acc.computeIfAbsent(segment.getDataSource(), ds -> new DatasourceSegmentStats.Accumulator())
         .add(DerivedSegmentStatus.forPublished(val, available), segment.getSize());
    }

    // Available-only segments not present in the published set.
    final Iterator<AvailableSegmentMetadata> availableOnly = availableSegmentCache.iterateSegmentMetadata(null);
    while (availableOnly.hasNext()) {
      final AvailableSegmentMetadata val = availableOnly.next();
      final DataSegment segment = val.getSegment();
      if (segmentsSeen.contains(segment.getId())) {
        continue;
      }
      acc.computeIfAbsent(segment.getDataSource(), ds -> new DatasourceSegmentStats.Accumulator())
         .add(DerivedSegmentStatus.forAvailable(val), segment.getSize());
    }

    final ImmutableSortedMap.Builder<String, DatasourceSegmentStats> builder = ImmutableSortedMap.naturalOrder();
    for (Map.Entry<String, DatasourceSegmentStats.Accumulator> e : acc.entrySet()) {
      builder.put(e.getKey(), e.getValue().build());
    }
    stats = builder.build();

    emitter.emit(ServiceMetricEvent.builder().setMetric(SYNC_TIME_METRIC, syncTime.millisElapsed()));
  }
}
