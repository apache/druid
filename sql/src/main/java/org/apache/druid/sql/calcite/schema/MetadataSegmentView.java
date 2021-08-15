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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.DataSegmentInterner;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class polls the Coordinator in background to keep the latest published segments.
 * Provides {@link #getPublishedSegmentsIterator()} for others to get segments in metadata store.
 *
 * The difference between this class and {@link SegmentsMetadataManager} is that this class resides
 * in Broker's memory, while {@link SegmentsMetadataManager} resides in Coordinator's memory. In
 * fact, this class polls the data from {@link SegmentsMetadataManager} object in the memory of the
 * currently leading Coordinator via HTTP queries.
 */
@ManageLifecycle
public class MetadataSegmentView
{
  private static final EmittingLogger log = new EmittingLogger(MetadataSegmentView.class);

  /**
   * This order must match to the order of segments returned by
   * {@link DruidSchema#getSortedAvailableSegmentMetadataIterator()}.
   */
  static final Comparator<SegmentsTableRow> SEGMENT_ORDER = Comparator
      .comparing((SegmentsTableRow row) -> row.getSegmentId().getDataSource())
      .thenComparing(
          (r1, r2) -> r2.getSegmentId().getInterval().getStart().compareTo(r1.getSegmentId().getInterval().getStart())
      )
      .thenComparing(SegmentsTableRow::getSegmentId);

  private final DruidLeaderClient coordinatorDruidLeaderClient;
  private final ObjectMapper jsonMapper;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;
  private final ScheduledExecutorService scheduledExec;
  private final long pollPeriodInMS;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CountDownLatch cachePopulated = new CountDownLatch(1);

  private final boolean isCacheEnabled;
  /**
   * A set to cache published segments when {@link #isCacheEnabled} is set.
   * This set should be sorted using {@link #SEGMENT_ORDER} which is the same order used
   * to sort available segments in {@link DruidSchema}, so that the query engine can use
   * the merge sorted algorithm to merge those two sets of segments.
   *
   * Volatile since this reference is reassigned in {@link #poll()} and then read in
   * {@link #getPublishedSegmentsIterator()} and {@link #getSortedPublishedSegments()} from other threads.
   */
  @MonotonicNonNull
  private volatile ImmutableSortedSet<SegmentsTableRow> publishedSegments = null;

  @Inject
  public MetadataSegmentView(
      final @Coordinator DruidLeaderClient druidLeaderClient,
      final ObjectMapper jsonMapper,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      final PlannerConfig plannerConfig
  )
  {
    Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.coordinatorDruidLeaderClient = druidLeaderClient;
    this.jsonMapper = jsonMapper;
    this.segmentWatcherConfig = segmentWatcherConfig;
    this.isCacheEnabled = plannerConfig.isMetadataSegmentCacheEnable();
    this.pollPeriodInMS = plannerConfig.getMetadataSegmentPollPeriod();
    this.scheduledExec = Execs.scheduledSingleThreaded("MetadataSegmentView-Cache--%d");
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      if (isCacheEnabled) {
        scheduledExec.schedule(new PollTask(), pollPeriodInMS, TimeUnit.MILLISECONDS);
      }
      lifecycleLock.started();
      log.info("MetadataSegmentView Started.");
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
    log.info("MetadataSegmentView is stopping.");
    if (isCacheEnabled) {
      scheduledExec.shutdown();
    }
    log.info("MetadataSegmentView Stopped.");
  }

  @VisibleForTesting
  void waitForFirstPollToComplete()
  {
    Uninterruptibles.awaitUninterruptibly(cachePopulated);
  }

  private void poll()
  {
    log.info("polling published segments from coordinator");
    final Iterator<SegmentWithOvershadowedStatus> metadataSegments = getMetadataSegments(
        coordinatorDruidLeaderClient,
        jsonMapper,
        segmentWatcherConfig.getWatchedDataSources()
    );

    final ImmutableSortedSet.Builder<SegmentsTableRow> builder = ImmutableSortedSet.orderedBy(SEGMENT_ORDER);
    while (metadataSegments.hasNext()) {
      final SegmentWithOvershadowedStatus segment = metadataSegments.next();
      final DataSegment interned = DataSegmentInterner.intern(segment.getDataSegment());
      final SegmentWithOvershadowedStatus segmentWithOvershadowedStatus = new SegmentWithOvershadowedStatus(
          interned,
          segment.isOvershadowed()
      );
      builder.add(SegmentsTableRow.from(segmentWithOvershadowedStatus));
    }
    publishedSegments = builder.build();
    cachePopulated.countDown();
  }

  protected Iterator<SegmentsTableRow> getPublishedSegmentsIterator()
  {
    if (isCacheEnabled) {
      return getSortedPublishedSegments().iterator();
    } else {
      return FluentIterable
          .from(() -> getMetadataSegments(
              coordinatorDruidLeaderClient,
              jsonMapper,
              segmentWatcherConfig.getWatchedDataSources()
          ))
          .transform(SegmentsTableRow::from)
          .iterator();
    }
  }

  /**
   * Returns the cached published segments as a SortedSet.
   * This method must be called only when {@link #isCacheEnabled} is set.
   */
  protected SortedSet<SegmentsTableRow> getSortedPublishedSegments()
  {
    assert isCacheEnabled;
    waitForFirstPollToComplete();
    return publishedSegments;
  }

  protected boolean isCacheEnabled()
  {
    return isCacheEnabled;
  }

  // Note that coordinator must be up to get segments
  @VisibleForTesting
  protected Iterator<SegmentWithOvershadowedStatus> getMetadataSegments(
      DruidLeaderClient coordinatorClient,
      ObjectMapper jsonMapper,
      Set<String> watchedDataSources
  )
  {
    String query = "/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus";
    if (watchedDataSources != null && !watchedDataSources.isEmpty()) {
      log.debug(
          "filtering datasources in published segments based on broker's watchedDataSources[%s]", watchedDataSources);
      final StringBuilder sb = new StringBuilder();
      for (String ds : watchedDataSources) {
        sb.append("datasources=").append(ds).append("&");
      }
      sb.setLength(sb.length() - 1);
      query = "/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&" + sb;
    }

    return SystemSchema.getThingsFromLeaderNode(
        query,
        new TypeReference<SegmentWithOvershadowedStatus>()
        {
        },
        coordinatorClient,
        jsonMapper
    );
  }

  private class PollTask implements Runnable
  {
    @Override
    public void run()
    {
      long delayMS = pollPeriodInMS;
      try {
        final long pollStartTime = System.nanoTime();
        poll();
        final long pollEndTime = System.nanoTime();
        final long pollTimeNS = pollEndTime - pollStartTime;
        final long pollTimeMS = TimeUnit.NANOSECONDS.toMillis(pollTimeNS);
        delayMS = Math.max(pollPeriodInMS - pollTimeMS, 0);
      }
      catch (Exception e) {
        log.makeAlert(e, "Problem polling Coordinator.").emit();
      }
      finally {
        if (!Thread.currentThread().isInterrupted()) {
          scheduledExec.schedule(new PollTask(), delayMS, TimeUnit.MILLISECONDS);
        }
      }
    }
  }
}
