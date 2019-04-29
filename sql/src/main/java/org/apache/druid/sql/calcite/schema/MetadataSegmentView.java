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
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.DataSegmentInterner;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.server.coordinator.BytesAccumulatingResponseHandler;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class polls the coordinator in background to keep the latest published segments.
 * Provides {@link #getPublishedSegments()} for others to get segments in metadata store.
 */
@ManageLifecycle
public class MetadataSegmentView
{

  private static final EmittingLogger log = new EmittingLogger(MetadataSegmentView.class);

  private final DruidLeaderClient coordinatorDruidLeaderClient;
  private final ObjectMapper jsonMapper;
  private final BytesAccumulatingResponseHandler responseHandler;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;

  private final boolean isCacheEnabled;
  /**
   * Use {@link ImmutableSortedSet} so that the order of segments is deterministic and
   * sys.segments queries return the segments in sorted order based on segmentId.
   *
   * Volatile since this reference is reassigned in {@code poll()} and then read in {@code getPublishedSegments()}
   * from other threads.
   */
  @MonotonicNonNull
  private volatile ImmutableSortedSet<SegmentWithOvershadowedStatus> publishedSegments = null;
  private final ScheduledExecutorService scheduledExec;
  private final long pollPeriodInMS;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CountDownLatch cachePopulated = new CountDownLatch(1);

  @Inject
  public MetadataSegmentView(
      final @Coordinator DruidLeaderClient druidLeaderClient,
      final ObjectMapper jsonMapper,
      final BytesAccumulatingResponseHandler responseHandler,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      final PlannerConfig plannerConfig
  )
  {
    Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.coordinatorDruidLeaderClient = druidLeaderClient;
    this.jsonMapper = jsonMapper;
    this.responseHandler = responseHandler;
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

  private void poll()
  {
    log.info("polling published segments from coordinator");
    final JsonParserIterator<SegmentWithOvershadowedStatus> metadataSegments = getMetadataSegments(
        coordinatorDruidLeaderClient,
        jsonMapper,
        responseHandler,
        segmentWatcherConfig.getWatchedDataSources()
    );

    final ImmutableSortedSet.Builder<SegmentWithOvershadowedStatus> builder = ImmutableSortedSet.naturalOrder();
    while (metadataSegments.hasNext()) {
      final SegmentWithOvershadowedStatus segment = metadataSegments.next();
      final DataSegment interned = DataSegmentInterner.intern(segment.getDataSegment());
      final SegmentWithOvershadowedStatus segmentWithOvershadowedStatus = new SegmentWithOvershadowedStatus(
          interned,
          segment.isOvershadowed()
      );
      builder.add(segmentWithOvershadowedStatus);
    }
    publishedSegments = builder.build();
    cachePopulated.countDown();
  }

  public Iterator<SegmentWithOvershadowedStatus> getPublishedSegments()
  {
    if (isCacheEnabled) {
      Uninterruptibles.awaitUninterruptibly(cachePopulated);
      return publishedSegments.iterator();
    } else {
      return getMetadataSegments(
          coordinatorDruidLeaderClient,
          jsonMapper,
          responseHandler,
          segmentWatcherConfig.getWatchedDataSources()
      );
    }
  }

  // Note that coordinator must be up to get segments
  private JsonParserIterator<SegmentWithOvershadowedStatus> getMetadataSegments(
      DruidLeaderClient coordinatorClient,
      ObjectMapper jsonMapper,
      BytesAccumulatingResponseHandler responseHandler,
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
    Request request;
    try {
      request = coordinatorClient.makeRequest(
          HttpMethod.GET,
          StringUtils.format(query),
          false
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    ListenableFuture<InputStream> future = coordinatorClient.goAsync(
        request,
        responseHandler
    );

    final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<SegmentWithOvershadowedStatus>()
    {
    });
    return new JsonParserIterator<>(
        typeRef,
        future,
        request.getUrl().toString(),
        null,
        request.getUrl().getHost(),
        jsonMapper,
        responseHandler
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
