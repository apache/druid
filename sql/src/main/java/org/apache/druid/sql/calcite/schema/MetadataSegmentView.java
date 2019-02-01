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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.DataSegmentInterner;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
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
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  private final ConcurrentMap<DataSegment, DateTime> publishedSegments;
  private final ScheduledExecutorService scheduledExec;
  private final long pollPeriodinMS;
  private LifecycleLock lifecycleLock = new LifecycleLock();

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
    this.pollPeriodinMS = plannerConfig.getMetadataSegmentPollPeriod();
    this.publishedSegments = isCacheEnabled ? new ConcurrentHashMap<>(1000) : null;
    this.scheduledExec = Execs.scheduledSingleThreaded("MetadataSegmentView-Cache--%d");
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStart()) {
        throw new ISE("can't start.");
      }
      try {
        if (isCacheEnabled) {
          scheduledExec.schedule(new PollTask(), 0, TimeUnit.MILLISECONDS);
          lifecycleLock.started();
          log.info("MetadataSegmentView Started.");
        }
      }
      finally {
        lifecycleLock.exitStart();
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStop()) {
        throw new ISE("can't stop.");
      }
      if (isCacheEnabled) {
        log.info("MetadataSegmentView is stopping.");
        scheduledExec.shutdown();
        log.info("MetadataSegmentView Stopped.");
      }
    }
  }

  private void poll()
  {
    log.info("polling published segments from coordinator");
    final JsonParserIterator<DataSegment> metadataSegments = getMetadataSegments(
        coordinatorDruidLeaderClient,
        jsonMapper,
        responseHandler,
        segmentWatcherConfig.getWatchedDataSources()
    );

    final DateTime timestamp = DateTimes.nowUtc();
    while (metadataSegments.hasNext()) {
      final DataSegment interned = DataSegmentInterner.intern(metadataSegments.next());
      // timestamp is used to filter deleted segments
      publishedSegments.put(interned, timestamp);
    }
    // filter the segments from cache whose timestamp is not equal to latest timestamp stored,
    // since the presence of a segment with an earlier timestamp indicates that
    // "that" segment is not returned by coordinator in latest poll, so it's
    // likely deleted and therefore we remove it from publishedSegments
    // Since segments are not atomically replaced because it can cause high
    // memory footprint due to large number of published segments, so
    // we are incrementally removing deleted segments from the map
    // This means publishedSegments will be eventually consistent with
    // the segments in coordinator
    publishedSegments.entrySet().removeIf(e -> e.getValue() != timestamp);

  }

  public Iterator<DataSegment> getPublishedSegments()
  {
    if (isCacheEnabled) {
      return publishedSegments.keySet().iterator();
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
  private static JsonParserIterator<DataSegment> getMetadataSegments(
      DruidLeaderClient coordinatorClient,
      ObjectMapper jsonMapper,
      BytesAccumulatingResponseHandler responseHandler,
      Set<String> watchedDataSources
  )
  {
    String query = "/druid/coordinator/v1/metadata/segments";
    if (watchedDataSources != null && !watchedDataSources.isEmpty()) {
      log.debug(
          "filtering datasources in published segments based on broker's watchedDataSources[%s]", watchedDataSources);
      final StringBuilder sb = new StringBuilder();
      for (String ds : watchedDataSources) {
        sb.append("datasources=").append(ds).append("&");
      }
      sb.setLength(sb.length() - 1);
      query = "/druid/coordinator/v1/metadata/segments?" + sb;
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

    final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<DataSegment>()
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
      try {
        final long pollStartTime = System.nanoTime();
        poll();
        final long pollEndTime = System.nanoTime();
        final long pollTimeNS = pollEndTime - pollStartTime;
        final long pollTimeMS = TimeUnit.NANOSECONDS.toMillis(pollTimeNS);
        final long delayMS = Math.max(pollPeriodinMS - pollTimeMS, 0);
        if (!Thread.currentThread().isInterrupted()) {
          scheduledExec.schedule(new PollTask(), delayMS, TimeUnit.MILLISECONDS);
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Problem polling Coordinator.").emit();
      }
    }
  }

}
