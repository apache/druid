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

package org.apache.druid.segment.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.SegmentSchemaManager.SegmentSchemaMetadataPlus;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class publishes the segment schema for segments obtained via segment metadata query.
 * It maintains a queue which is populated by {@link CoordinatorSegmentMetadataCache}.
 */
@ManageLifecycle
public class SegmentSchemaBackFillQueue
{
  private static final EmittingLogger log = new EmittingLogger(SegmentSchemaBackFillQueue.class);
  private static final int MAX_BATCH_SIZE = 500;

  /**
   * This queue is updated by {@link AbstractSegmentMetadataCache#cacheExec} thread,
   * and it is polled by {@link #executor} thread.
   */
  private final BlockingDeque<SegmentSchemaMetadataPlus> queue = new LinkedBlockingDeque<>();
  private final long executionPeriod;

  private final SegmentSchemaManager segmentSchemaManager;
  private final SegmentSchemaCache segmentSchemaCache;
  private final FingerprintGenerator fingerprintGenerator;
  private final ServiceEmitter emitter;
  private final CentralizedDatasourceSchemaConfig config;
  private final ScheduledExecutorService executor;
  private @Nullable ScheduledFuture<?> scheduledFuture = null;

  @Inject
  public SegmentSchemaBackFillQueue(
      SegmentSchemaManager segmentSchemaManager,
      ScheduledExecutorFactory scheduledExecutorFactory,
      SegmentSchemaCache segmentSchemaCache,
      FingerprintGenerator fingerprintGenerator,
      ServiceEmitter emitter,
      CentralizedDatasourceSchemaConfig config
  )
  {
    this.segmentSchemaManager = segmentSchemaManager;
    this.segmentSchemaCache = segmentSchemaCache;
    this.fingerprintGenerator = fingerprintGenerator;
    this.emitter = emitter;
    this.config = config;
    this.executionPeriod = config.getBackFillPeriod();
    this.executor = isEnabled() ? scheduledExecutorFactory.create(1, "SegmentSchemaBackFillQueue-%s") : null;
  }

  @LifecycleStop
  public void stop()
  {
    this.executor.shutdownNow();
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
    }
  }

  public void onLeaderStart()
  {
    if (isEnabled()) {
      scheduledFuture = executor.scheduleAtFixedRate(this::processBatchesDueSafely, executionPeriod, executionPeriod, TimeUnit.MILLISECONDS);
    }
  }

  public void onLeaderStop()
  {
    if (isEnabled()) {
      if (scheduledFuture != null) {
        scheduledFuture.cancel(true);
      }
    }
  }

  public void add(
      SegmentId segmentId,
      RowSignature rowSignature,
      Map<String, AggregatorFactory> aggregators,
      long numRows
  )
  {
    SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregators);
    SchemaPayloadPlus schemaMetadata = new SchemaPayloadPlus(schemaPayload, numRows);
    queue.add(
        new SegmentSchemaMetadataPlus(
            segmentId,
            fingerprintGenerator.generateFingerprint(
                schemaMetadata.getSchemaPayload(),
                segmentId.getDataSource(),
                CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
            ),
            schemaMetadata
        )
    );
  }

  public boolean isEnabled()
  {
    return config.isEnabled() && config.isBackFillEnabled();
  }

  private void processBatchesDueSafely()
  {
    try {
      processBatchesDue();
    }
    catch (Exception e) {
      log.error(e, "Exception backfilling segment schemas.");
    }
  }

  @VisibleForTesting
  public void processBatchesDue()
  {
    if (queue.isEmpty()) {
      return;
    }

    Stopwatch stopwatch = Stopwatch.createStarted();

    log.info("Backfilling segment schema. Queue size is [%s]", queue.size());

    int itemsToProcess = Math.min(MAX_BATCH_SIZE, queue.size());

    Map<String, List<SegmentSchemaMetadataPlus>> polled = new HashMap<>();
    for (int i = 0; i < itemsToProcess; i++) {
      SegmentSchemaMetadataPlus item = queue.poll();
      if (item != null) {
        polled.computeIfAbsent(item.getSegmentId().getDataSource(), value -> new ArrayList<>()).add(item);
      }
    }

    for (Map.Entry<String, List<SegmentSchemaMetadataPlus>> entry : polled.entrySet()) {
      try {
        segmentSchemaManager.persistSchemaAndUpdateSegmentsTable(entry.getKey(), entry.getValue(), CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);
        // Mark the segments as published in the cache.
        for (SegmentSchemaMetadataPlus plus : entry.getValue()) {
          segmentSchemaCache.markInTransitSMQResultPublished(plus.getSegmentId());
        }
        emitter.emit(
            ServiceMetricEvent.builder()
                                       .setDimension("dataSource", entry.getKey())
                                       .setMetric("metadatacache/backfill/count", polled.size())
        );
      }
      catch (Exception e) {
        log.error(e, "Exception persisting schema and updating segments table for datasource [%s].", entry.getKey());
      }
    }
    emitter.emit(ServiceMetricEvent.builder().setMetric("metadatacache/backfill/time", stopwatch.millisElapsed()));
  }
}
