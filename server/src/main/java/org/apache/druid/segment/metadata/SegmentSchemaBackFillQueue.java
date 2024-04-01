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

import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.segment.metadata.SegmentSchemaManager.SegmentSchemaMetadataPlus;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
  private final BlockingDeque<SegmentSchemaMetadataPlus> queue = new LinkedBlockingDeque<>();
  private final long executionPeriod;

  private final SegmentSchemaManager segmentSchemaManager;
  private final SegmentSchemaCache segmentSchemaCache;
  private final FingerprintGenerator fingerprintGenerator;
  private final ServiceEmitter emitter;
  private final CentralizedDatasourceSchemaConfig config;
  private ScheduledExecutorService executor;
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
    if (isEnabled()) {
      this.executor = scheduledExecutorFactory.create(1, "SegmentSchemaBackFillQueue-%s");
    }
  }

  @LifecycleStop
  public void stop()
  {
    this.executor.shutdownNow();
    scheduledFuture = null;
  }

  public void leaderStart()
  {
    if (isEnabled()) {
      scheduledFuture = executor.scheduleAtFixedRate(this::processBatchesDue, executionPeriod, executionPeriod, TimeUnit.MILLISECONDS);
    }
  }

  public void leaderStop()
  {
    if (isEnabled()) {
      scheduledFuture.cancel(true);
    }
  }

  public void add(
      SegmentId segmentId,
      RowSignature rowSignature,
      long numRows,
      Map<String, AggregatorFactory> aggregators
  )
  {
    SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregators);
    SegmentSchemaMetadata schemaMetadata = new SegmentSchemaMetadata(schemaPayload, numRows);
    queue.add(new SegmentSchemaMetadataPlus(
        segmentId,
        fingerprintGenerator.generateFingerprint(schemaMetadata.getSchemaPayload()),
        schemaMetadata)
    );
  }

  public boolean isEnabled()
  {
    return config.isEnabled() && config.isBackFillEnabled();
  }

  public void processBatchesDue()
  {
    if (queue.isEmpty()) {
      return;
    }

    log.info("Backfilling segment schema. Queue size is [%s]", queue.size());

    int itemsToProcess = Math.min(MAX_BATCH_SIZE, queue.size());

    List<SegmentSchemaMetadataPlus> polled = new ArrayList<>();

    for (int i = 0; i < itemsToProcess; i++) {
      SegmentSchemaMetadataPlus item = queue.poll();
      if (item != null) {
        polled.add(item);
      }
    }

    try {
      segmentSchemaManager.persistSchemaAndUpdateSegmentsTable(polled);
      // Mark the segments as published in the cache.
      for (SegmentSchemaMetadataPlus plus : polled) {
        segmentSchemaCache.markInTransitSMQResultPublished(plus.getSegmentId());
      }
      emitter.emit(ServiceMetricEvent.builder().setMetric("metadatacache/backfill/count", polled.size()));
    }
    catch (Exception e) {
      log.error(e, "Exception persisting schema and updating segments table.");
    }
  }
}