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
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.segment.metadata.SegmentSchemaManager.SegmentSchemaMetadataPlus;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for persisting segment schema which are obtained as a result of executing segment metadata queries.
 * SMQ results are added to a queue and are periodically published to the DB in batches.
 */
@ManageLifecycle
public class SegmentSchemaBackFillQueue
{
  private static final EmittingLogger log = new EmittingLogger(SegmentSchemaBackFillQueue.class);
  private static final int MAX_BATCH_SIZE = 500;
  private final BlockingDeque<SegmentSchemaMetadataPlus> queue = new LinkedBlockingDeque<>();
  private final long executionPeriod;

  private final SegmentSchemaCache segmentSchemaCache;
  private final SegmentSchemaManager segmentSchemaManager;
  private final FingerprintGenerator fingerprintGenerator;
  private ScheduledExecutorService executor;

  @Inject
  public SegmentSchemaBackFillQueue(
      SegmentSchemaManager segmentSchemaManager,
      ScheduledExecutorFactory scheduledExecutorFactory,
      SegmentSchemaCache segmentSchemaCache,
      FingerprintGenerator fingerprintGenerator,
      CentralizedDatasourceSchemaConfig config
  )
  {
    if (config.isEnabled() && config.isBackFillEnabled()) {
      this.executor = scheduledExecutorFactory.create(1, "SegmentSchemaBackFillQueue-%s");
    }
    this.segmentSchemaManager = segmentSchemaManager;
    this.segmentSchemaCache = segmentSchemaCache;
    this.executionPeriod = config.getBackFillPeriod();
    this.fingerprintGenerator = fingerprintGenerator;
  }

  @LifecycleStart
  public void start()
  {
    if (isEnabled()) {
      scheduleQueuePoll(executionPeriod);
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (isEnabled()) {
      executor.shutdownNow();
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

  private void add(SegmentSchemaMetadataPlus plus)
  {
    queue.add(new SegmentSchemaMetadataPlus(
        plus.getSegmentId(),
        plus.getFingerprint(),
        plus.getSegmentSchemaMetadata())
    );
  }

  public boolean isEnabled()
  {
    return executor != null && !executor.isShutdown();
  }

  private void scheduleQueuePoll(long delay)
  {
    executor.schedule(this::processBatchesDue, delay, TimeUnit.MILLISECONDS);
  }

  public void processBatchesDue()
  {
    log.info("Publishing segment schemas. Queue size is [%s]", queue.size());

    int itemsToProcess = Math.min(MAX_BATCH_SIZE, queue.size());

    if (queue.isEmpty()) {
      return;
    }

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
    }
    catch (Exception e) {
      log.error(e, "Exception persisting schema and updating segments table.");
    }
  }
}
