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

package org.apache.druid.indexing.common.stats;

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.segment.incremental.InputRowThrownAwayReason;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;

import java.util.Map;

/**
 * Emits metrics from {@link SegmentGenerationMetrics} and {@link RowIngestionMeters}.
 */
public class TaskRealtimeMetricsMonitor extends AbstractMonitor
{
  private static final EmittingLogger log = new EmittingLogger(TaskRealtimeMetricsMonitor.class);
  private static final String REASON_DIMENSION = "reason";

  private final SegmentGenerationMetrics segmentGenerationMetrics;
  private final RowIngestionMeters rowIngestionMeters;
  private final ServiceMetricEvent.Builder builder;

  private SegmentGenerationMetrics previousSegmentGenerationMetrics;
  private RowIngestionMetersTotals previousRowIngestionMetersTotals;

  public TaskRealtimeMetricsMonitor(
      SegmentGenerationMetrics segmentGenerationMetrics,
      RowIngestionMeters rowIngestionMeters,
      ServiceMetricEvent.Builder metricEventBuilder
  )
  {
    this.segmentGenerationMetrics = segmentGenerationMetrics;
    this.rowIngestionMeters = rowIngestionMeters;
    this.builder = metricEventBuilder;
    previousSegmentGenerationMetrics = new SegmentGenerationMetrics();
    previousRowIngestionMetersTotals = new RowIngestionMetersTotals(0, 0, 0, Map.of(), 0);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    SegmentGenerationMetrics metrics = segmentGenerationMetrics.snapshot();
    RowIngestionMetersTotals rowIngestionMetersTotals = rowIngestionMeters.getTotals();

    // Emit per-reason metrics with the reason dimension
    final Map<InputRowThrownAwayReason, Long> currentThrownAwayByReason = rowIngestionMetersTotals.getThrownAwayByReason();
    final Map<InputRowThrownAwayReason, Long> previousThrownAwayByReason = previousRowIngestionMetersTotals.getThrownAwayByReason();
    long totalThrownAway = 0;
    for (InputRowThrownAwayReason reason : InputRowThrownAwayReason.values()) {
      final long currentCount = currentThrownAwayByReason.getOrDefault(reason, 0L);
      final long previousCount = previousThrownAwayByReason.getOrDefault(reason, 0L);
      final long delta = currentCount - previousCount;
      if (delta > 0) {
        totalThrownAway += delta;
        emitter.emit(
            builder.setDimension(REASON_DIMENSION, reason.getReason())
                   .setMetric("ingest/events/thrownAway", delta)
        );
      }
    }
    if (totalThrownAway > 0) {
      log.warn(
          "[%,d] events thrown away. Possible causes: null events, events filtered out by transformSpec, or events outside earlyMessageRejectionPeriod / lateMessageRejectionPeriod.",
          totalThrownAway
      );
    }

    final long unparseable = rowIngestionMetersTotals.getUnparseable()
                             - previousRowIngestionMetersTotals.getUnparseable();
    if (unparseable > 0) {
      log.error("[%,d] unparseable events discarded. Turn on debug logging to see exception stack trace.", unparseable);
    }
    emitter.emit(builder.setMetric("ingest/events/unparseable", unparseable));

    final long processedWithError = rowIngestionMetersTotals.getProcessedWithError() - previousRowIngestionMetersTotals.getProcessedWithError();
    if (processedWithError > 0) {
      log.error("[%,d] events processed with errors! Set logParseExceptions to true in the ingestion spec to log these errors.", processedWithError);
    }
    emitter.emit(builder.setMetric("ingest/events/processedWithError", processedWithError));

    emitter.emit(builder.setMetric("ingest/events/processed", rowIngestionMetersTotals.getProcessed() - previousRowIngestionMetersTotals.getProcessed()));

    final long dedup = metrics.dedup() - previousSegmentGenerationMetrics.dedup();
    if (dedup > 0) {
      log.warn("[%,d] duplicate events!", dedup);
    }
    emitter.emit(builder.setMetric("ingest/events/duplicate", dedup));
    emitter.emit(
        builder.setMetric(
            "ingest/input/bytes",
            rowIngestionMetersTotals.getProcessedBytes() - previousRowIngestionMetersTotals.getProcessedBytes()
        )
    );

    emitter.emit(builder.setMetric("ingest/rows/output", metrics.rowOutput() - previousSegmentGenerationMetrics.rowOutput()));
    emitter.emit(builder.setMetric("ingest/persists/count", metrics.numPersists() - previousSegmentGenerationMetrics.numPersists()));
    emitter.emit(builder.setMetric("ingest/persists/time", metrics.persistTimeMillis() - previousSegmentGenerationMetrics.persistTimeMillis()));
    emitter.emit(builder.setMetric("ingest/persists/cpu", metrics.persistCpuTime() - previousSegmentGenerationMetrics.persistCpuTime()));
    emitter.emit(
        builder.setMetric(
            "ingest/persists/backPressure",
            metrics.persistBackPressureMillis() - previousSegmentGenerationMetrics.persistBackPressureMillis()
        )
    );
    emitter.emit(builder.setMetric("ingest/persists/failed", metrics.failedPersists() - previousSegmentGenerationMetrics.failedPersists()));
    emitter.emit(builder.setMetric("ingest/handoff/failed", metrics.failedHandoffs() - previousSegmentGenerationMetrics.failedHandoffs()));
    emitter.emit(builder.setMetric("ingest/merge/time", metrics.mergeTimeMillis() - previousSegmentGenerationMetrics.mergeTimeMillis()));
    emitter.emit(builder.setMetric("ingest/merge/cpu", metrics.mergeCpuTime() - previousSegmentGenerationMetrics.mergeCpuTime()));
    emitter.emit(builder.setMetric("ingest/handoff/count", metrics.handOffCount() - previousSegmentGenerationMetrics.handOffCount()));
    emitter.emit(builder.setMetric("ingest/sink/count", metrics.sinkCount()));

    long messageGap = metrics.messageGap();
    if (messageGap >= 0) {
      emitter.emit(builder.setMetric("ingest/events/messageGap", messageGap));
    }

    final SegmentGenerationMetrics.MessageGapStats messageGapStats = metrics.getMessageGapStats();
    if (messageGapStats.count() > 0) {
      emitter.emit(builder.setMetric("ingest/events/minMessageGap", messageGapStats.min()));
      emitter.emit(builder.setMetric("ingest/events/maxMessageGap", messageGapStats.max()));
      emitter.emit(builder.setMetric("ingest/events/avgMessageGap", messageGapStats.avg()));
    }

    long maxSegmentHandoffTime = metrics.maxSegmentHandoffTime();
    if (maxSegmentHandoffTime >= 0) {
      emitter.emit(builder.setMetric("ingest/handoff/time", maxSegmentHandoffTime));
    }

    previousRowIngestionMetersTotals = rowIngestionMetersTotals;
    previousSegmentGenerationMetrics = metrics;
    return true;
  }
}
