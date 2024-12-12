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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.MonitorUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Emits metrics from {@link SegmentGenerationMetrics} and {@link RowIngestionMeters}.
 */
public class TaskRealtimeMetricsMonitor extends AbstractMonitor
{
  private static final EmittingLogger log = new EmittingLogger(TaskRealtimeMetricsMonitor.class);

  private final SegmentGenerationMetrics segmentGenerationMetrics;
  private final RowIngestionMeters rowIngestionMeters;
  private final Map<String, String[]> dimensions;
  @Nullable
  private final Map<String, Object> metricTags;

  private SegmentGenerationMetrics previousSegmentGenerationMetrics;
  private RowIngestionMetersTotals previousRowIngestionMetersTotals;

  public TaskRealtimeMetricsMonitor(
      SegmentGenerationMetrics segmentGenerationMetrics,
      RowIngestionMeters rowIngestionMeters,
      Map<String, String[]> dimensions,
      @Nullable Map<String, Object> metricTags
  )
  {
    this.segmentGenerationMetrics = segmentGenerationMetrics;
    this.rowIngestionMeters = rowIngestionMeters;
    this.dimensions = ImmutableMap.copyOf(dimensions);
    this.metricTags = metricTags;
    previousSegmentGenerationMetrics = new SegmentGenerationMetrics();
    previousRowIngestionMetersTotals = new RowIngestionMetersTotals(0, 0, 0, 0, 0);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    SegmentGenerationMetrics metrics = segmentGenerationMetrics.snapshot();
    RowIngestionMetersTotals rowIngestionMetersTotals = rowIngestionMeters.getTotals();

    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);

    final long thrownAway = rowIngestionMetersTotals.getThrownAway() - previousRowIngestionMetersTotals.getThrownAway();
    if (thrownAway > 0) {
      log.warn(
          "[%,d] events thrown away. Possible causes: null events, events filtered out by transformSpec, or events outside earlyMessageRejectionPeriod / lateMessageRejectionPeriod.",
          thrownAway
      );
    }
    builder.setDimensionIfNotNull(DruidMetrics.TAGS, metricTags);
    emitter.emit(builder.setMetric("ingest/events/thrownAway", thrownAway));

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

    long maxSegmentHandoffTime = metrics.maxSegmentHandoffTime();
    if (maxSegmentHandoffTime >= 0) {
      emitter.emit(builder.setMetric("ingest/handoff/time", maxSegmentHandoffTime));
    }

    previousRowIngestionMetersTotals = rowIngestionMetersTotals;
    previousSegmentGenerationMetrics = metrics;
    return true;
  }
}
