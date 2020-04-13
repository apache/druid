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
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;

import java.util.Map;

/**
 * Replaces the old RealtimeMetricsMonitor for indexing tasks that use a single FireDepartment, with changes to
 * read row ingestion stats from RowIngestionMeters (which supports moving averages) instead of FireDepartmentMetrics.
 * See comment on RowIngestionMeters for more information regarding relationship between RowIngestionMeters and
 * FireDepartmentMetrics.
 */
public class TaskRealtimeMetricsMonitor extends AbstractMonitor
{
  private static final EmittingLogger log = new EmittingLogger(TaskRealtimeMetricsMonitor.class);

  private final FireDepartment fireDepartment;
  private final RowIngestionMeters rowIngestionMeters;
  private final Map<String, String[]> dimensions;

  private FireDepartmentMetrics previousFireDepartmentMetrics;
  private RowIngestionMetersTotals previousRowIngestionMetersTotals;

  public TaskRealtimeMetricsMonitor(
      FireDepartment fireDepartment,
      RowIngestionMeters rowIngestionMeters,
      Map<String, String[]> dimensions
  )
  {
    this.fireDepartment = fireDepartment;
    this.rowIngestionMeters = rowIngestionMeters;
    this.dimensions = ImmutableMap.copyOf(dimensions);
    previousFireDepartmentMetrics = new FireDepartmentMetrics();
    previousRowIngestionMetersTotals = new RowIngestionMetersTotals(0, 0, 0, 0);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    FireDepartmentMetrics metrics = fireDepartment.getMetrics().snapshot();
    RowIngestionMetersTotals rowIngestionMetersTotals = rowIngestionMeters.getTotals();

    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
        .setDimension(DruidMetrics.DATASOURCE, fireDepartment.getDataSchema().getDataSource());
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);

    final long thrownAway = rowIngestionMetersTotals.getThrownAway() - previousRowIngestionMetersTotals.getThrownAway();
    if (thrownAway > 0) {
      log.warn(
          "[%,d] events thrown away. Possible causes: null events, events filtered out by transformSpec, or events outside earlyMessageRejectionPeriod / lateMessageRejectionPeriod.",
          thrownAway
      );
    }
    emitter.emit(builder.build("ingest/events/thrownAway", thrownAway));

    final long unparseable = rowIngestionMetersTotals.getUnparseable()
                             - previousRowIngestionMetersTotals.getUnparseable();
    if (unparseable > 0) {
      log.error("[%,d] unparseable events discarded. Turn on debug logging to see exception stack trace.", unparseable);
    }
    emitter.emit(builder.build("ingest/events/unparseable", unparseable));

    final long processedWithError = rowIngestionMetersTotals.getProcessedWithError() - previousRowIngestionMetersTotals.getProcessedWithError();
    if (processedWithError > 0) {
      log.error("[%,d] events processed with errors! Set logParseExceptions to true in the ingestion spec to log these errors.", processedWithError);
    }
    emitter.emit(builder.build("ingest/events/processedWithError", processedWithError));

    emitter.emit(builder.build("ingest/events/processed", rowIngestionMetersTotals.getProcessed() - previousRowIngestionMetersTotals.getProcessed()));

    final long dedup = metrics.dedup() - previousFireDepartmentMetrics.dedup();
    if (dedup > 0) {
      log.warn("[%,d] duplicate events!", dedup);
    }
    emitter.emit(builder.build("ingest/events/duplicate", dedup));

    emitter.emit(builder.build("ingest/rows/output", metrics.rowOutput() - previousFireDepartmentMetrics.rowOutput()));
    emitter.emit(builder.build("ingest/persists/count", metrics.numPersists() - previousFireDepartmentMetrics.numPersists()));
    emitter.emit(builder.build("ingest/persists/time", metrics.persistTimeMillis() - previousFireDepartmentMetrics.persistTimeMillis()));
    emitter.emit(builder.build("ingest/persists/cpu", metrics.persistCpuTime() - previousFireDepartmentMetrics.persistCpuTime()));
    emitter.emit(
        builder.build(
            "ingest/persists/backPressure",
            metrics.persistBackPressureMillis() - previousFireDepartmentMetrics.persistBackPressureMillis()
        )
    );
    emitter.emit(builder.build("ingest/persists/failed", metrics.failedPersists() - previousFireDepartmentMetrics.failedPersists()));
    emitter.emit(builder.build("ingest/handoff/failed", metrics.failedHandoffs() - previousFireDepartmentMetrics.failedHandoffs()));
    emitter.emit(builder.build("ingest/merge/time", metrics.mergeTimeMillis() - previousFireDepartmentMetrics.mergeTimeMillis()));
    emitter.emit(builder.build("ingest/merge/cpu", metrics.mergeCpuTime() - previousFireDepartmentMetrics.mergeCpuTime()));
    emitter.emit(builder.build("ingest/handoff/count", metrics.handOffCount() - previousFireDepartmentMetrics.handOffCount()));
    emitter.emit(builder.build("ingest/sink/count", metrics.sinkCount()));
    emitter.emit(builder.build("ingest/events/messageGap", metrics.messageGap()));

    previousRowIngestionMetersTotals = rowIngestionMetersTotals;
    previousFireDepartmentMetrics = metrics;
    return true;
  }
}
