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

package org.apache.druid.segment.realtime;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.MonitorUtils;
import org.apache.druid.query.DruidMetrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RealtimeMetricsMonitor is only used by RealtimeIndexTask, this monitor only supports FireDepartmentMetrics.
 * New ingestion task types should support RowIngestionMeters and use TaskRealtimeMetricsMonitor instead.
 * Please see the comment on RowIngestionMeters for more information regarding the relationship between
 * RowIngestionMeters and FireDepartmentMetrics.
 */
public class RealtimeMetricsMonitor extends AbstractMonitor
{
  private static final EmittingLogger log = new EmittingLogger(RealtimeMetricsMonitor.class);

  private final Map<FireDepartment, FireDepartmentMetrics> previousValues;
  private final List<FireDepartment> fireDepartments;
  private final Map<String, String[]> dimensions;

  @Inject
  public RealtimeMetricsMonitor(List<FireDepartment> fireDepartments)
  {
    this(fireDepartments, ImmutableMap.of());
  }

  public RealtimeMetricsMonitor(List<FireDepartment> fireDepartments, Map<String, String[]> dimensions)
  {
    this.fireDepartments = fireDepartments;
    this.previousValues = new HashMap<>();
    this.dimensions = ImmutableMap.copyOf(dimensions);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (FireDepartment fireDepartment : fireDepartments) {
      FireDepartmentMetrics metrics = fireDepartment.getMetrics().snapshot();
      FireDepartmentMetrics previous = previousValues.get(fireDepartment);

      if (previous == null) {
        previous = new FireDepartmentMetrics();
      }

      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setDimension(DruidMetrics.DATASOURCE, fireDepartment.getDataSchema().getDataSource());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      final long thrownAway = metrics.thrownAway() - previous.thrownAway();
      if (thrownAway > 0) {
        log.warn(
            "[%,d] events thrown away. Possible causes: null events, events filtered out by transformSpec, or events outside windowPeriod.",
            thrownAway
        );
      }
      emitter.emit(builder.build("ingest/events/thrownAway", thrownAway));
      final long unparseable = metrics.unparseable() - previous.unparseable();
      if (unparseable > 0) {
        log.error(
            "[%,d] unparseable events discarded. Turn on debug logging to see exception stack trace.",
            unparseable
        );
      }
      emitter.emit(builder.build("ingest/events/unparseable", unparseable));
      final long dedup = metrics.dedup() - previous.dedup();
      if (dedup > 0) {
        log.warn("[%,d] duplicate events!", dedup);
      }
      emitter.emit(builder.build("ingest/events/duplicate", dedup));

      emitter.emit(builder.build("ingest/events/processed", metrics.processed() - previous.processed()));
      emitter.emit(builder.build("ingest/rows/output", metrics.rowOutput() - previous.rowOutput()));
      emitter.emit(builder.build("ingest/persists/count", metrics.numPersists() - previous.numPersists()));
      emitter.emit(builder.build("ingest/persists/time", metrics.persistTimeMillis() - previous.persistTimeMillis()));
      emitter.emit(builder.build("ingest/persists/cpu", metrics.persistCpuTime() - previous.persistCpuTime()));
      emitter.emit(
          builder.build(
              "ingest/persists/backPressure",
              metrics.persistBackPressureMillis() - previous.persistBackPressureMillis()
          )
      );
      emitter.emit(builder.build("ingest/persists/failed", metrics.failedPersists() - previous.failedPersists()));
      emitter.emit(builder.build("ingest/handoff/failed", metrics.failedHandoffs() - previous.failedHandoffs()));
      emitter.emit(builder.build("ingest/merge/time", metrics.mergeTimeMillis() - previous.mergeTimeMillis()));
      emitter.emit(builder.build("ingest/merge/cpu", metrics.mergeCpuTime() - previous.mergeCpuTime()));
      emitter.emit(builder.build("ingest/handoff/count", metrics.handOffCount() - previous.handOffCount()));
      emitter.emit(builder.build("ingest/sink/count", metrics.sinkCount()));
      emitter.emit(builder.build("ingest/events/messageGap", metrics.messageGap()));
      previousValues.put(fireDepartment, metrics);
    }

    return true;
  }
}
