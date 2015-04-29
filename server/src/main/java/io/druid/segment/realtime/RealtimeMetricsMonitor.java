/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;
import io.druid.query.DruidMetrics;

import java.util.List;
import java.util.Map;

/**
 */
public class RealtimeMetricsMonitor extends AbstractMonitor
{
  private static final EmittingLogger log = new EmittingLogger(RealtimeMetricsMonitor.class);

  private final Map<FireDepartment, FireDepartmentMetrics> previousValues;
  private final List<FireDepartment> fireDepartments;

  @Inject
  public RealtimeMetricsMonitor(List<FireDepartment> fireDepartments)
  {
    this.fireDepartments = fireDepartments;
    this.previousValues = Maps.newHashMap();
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

      final long thrownAway = metrics.thrownAway() - previous.thrownAway();
      if (thrownAway > 0) {
        log.warn("[%,d] events thrown away because they are outside the window period!", thrownAway);
      }
      emitter.emit(builder.build("ingest/events/thrownAway", thrownAway));
      final long unparseable = metrics.unparseable() - previous.unparseable();
      if (unparseable > 0) {
        log.error("[%,d] Unparseable events! Turn on debug logging to see exception stack trace.", unparseable);
      }
      emitter.emit(builder.build("ingest/events/unparseable", unparseable));
      emitter.emit(builder.build("ingest/events/processed", metrics.processed() - previous.processed()));
      emitter.emit(builder.build("ingest/rows/output", metrics.rowOutput() - previous.rowOutput()));
      emitter.emit(builder.build("ingest/persists/count", metrics.numPersists() - previous.numPersists()));
      emitter.emit(builder.build("ingest/persists/time", metrics.persistTimeMillis() - previous.persistTimeMillis()));
      emitter.emit(
          builder.build(
              "ingest/persists/backPressure",
              metrics.persistBackPressureMillis() - previous.persistBackPressureMillis()
          )
      );
      emitter.emit(builder.build("ingest/persists/failed", metrics.failedPersists() - previous.failedPersists()));
      emitter.emit(builder.build("ingest/handoff/failed", metrics.failedHandoffs() - previous.failedHandoffs()));

      previousValues.put(fireDepartment, metrics);
    }

    return true;
  }
}
