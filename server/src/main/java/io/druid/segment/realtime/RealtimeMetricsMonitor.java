/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;

import java.util.List;
import java.util.Map;

/**
*/
public class RealtimeMetricsMonitor extends AbstractMonitor
{
  private final Map<FireDepartment, FireDepartmentMetrics> previousValues;
  private final List<FireDepartment> fireDepartments;

  @Inject
  public RealtimeMetricsMonitor(List<FireDepartment> fireDepartments)
  {
    this.fireDepartments = fireDepartments;
    previousValues = Maps.newHashMap();
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
          .setUser2(fireDepartment.getDataSchema().getDataSource());

      emitter.emit(builder.build("events/thrownAway", metrics.thrownAway() - previous.thrownAway()));
      emitter.emit(builder.build("events/unparseable", metrics.unparseable() - previous.unparseable()));
      emitter.emit(builder.build("events/processed", metrics.processed() - previous.processed()));
      emitter.emit(builder.build("rows/output", metrics.rowOutput() - previous.rowOutput()));

      previousValues.put(fireDepartment, metrics);
    }

    return true;
  }
}
