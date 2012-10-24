package com.metamx.druid.realtime;

import com.google.common.collect.Maps;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;

import java.util.List;
import java.util.Map;

/**
*/
public class RealtimeMetricsMonitor extends AbstractMonitor
{
  Map<FireDepartment, FireDepartmentMetrics> previousValues;
  private final List<FireDepartment> fireDepartments;

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
          .setUser2(fireDepartment.getSchema().getDataSource());

      emitter.emit(builder.build("events/thrownAway", metrics.thrownAway() - previous.thrownAway()));
      emitter.emit(builder.build("events/unparseable", metrics.unparseable() - previous.unparseable()));
      emitter.emit(builder.build("events/processed", metrics.processed() - previous.processed()));
      emitter.emit(builder.build("rows/output", metrics.rowOutput() - previous.rowOutput()));

      previousValues.put(fireDepartment, metrics);
    }

    return true;
  }
}
