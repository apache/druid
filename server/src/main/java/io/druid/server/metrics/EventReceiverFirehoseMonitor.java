/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.java.util.metrics.AbstractMonitor;
import io.druid.java.util.metrics.KeyedDiff;
import io.druid.java.util.metrics.MonitorUtils;
import io.druid.query.DruidMetrics;

import java.util.Map;
import java.util.Properties;

public class EventReceiverFirehoseMonitor extends AbstractMonitor
{

  private final EventReceiverFirehoseRegister register;
  private final KeyedDiff keyedDiff = new KeyedDiff();
  private final Map<String, String[]> dimensions;

  @Inject
  public EventReceiverFirehoseMonitor(
      EventReceiverFirehoseRegister eventReceiverFirehoseRegister,
      Properties props
  )
  {
    this.register = eventReceiverFirehoseRegister;
    this.dimensions = MonitorsConfig.extractDimensions(
        props,
        Lists.newArrayList(DruidMetrics.DATASOURCE, DruidMetrics.TASK_ID, DruidMetrics.TASK_TYPE)
    );
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Map.Entry<String, EventReceiverFirehoseMetric> entry : register.getMetrics()) {
      final String serviceName = entry.getKey();
      final EventReceiverFirehoseMetric metric = entry.getValue();

      final ServiceMetricEvent.Builder builder = createEventBuilder(serviceName)
          .setDimension(
              "bufferCapacity",
              String.valueOf(metric.getCapacity())
          );
      emitter.emit(builder.build("ingest/events/buffered", metric.getCurrentBufferSize()));
      Map<String, Long> diff = keyedDiff.to(
          serviceName,
          ImmutableMap.of("ingest/bytes/received", metric.getBytesReceived())
      );
      if (diff != null) {
        final ServiceMetricEvent.Builder eventBuilder = createEventBuilder(serviceName);
        for (Map.Entry<String, Long> diffEntry : diff.entrySet()) {
          emitter.emit(eventBuilder.build(diffEntry.getKey(), diffEntry.getValue()));
        }
      }
    }

    return true;
  }

  private ServiceMetricEvent.Builder createEventBuilder(String serviceName)
  {
    ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder()
                                                           .setDimension("serviceName", serviceName);
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);
    return builder;
  }
}
