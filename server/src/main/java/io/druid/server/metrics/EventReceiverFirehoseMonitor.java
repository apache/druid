/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.metrics;

import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;

import java.util.Map;

public class EventReceiverFirehoseMonitor extends AbstractMonitor
{

  private final EventReceiverFirehoseRegister register;

  @Inject
  public EventReceiverFirehoseMonitor(
      EventReceiverFirehoseRegister eventReceiverFirehoseRegister
  )
  {
    this.register = eventReceiverFirehoseRegister;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Map.Entry<String, EventReceiverFirehoseMetric> entry : register.getMetrics()) {
      final String serviceName = entry.getKey();
      final EventReceiverFirehoseMetric metric = entry.getValue();

      final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder()
                                                                   .setDimension("serviceName", serviceName)
                                                                   .setDimension(
                                                                       "bufferCapacity",
                                                                       String.valueOf(metric.getCapacity())
                                                                   );

      emitter.emit(builder.build("ingest/events/buffered", metric.getCurrentBufferSize()));
    }

    return true;
  }
}
