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

package org.apache.druid.server.metrics;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.Map;

/**
 * Reports a heartbeat for the service.
 */
public class ServiceStatusMonitor extends AbstractMonitor
{
  /**
   * The named binding for tags that should be reported with the `service/heartbeat` metric.
   */
  public static final String HEARTBEAT_TAGS_BINDING = "heartbeat";

  @Named(HEARTBEAT_TAGS_BINDING)
  @Inject(optional = true)
  Supplier<Map<String, Object>> heartbeatTagsSupplier = null;

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    if (heartbeatTagsSupplier != null && heartbeatTagsSupplier.get() != null) {
      heartbeatTagsSupplier.get().forEach(builder::setDimension);
    }

    emitter.emit(builder.setMetric("service/heartbeat", 1));
    return true;
  }
}

