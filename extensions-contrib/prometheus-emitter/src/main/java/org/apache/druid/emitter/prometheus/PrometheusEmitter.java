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

package org.apache.druid.emitter.prometheus;

import java.util.Map;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

/**
 */
public class PrometheusEmitter implements Emitter {

  private static final Logger log = new Logger(PrometheusEmitter.class);
  private final Metrics metrics;

  static PrometheusEmitter of(PrometheusEmitterConfig config) {
    return new PrometheusEmitter(config);
  }

  public PrometheusEmitter(PrometheusEmitterConfig config) {
    metrics = new Metrics(config.getNamespace());
  }


  @Override
  public void start() {
  }

  @Override
  public void emit(Event event) {
    if (event instanceof ServiceMetricEvent) {
      emitMetric((ServiceMetricEvent) event);
    }
  }

  void emitMetric(ServiceMetricEvent metricEvent) {
    String host = metricEvent.getHost();
    String service = metricEvent.getService();
    String metric = metricEvent.getMetric();
    Map<String, Object> userDims = metricEvent.getUserDims();
    Number value = metricEvent.getValue();

    Metrics.Metric byName = metrics.getByName(metric);
    String[] labelValues = new String[byName.getDimensions().length];
    String[] labelNames = byName.getDimensions();
    for (int i = 0; i < labelValues.length; i++) {
      String labelName = labelNames[i];
      Object userDim = userDims.get(labelName);
      labelValues[i] = userDim.toString();
    }

    if (byName.getCollector() instanceof Counter) {
      ((Counter) byName.getCollector()).labels(labelValues).inc(value.doubleValue());
    } else if (byName.getCollector() instanceof Gauge) {
      ((Gauge) byName.getCollector()).labels(labelValues).set(value.doubleValue());
    } else if (byName.getCollector() instanceof Histogram) {
      ((Histogram) byName.getCollector()).labels(labelValues).observe(value.doubleValue());
    } else {
      //TODO
    }

  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }
}
