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


import com.google.common.collect.ImmutableMap;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public class PrometheusEmitter implements Emitter
{
  private static final Logger log = new Logger(PrometheusEmitter.class);
  private final Metrics metrics;
  private final PrometheusEmitterConfig config;
  private final PrometheusEmitterConfig.Strategy strategy;
  private static final Pattern PATTERN = Pattern.compile("[^a-zA-Z0-9_][^a-zA-Z0-9_]*");

  private HTTPServer server;
  private PushGateway pushGateway;
  private String identifier;

  static PrometheusEmitter of(PrometheusEmitterConfig config)
  {
    return new PrometheusEmitter(config);
  }

  public PrometheusEmitter(PrometheusEmitterConfig config)
  {
    this.config = config;
    this.strategy = config.getStrategy();
    metrics = new Metrics(config.getNamespace(), config.getDimensionMapPath());
  }


  @Override
  public void start()
  {
    if (strategy.equals(PrometheusEmitterConfig.Strategy.exporter)) {
      if (server == null) {
        try {
          server = new HTTPServer(config.getPort());
        }
        catch (IOException e) {
          log.error(e, "Unable to start prometheus HTTPServer");
        }
      } else {
        log.error("HTTPServer is already started");
      }
    } else if (strategy.equals(PrometheusEmitterConfig.Strategy.pushgateway)) {
      pushGateway = new PushGateway(config.getPushGatewayAddress());
    }

  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof ServiceMetricEvent) {
      emitMetric((ServiceMetricEvent) event);
    }
  }

  private void emitMetric(ServiceMetricEvent metricEvent)
  {
    String name = metricEvent.getMetric();
    String service = metricEvent.getService();
    Map<String, Object> userDims = metricEvent.getUserDims();
    identifier = (userDims.get("task") == null ? metricEvent.getHost() : (String) userDims.get("task"));
    Number value = metricEvent.getValue();

    DimensionsAndCollector metric = metrics.getByName(name, service);
    if (metric != null) {
      String[] labelValues = new String[metric.getDimensions().length];
      String[] labelNames = metric.getDimensions();
      for (int i = 0; i < labelValues.length; i++) {
        String labelName = labelNames[i];
        //labelName is controlled by the user. Instead of potential NPE on invalid labelName we use "unknown" as the dimension value
        Object userDim = userDims.get(labelName);
        labelValues[i] = userDim != null ? PATTERN.matcher(userDim.toString()).replaceAll("_") : "unknown";
      }

      if (metric.getCollector() instanceof Counter) {
        ((Counter) metric.getCollector()).labels(labelValues).inc(value.doubleValue());
      } else if (metric.getCollector() instanceof Gauge) {
        ((Gauge) metric.getCollector()).labels(labelValues).set(value.doubleValue());
      } else if (metric.getCollector() instanceof Histogram) {
        ((Histogram) metric.getCollector()).labels(labelValues).observe(value.doubleValue() / metric.getConversionFactor());
      } else {
        log.error("Unrecognized metric type [%s]", metric.getCollector().getClass());
      }
    } else {
      log.debug("Unmapped metric [%s]", name);
    }
  }

  private void pushMetric()
  {
    Map<String, DimensionsAndCollector> map = metrics.getRegisteredMetrics();
    try {
      for (DimensionsAndCollector collector : map.values()) {
        if (config.getNamespace() != null) {
          pushGateway.push(collector.getCollector(), config.getNamespace(), ImmutableMap.of(config.getNamespace(), identifier));
        }
      }
    }
    catch (IOException e) {
      log.error(e, "Unable to push prometheus metrics to pushGateway");
    }
  }

  @Override
  public void flush()
  {
    if (pushGateway != null) {
      pushMetric();
    }
  }

  @Override
  public void close()
  {
    if (strategy.equals(PrometheusEmitterConfig.Strategy.exporter)) {
      if (server != null) {
        server.stop();
      }
    } else {
      flush();
    }
  }

  public HTTPServer getServer()
  {
    return server;
  }

  public PushGateway getPushGateway()
  {
    return pushGateway;
  }

  public void setPushGateway(PushGateway pushGateway)
  {
    this.pushGateway = pushGateway;
  }
}
