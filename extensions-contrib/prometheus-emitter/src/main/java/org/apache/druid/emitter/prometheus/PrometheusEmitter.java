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
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  private static final String TAG_HOSTNAME = "host_name";
  private static final String TAG_SERVICE = "druid_service";

  private HTTPServer server;
  private PushGateway pushGateway;
  private volatile String identifier;
  private ScheduledExecutorService exec;

  static PrometheusEmitter of(PrometheusEmitterConfig config)
  {
    return new PrometheusEmitter(config);
  }

  public PrometheusEmitter(PrometheusEmitterConfig config)
  {
    this.config = config;
    this.strategy = config.getStrategy();
    metrics = new Metrics(
        config.getNamespace(),
        config.getDimensionMapPath(),
        config.isAddHostAsLabel(),
        config.isAddServiceAsLabel(),
        config.getExtraLabels()
    );
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
      String address = config.getPushGatewayAddress();
      if (address.startsWith("https") || address.startsWith("http")) {
        URL myURL = createURLSneakily(address);
        pushGateway = new PushGateway(myURL);
      } else {
        pushGateway = new PushGateway(address);
      }
      exec = ScheduledExecutors.fixed(1, "PrometheusPushGatewayEmitter-%s");
      exec.scheduleAtFixedRate(
          () -> flush(),
          config.getFlushPeriod(),
          config.getFlushPeriod(),
          TimeUnit.SECONDS
      );
    }
  }

  private static URL createURLSneakily(final String urlString)
  {
    try {
      return new URL(urlString);
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
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
    String host = metricEvent.getHost();
    Map<String, Object> userDims = metricEvent.getUserDims();
    identifier = (userDims.get("task") == null ? metricEvent.getHost() : (String) userDims.get("task"));
    Number value = metricEvent.getValue();

    DimensionsAndCollector metric = metrics.getByName(name, service);
    if (metric != null) {
      String[] labelValues = new String[metric.getDimensions().length];
      String[] labelNames = metric.getDimensions();

      Map<String, String> extraLabels = config.getExtraLabels();

      for (int i = 0; i < labelValues.length; i++) {
        String labelName = labelNames[i];
        //labelName is controlled by the user. Instead of potential NPE on invalid labelName we use "unknown" as the dimension value
        Object userDim = userDims.get(labelName);

        if (userDim != null) {
          labelValues[i] = PATTERN.matcher(userDim.toString()).replaceAll("_");
        } else {
          if (config.isAddHostAsLabel() && TAG_HOSTNAME.equals(labelName)) {
            labelValues[i] = host;
          } else if (config.isAddServiceAsLabel() && TAG_SERVICE.equals(labelName)) {
            labelValues[i] = service;
          } else if (extraLabels.containsKey(labelName)) {
            labelValues[i] = config.getExtraLabels().get(labelName);
          } else {
            labelValues[i] = "unknown";
          }
        }
      }

      if (metric.getCollector() instanceof Counter) {
        ((Counter) metric.getCollector()).labels(labelValues).inc(value.doubleValue());
      } else if (metric.getCollector() instanceof Gauge) {
        ((Gauge) metric.getCollector()).labels(labelValues).set(value.doubleValue());
      } else if (metric.getCollector() instanceof Histogram) {
        ((Histogram) metric.getCollector()).labels(labelValues)
                                           .observe(value.doubleValue() / metric.getConversionFactor());
      } else {
        log.error("Unrecognized metric type [%s]", metric.getCollector().getClass());
      }
    } else {
      log.debug("Unmapped metric [%s]", name);
    }
  }

  private void pushMetric()
  {
    if (pushGateway == null || identifier == null) {
      return;
    }
    Map<String, DimensionsAndCollector> map = metrics.getRegisteredMetrics();
    CollectorRegistry metrics = new CollectorRegistry();
    try {
      for (DimensionsAndCollector collector : map.values()) {
        metrics.register(collector.getCollector());
      }
      pushGateway.push(metrics, config.getNamespace(), ImmutableMap.of(config.getNamespace(), identifier));
    }
    catch (IOException e) {
      log.error(e, "Unable to push prometheus metrics to pushGateway");
    }
  }

  @Override
  public void flush()
  {
    pushMetric();
  }

  @Override
  public void close()
  {
    if (strategy.equals(PrometheusEmitterConfig.Strategy.exporter)) {
      if (server != null) {
        server.close();
      }
    } else {
      exec.shutdownNow();
      flush();

      try {
        if (config.getWaitForShutdownDelay().getMillis() > 0) {
          log.info("Waiting [%s]ms before deleting metrics from the push gateway.", config.getWaitForShutdownDelay().getMillis());
          Thread.sleep(config.getWaitForShutdownDelay().getMillis());
        }
      }
      catch (InterruptedException e) {
        log.error(e, "Interrupted while waiting for shutdown delay. Deleting metrics from the push gateway now.");
      }
      finally {
        deletePushGatewayMetrics();
      }
    }
  }

  private void deletePushGatewayMetrics()
  {
    if (pushGateway != null && config.isDeletePushGatewayMetricsOnShutdown()) {
      try {
        pushGateway.delete(config.getNamespace(), ImmutableMap.of(config.getNamespace(), identifier));
      }
      catch (IOException e) {
        log.error(e, "Unable to delete prometheus metrics from push gateway");
      }
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
