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

package org.apache.druid.emitter.dropwizard;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DropwizardEmitter implements Emitter
{
  private static Logger log = new Logger(DropwizardEmitter.class);
  private final MetricRegistry metricsRegistry = new MetricRegistry();
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final DropwizardConverter converter;
  private final List<Emitter> alertEmitters;
  private final List<DropwizardReporter> reporters;
  private final DropwizardEmitterConfig config;
  // Note: the gauges do not represent the actual instantaneous value for the metrics.
  // Instead they have the last known value for it.
  private final ConcurrentHashMap<String, Number> gagues = new ConcurrentHashMap<>();

  public DropwizardEmitter(
      DropwizardEmitterConfig config,
      ObjectMapper mapper,
      List<Emitter> alertEmitters
  )
  {
    this.alertEmitters = alertEmitters;
    this.config = config;
    this.reporters = config.getReporters();
    this.converter = new DropwizardConverter(mapper, config.getDimensionMapPath());
  }


  @Override
  public void start()
  {
    for (DropwizardReporter reporter : reporters) {
      try {
        reporter.start(metricsRegistry);
      }
      catch (IOException e) {
        log.error(e, "Error while flushing Dropwizard reporter");
      }
    }
    started.set(true);
  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof ServiceMetricEvent) {
      ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
      String host = metricEvent.getHost();
      String service = metricEvent.getService();
      String metric = metricEvent.getMetric();
      Map<String, Object> userDims = metricEvent.getUserDims();
      Number value = metricEvent.getValue();
      ImmutableList.Builder<String> nameBuilder = new ImmutableList.Builder<>();
      LinkedHashMap<String, String> dims = new LinkedHashMap<>();
      final DropwizardMetricSpec metricSpec = converter.addFilteredUserDims(service, metric, userDims, dims);

      if (metricSpec != null) {
        if (config.isIncludeDimensionNames()) {
          if (config.getPrefix() != null) {
            nameBuilder.add(config.getPrefix());
          }
          nameBuilder.add("metric=" + metric);
          nameBuilder.add("service=" + service);
          if (config.getIncludeHost()) {
            nameBuilder.add("hostname=" + host);
          }
          dims.forEach((key, value1) -> nameBuilder.add(key + "=" + value1));
        } else {
          nameBuilder.add(metric);
          nameBuilder.add(service);
          if (config.getIncludeHost()) {
            nameBuilder.add(host);
          }
          nameBuilder.addAll(dims.values());
        }

        String fullName = StringUtils.replaceChar(Joiner.on(",").join(nameBuilder.build()), '/', ".");
        updateMetric(fullName, value, metricSpec);
      } else {
        log.debug("Service=[%s], Metric=[%s] has no mapping", service, metric);
      }
    } else if (event instanceof AlertEvent) {
      for (Emitter emitter : alertEmitters) {
        emitter.emit(event);
      }
    } else {
      throw new ISE("unknown event type [%s]", event.getClass());
    }
  }

  private void updateMetric(String name, Number value, DropwizardMetricSpec metricSpec)
  {
    switch (metricSpec.getType()) {
      case meter:
        metricsRegistry.meter(name).mark(value.longValue());
        break;
      case timer:
        metricsRegistry.timer(name)
                       .update(value.longValue(), metricSpec.getTimeUnit());
        break;
      case counter:
        metricsRegistry.counter(name).inc(value.longValue());
        break;
      case histogram:
        metricsRegistry.histogram(name).update(value.longValue());
        break;
      case gauge:
        Number prevVal = gagues.put(name, value);
        // It is the first time we get this metric, register it with metricsRegistry
        if (prevVal == null) {
          metricsRegistry.register(name, (Gauge<Number>) () -> gagues.get(name));
        }
    }
  }

  @Override
  public void flush()
  {
    for (DropwizardReporter reporter : reporters) {
      try {
        reporter.flush();
      }
      catch (IOException e) {
        log.error(e, "Error while flushing Dropwizard reporter");
      }
    }
  }

  @Override
  public void close()
  {
    for (DropwizardReporter reporter : reporters) {
      try {
        reporter.close();
      }
      catch (IOException e) {
        log.error(e, "Error while closing Dropwizard reporter");
      }
    }
  }

}
