/*
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.druid.emitter.ganglia;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceMetricEvent;
import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GangliaException;

import java.io.IOException;
import java.net.InetAddress;

/**
 */
public class GangliaEmitter implements Emitter
{

  private static final Logger log = new Logger(GangliaEmitter.class);

  private final GangliaEmitterConfig config;
  private final MetricRegistry registry;
  private GMetric ganglia;

  public GangliaEmitter(GangliaEmitterConfig config)
  {
    this.config = config;
    this.registry = new MetricRegistry();
  }

  @Override
  public void start()
  {
    try {
      String spoof = InetAddress.getLocalHost().getHostAddress() + ":" + InetAddress.getLocalHost().getCanonicalHostName().split("\\.")[0];
      log.info("Starting Ganglia Emitter. Spoof=[%s]", spoof);
      this.ganglia = new GMetric(
          config.getHostname(),
          config.getPort(),
          GMetric.UDPAddressingMode.UNICAST,
          0,
          true,
          null,
          spoof
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    log.info("constructed GMetric. host=[%s] port=[%s]", config.getHostname(), config.getPort());
  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof ServiceMetricEvent) {
      ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;

      String name = sanitize(metricEvent.getService() + "_" + metricEvent.getMetric());
      Histogram histogram = registry.histogram(name);
      histogram.update(metricEvent.getValue().longValue());
      try {
        ganglia.announce(
            name,
            histogram.getSnapshot().get95thPercentile(),
            metricEvent.getService()
        );
      }
      catch (GangliaException e) {
        log.error(e, "Dropping event [%s]. Unable to send to Ganglia.", event);
      }
    }
  }

  @Override
  public void flush() throws IOException
  {
  }

  @Override
  public void close() throws IOException
  {
    log.info("Closing Ganglia Emitter");
    ganglia.close();
  }

  private String sanitize(String toClean) {
    return toClean.replaceAll("[^A-Za-z0-9:]+", "_");
  }

}
