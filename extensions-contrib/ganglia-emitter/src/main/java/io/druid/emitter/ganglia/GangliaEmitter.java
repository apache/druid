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
package io.druid.emitter.ganglia;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GangliaException;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.Event;
import io.druid.java.util.emitter.service.ServiceMetricEvent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by yangxuan on 2018/9/6.
 */
public class GangliaEmitter implements Emitter
{
  private static final Logger log = new Logger(GangliaEmitter.class);
  private static final String DRUID_METRIC_SEPARATOR = "\\/";
  private static final String STATSD_SEPARATOR = ":|\\|";
  private static final String BLANK = "\\s+";

  private final GangliaEmitterConfig config;
  private final MetricRegistry registry;
  private GMetric ganglia;
  private final DimensionConverter converter;
  private volatile ScheduledExecutorService scheduledExecutor;
  private final ObjectMapper mapper;
  private final LinkedBlockingQueue<GangliaEvent> eventsQueue;
  private AtomicLong countLostEvents = new AtomicLong(0);
  private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("GangliaEmitter-%s")
      .build()); // Thread pool of two in order to schedule flush runnable

  private Map<String, GangliaMetric> metricMap = new HashMap<>();

  public GangliaEmitter(GangliaEmitterConfig config, ObjectMapper mapper)
  {
    this.config = config;
    this.registry = new MetricRegistry();
    this.converter = new DimensionConverter();
    this.mapper = mapper;
    this.eventsQueue = new LinkedBlockingQueue(config.getMaxQueueSize());
  }

  @Override
  public void start()
  {
    try {
      String spoof = InetAddress.getLocalHost().getHostAddress() + ":" + InetAddress.getLocalHost().getCanonicalHostName().split("\\.")[0];
      log.info("Starting Ganglia Emitter");

      this.ganglia = new GMetric(
          config.getHostname(),
          config.getPort(),
          GMetric.UDPAddressingMode.UNICAST,
          0,
          true,
          null,
          spoof
      );

      //load config
      scheduledExecutor = Execs.scheduledSingleThreaded("ganglia_metrics_load__scheduled_%d");
      scheduledExecutor.scheduleAtFixedRate(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                metricMap = readMap(mapper, config.getDimensionMapPath());
              }
              catch (Exception e) {
                log.error(e.toString());
              }
            }
          }, 0L, config.getLoadPeriod(), TimeUnit.MILLISECONDS
      );

      exec.scheduleAtFixedRate(
          new ConsumerRunnable(),
          config.getFlushPeriod(),
          config.getFlushPeriod(),
          TimeUnit.MILLISECONDS
      );
      log.info("constructed GMetric. host=[%s] port=[%s]", config.getHostname(), config.getPort());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
      ImmutableList.Builder<String> nameBuilder = new ImmutableList.Builder<>();
      if (config.getIncludeHost()) {
        nameBuilder.add(host);
      }
      nameBuilder.add(service);
      GangliaMetric gangliaMetric = converter.addFilteredUserDims(service, metric, userDims, nameBuilder, metricMap);
      if (gangliaMetric == null) {
        log.debug("Metric=[%s] has no ganglia type mapping", gangliaMetric);
        return;
      }
      nameBuilder.add(metric);
      String fullName = Joiner.on(config.getSeparator())
                              .join(nameBuilder.build())
                              .replaceAll(DRUID_METRIC_SEPARATOR, config.getSeparator())
                              .replaceAll(STATSD_SEPARATOR, config.getSeparator())
                              .replaceAll(BLANK, config.getBlankHolder());
      fullName = sanitize(fullName);
      GangliaEvent gangliaEvent = new GangliaEvent(fullName, metricEvent.getValue(), service.replaceAll("/", "_"));
      try {
        final boolean successful = eventsQueue.offer(
            gangliaEvent,
            config.getEmitWaitTime(),
            TimeUnit.MILLISECONDS
        );
        if (!successful) {
          if (countLostEvents.getAndIncrement() % 1000 == 0) {
            log.error(
                "Lost total of [%s] events because of emitter queue is full. Please increase the capacity or/and the consumer frequency",
                countLostEvents.get()
            );
          }
        }
      }
      catch (InterruptedException e) {
        log.error(e, "got interrupted with message [%s]", e.getMessage());
        Thread.currentThread().interrupt();
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

  private String sanitize(String toClean)
  {
    return toClean.replaceAll("[^A-Za-z0-9:_.-]+", "");
  }

  private Map<String, GangliaMetric> readMap(ObjectMapper mapper, String dimensionMapPath)
  {
    try {
      InputStream is;
      if (Strings.isNullOrEmpty(dimensionMapPath)) {
        log.info("Using default ganglia metric dimension and types");
        is = this.getClass().getClassLoader().getResourceAsStream("defaultMetricDimensions.json");
      } else {
        log.info("Using ganglia metric dimensions at types at [%s]", dimensionMapPath);
        is = new FileInputStream(new File(dimensionMapPath));
      }
      return mapper.reader(new TypeReference<Map<String, GangliaMetric>>()
      {
      }).readValue(is);
    }
    catch (IOException e) {
      e.printStackTrace();
      throw new ISE(e, "Failed to parse ganglia metric dimensions and types");
    }
  }

  private class ConsumerRunnable implements Runnable
  {
    @Override
    public void run()
    {
      log.info("eventQueu size :" + eventsQueue.size());
      while (eventsQueue.size() > 0 && !exec.isShutdown()) {
        try {
          final GangliaEvent gangliaEvent = eventsQueue.poll(
              config.getWaitForEventTime(),
              TimeUnit.MILLISECONDS
          );
          if (gangliaEvent != null) {
            try {
              ganglia.announce(
                  gangliaEvent.getName(),
                  gangliaEvent.getValue().longValue(),
                  gangliaEvent.getService()
              );
            }
            catch (GangliaException e) {
              log.error(e, "Dropping event [%s]. Unable to send to Ganglia.", gangliaEvent);
            }
          }
        }
        catch (InterruptedException e) {
          log.debug(e, e.getMessage());
          log.info("Ganglia connection broken for: %s", e.getMessage());
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
  }
}
