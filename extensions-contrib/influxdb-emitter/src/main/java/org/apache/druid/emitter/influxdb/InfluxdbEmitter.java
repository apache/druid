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

package org.apache.druid.emitter.influxdb;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;


public class InfluxdbEmitter implements Emitter
{

  private static final Logger log = new Logger(InfluxdbEmitter.class);
  private final HttpClient influxdbClient;
  private final InfluxdbEmitterConfig influxdbEmitterConfig;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final ScheduledExecutorService exec = ScheduledExecutors.fixed(1, "InfluxdbEmitter-%s");
  private final ImmutableSet dimensionWhiteList;
  private final LinkedBlockingQueue<ServiceMetricEvent> eventsQueue;
  private static final Pattern DOT_OR_WHITESPACE = Pattern.compile("[\\s]+|[.]+");

  public InfluxdbEmitter(InfluxdbEmitterConfig influxdbEmitterConfig)
  {
    this.influxdbEmitterConfig = influxdbEmitterConfig;
    this.influxdbClient = HttpClientBuilder.create().build();
    this.eventsQueue = new LinkedBlockingQueue<>(influxdbEmitterConfig.getMaxQueueSize());
    this.dimensionWhiteList = influxdbEmitterConfig.getDimensionWhitelist();
    log.info("constructed influxdb emitter");
  }

  @Override
  public void start()
  {
    synchronized (started) {
      if (!started.get()) {
        exec.scheduleAtFixedRate(
            () -> transformAndSendToInfluxdb(eventsQueue),
            influxdbEmitterConfig.getFlushDelay(),
            influxdbEmitterConfig.getFlushPeriod(),
            TimeUnit.MILLISECONDS
        );
        started.set(true);
      }
    }
  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof ServiceMetricEvent) {
      ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
      try {
        eventsQueue.put(metricEvent);
      }
      catch (InterruptedException exception) {
        log.error(exception, "Failed to add metricEvent to events queue.");
        Thread.currentThread().interrupt();
      }
    }
  }

  public void postToInflux(String payload)
  {
    HttpPost post = new HttpPost(
        "http://" + influxdbEmitterConfig.getHostname()
        + ":" + influxdbEmitterConfig.getPort()
        + "/write?db=" + influxdbEmitterConfig.getDatabaseName()
        + "&u=" + influxdbEmitterConfig.getInfluxdbUserName()
        + "&p=" + influxdbEmitterConfig.getInfluxdbPassword()
    );

    post.setEntity(new StringEntity(payload, ContentType.DEFAULT_TEXT));
    post.setHeader("Content-Type", "application/x-www-form-urlencoded");

    try {
      influxdbClient.execute(post);
    }
    catch (IOException ex) {
      log.info(ex, "Failed to post events to InfluxDB.");
    }
    finally {
      post.releaseConnection();
    }
  }

  public String transformForInfluxSystems(ServiceMetricEvent event)
  {
    // split Druid metric on slashes and join middle parts (if any) with "_"
    String[] parts = getValue("metric", event).split("/");
    String metric = String.join(
        "_",
        Arrays.asList(
            Arrays.copyOfRange(
                parts,
                1,
                parts.length - 1
            )
        )
    );

    // measurement
    StringBuilder payload = new StringBuilder("druid_");
    payload.append(parts[0]);

    // tags
    StringBuilder tag = new StringBuilder(",service=");
    tag.append(getValue("service", event));
    String metricTag = parts.length == 2 ? "" : ",metric=druid_" + metric;
    tag.append(metricTag);
    tag.append(StringUtils.format(",hostname=%s", getValue("host", event).split(":")[0]));
    ImmutableSet<String> dimNames = ImmutableSet.copyOf(event.getUserDims().keySet());
    for (String dimName : dimNames) {
      if (this.dimensionWhiteList.contains(dimName)) {
        tag.append(StringUtils.format(",%1$s=%2$s", dimName, sanitize(String.valueOf(event.getUserDims().get(dimName)))));
      }
    }
    payload.append(tag);

    // fields
    payload.append(StringUtils.format(" druid_%1$s=%2$s", parts[parts.length - 1], getValue("value", event)));

    // timestamp
    payload.append(StringUtils.format(" %d\n", event.getCreatedTime().getMillis() * 1000000));

    return payload.toString();
  }

  private static String sanitize(String namespace)
  {
    return DOT_OR_WHITESPACE.matcher(namespace).replaceAll("_");
  }

  public String getValue(String key, ServiceMetricEvent event)
  {
    switch (key) {
      case "service":
        return event.getService();
      case "eventType":
        return event.getClass().getSimpleName();
      case "metric":
        return event.getMetric();
      case "feed":
        return event.getFeed();
      case "host":
        return event.getHost();
      case "value":
        return event.getValue().toString();
      default:
        return key;
    }
  }

  @Override
  public void flush() throws IOException
  {
    if (started.get()) {
      transformAndSendToInfluxdb(eventsQueue);
    }
  }

  @Override
  public void close() throws IOException
  {
    flush();
    log.info("Closing [%s]", this.getClass().getName());
    started.set(false);
    exec.shutdownNow();
  }

  public void transformAndSendToInfluxdb(LinkedBlockingQueue<ServiceMetricEvent> eventsQueue)
  {
    StringBuilder payload = new StringBuilder();
    int initialQueueSize = eventsQueue.size();
    for (int i = 0; i < initialQueueSize; i++) {
      payload.append(transformForInfluxSystems(eventsQueue.poll()));
    }
    postToInflux(payload.toString());
  }

}
