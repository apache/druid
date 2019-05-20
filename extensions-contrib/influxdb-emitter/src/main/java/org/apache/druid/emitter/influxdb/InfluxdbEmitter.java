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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;


public class InfluxdbEmitter implements Emitter
{

  private static final Logger log = new Logger(InfluxdbEmitter.class);
  private HttpClient influxdbClient;
  private final InfluxdbEmitterConfig influxdbEmitterConfig;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("InfluxdbEmitter-%s")
      .build());

  private final ImmutableSet dimensionWhiteList;

  private final LinkedBlockingQueue<ServiceMetricEvent> eventsQueue;

  public InfluxdbEmitter(InfluxdbEmitterConfig influxdbEmitterConfig)
  {
    this.influxdbEmitterConfig = influxdbEmitterConfig;
    this.influxdbClient = HttpClientBuilder.create().build();
    this.eventsQueue = new LinkedBlockingQueue<>(influxdbEmitterConfig.getMaxQueueSize());

    this.dimensionWhiteList = ImmutableSet.of(
        "dataSource",
        "type",
        "numMetrics",
        "numDimensions",
        "threshold",
        "dimension",
        "taskType",
        "taskStatus",
        "tier"
    );

    log.info("constructing influxdb emitter");
  }

  @Override
  public void start()
  {
    synchronized (started) {
      if (!started.get()) {
        exec.scheduleAtFixedRate(
            new ConsumerRunnable(),
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
        log.error(exception.toString());
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
      log.info(ex.toString());
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

    String payload = "druid_" + parts[0] + ",";

    payload += "service=" + getValue("service", event)
               + ((parts.length == 2) ? "" : ",metric=druid_" + metric)
               + ",hostname=" + getValue("host", event).split(":")[0];


    ImmutableSet<String> dimNames = ImmutableSet.copyOf(event.getUserDims().keySet());
    for (String dimName : dimNames) {
      if (this.dimensionWhiteList.contains(dimName)) {
        payload += "," + dimName + "=" + sanitize(String.valueOf(event.getUserDims().get(dimName)));
      }
    }

    payload += " druid_" + parts[parts.length - 1] + "=" + getValue("value", event);

    return payload + " " + event.getCreatedTime().getMillis() * 1000000 + '\n';
  }

  protected static String sanitize(String namespace)
  {
    Pattern DOT_OR_WHITESPACE = Pattern.compile("[\\s]+|[.]+");
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
    log.info("Closing emitter org.apache.druid.emitter.influxdb.InfluxdbEmitter");
    started.set(false);
    exec.shutdown();
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

  private class ConsumerRunnable implements Runnable
  {
    @Override
    public void run()
    {
      transformAndSendToInfluxdb(eventsQueue);
    }
  }
}
