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

package org.apache.druid.emitter.opentsdb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import org.apache.druid.java.util.common.logger.Logger;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class OpentsdbSender
{
  /**
   * @see <a href="http://opentsdb.net/docs/build/html/api_http/put.html">Opentsdb - /api/put</a>
   */
  private static final String PATH = "/api/put";
  private static final Logger log = new Logger(OpentsdbSender.class);
  private static final long FLUSH_TIMEOUT = 60000; // default flush wait 1 min

  private final AtomicLong countLostEvents = new AtomicLong(0);
  private final int flushThreshold;
  private final BlockingQueue<OpentsdbEvent> eventQueue;
  private final ScheduledExecutorService scheduler;
  private final EventConsumer eventConsumer;
  private final long consumeDelay;
  private final Client client;
  private final WebResource webResource;

  public OpentsdbSender(
      String host,
      int port,
      int connectionTimeout,
      int readTimeout,
      int flushThreshold,
      int maxQueueSize,
      long consumeDelay
  )
  {
    this.flushThreshold = flushThreshold;
    this.consumeDelay = consumeDelay;
    eventQueue = new ArrayBlockingQueue<>(maxQueueSize);
    scheduler = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("OpentsdbEventSender-%s")
        .build());
    eventConsumer = new EventConsumer();

    client = Client.create();
    client.setConnectTimeout(connectionTimeout);
    client.setReadTimeout(readTimeout);
    webResource = client.resource("http://" + host + ":" + port + PATH);
  }

  public void enqueue(OpentsdbEvent event)
  {
    if (!eventQueue.offer(event)) {
      if (countLostEvents.getAndIncrement() % 1000 == 0) {
        log.error(
            "Lost total of [%s] events because of emitter queue is full. Please increase the capacity.",
            countLostEvents.get()
        );
      }
    }
  }

  public void start()
  {
    scheduler.scheduleWithFixedDelay(
        eventConsumer,
        consumeDelay,
        consumeDelay,
        TimeUnit.MILLISECONDS
    );
  }

  public void flush()
  {
    try {
      EventConsumer flushConsumer = new EventConsumer();
      Future future = scheduler.schedule(flushConsumer, 0, TimeUnit.MILLISECONDS);
      future.get(FLUSH_TIMEOUT, TimeUnit.MILLISECONDS);
      // send remaining events which size may less than flushThreshold
      eventConsumer.sendEvents();
      flushConsumer.sendEvents();
    }
    catch (Exception e) {
      log.warn(e, e.getMessage());
    }
  }

  public void close()
  {
    flush();
    client.destroy();
    scheduler.shutdown();
  }

  private class EventConsumer implements Runnable
  {
    private final List<OpentsdbEvent> events;

    public EventConsumer()
    {
      events = new ArrayList<>(flushThreshold);
    }

    @Override
    public void run()
    {
      while (!eventQueue.isEmpty() && !scheduler.isShutdown()) {
        OpentsdbEvent event = eventQueue.poll();
        events.add(event);
        if (events.size() >= flushThreshold) {
          sendEvents();
        }
      }
    }

    public void sendEvents()
    {
      if (!events.isEmpty()) {
        try {
          webResource.entity(events, MediaType.APPLICATION_JSON_TYPE).post();
        }
        catch (Exception e) {
          log.error(e, "error occurred when sending metrics to opentsdb server.");
        }
        finally {
          events.clear();
        }
      }
    }
  }

  @VisibleForTesting
  WebResource getWebResource()
  {
    return webResource;
  }
}
