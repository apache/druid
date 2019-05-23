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

package org.apache.druid.emitter.graphite;

import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteSender;
import com.codahale.metrics.graphite.PickledGraphite;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.log.RequestLogEvent;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;


public class GraphiteEmitter implements Emitter
{
  private static Logger log = new Logger(GraphiteEmitter.class);

  private final DruidToGraphiteEventConverter graphiteEventConverter;
  private final GraphiteEmitterConfig graphiteEmitterConfig;
  private final List<Emitter> alertEmitters;
  private final List<Emitter> requestLogEmitters;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final LinkedBlockingQueue<GraphiteEvent> eventsQueue;
  private static final long FLUSH_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(1); // default flush wait 1 min
  private static final Pattern DOT_OR_WHITESPACE_PATTERN = Pattern.compile("[\\s]+|[.]+");
  private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("GraphiteEmitter-%s")
      .build()); // Thread pool of two in order to schedule flush runnable
  private AtomicLong countLostEvents = new AtomicLong(0);

  public GraphiteEmitter(
      GraphiteEmitterConfig graphiteEmitterConfig,
      List<Emitter> alertEmitters,
      List<Emitter> requestLogEmitters
  )
  {
    this.alertEmitters = alertEmitters;
    this.requestLogEmitters = requestLogEmitters;
    this.graphiteEmitterConfig = graphiteEmitterConfig;
    this.graphiteEventConverter = graphiteEmitterConfig.getDruidToGraphiteEventConverter();
    this.eventsQueue = new LinkedBlockingQueue(graphiteEmitterConfig.getMaxQueueSize());
  }

  @Override
  public void start()
  {
    log.info("Starting Graphite Emitter.");
    synchronized (started) {
      if (!started.get()) {
        exec.scheduleAtFixedRate(
            new ConsumerRunnable(),
            graphiteEmitterConfig.getFlushPeriod(),
            graphiteEmitterConfig.getFlushPeriod(),
            TimeUnit.MILLISECONDS
        );
        started.set(true);
      }
    }
  }


  @Override
  public void emit(Event event)
  {
    if (!started.get()) {
      throw new ISE("WTF emit was called while service is not started yet");
    }
    if (event instanceof ServiceMetricEvent) {
      final GraphiteEvent graphiteEvent = graphiteEventConverter.druidEventToGraphite((ServiceMetricEvent) event);
      if (graphiteEvent == null) {
        return;
      }
      try {
        final boolean isSuccessful = eventsQueue.offer(
            graphiteEvent,
            graphiteEmitterConfig.getEmitWaitTime(),
            TimeUnit.MILLISECONDS
        );
        if (!isSuccessful) {
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
    } else if (event instanceof RequestLogEvent) {
      for (Emitter emitter : requestLogEmitters) {
        emitter.emit(event);
      }
    } else if (!alertEmitters.isEmpty() && event instanceof AlertEvent) {
      for (Emitter emitter : alertEmitters) {
        emitter.emit(event);
      }
    } else if (event instanceof AlertEvent) {
      AlertEvent alertEvent = (AlertEvent) event;
      log.error(
          "The following alert is dropped, description is [%s], severity is [%s]",
          alertEvent.getDescription(), alertEvent.getSeverity()
      );
    } else {
      log.error("unknown event type [%s]", event.getClass());
    }
  }

  private class ConsumerRunnable implements Runnable
  {
    private final GraphiteSender graphite;

    public ConsumerRunnable()
    {
      if (graphiteEmitterConfig.getProtocol().equals(GraphiteEmitterConfig.PLAINTEXT_PROTOCOL)) {
        graphite = new Graphite(
          graphiteEmitterConfig.getHostname(),
          graphiteEmitterConfig.getPort()
        );
      } else {
        graphite = new PickledGraphite(
          graphiteEmitterConfig.getHostname(),
          graphiteEmitterConfig.getPort(),
          graphiteEmitterConfig.getBatchSize()
        );
      }
      log.info("Using %s protocol.", graphiteEmitterConfig.getProtocol());
    }

    @Override
    public void run()
    {
      try {
        if (!graphite.isConnected()) {
          log.info("trying to connect to graphite server");
          graphite.connect();
        }
        while (eventsQueue.size() > 0 && !exec.isShutdown()) {
          try {
            final GraphiteEvent graphiteEvent = eventsQueue.poll(
                graphiteEmitterConfig.getWaitForEventTime(),
                TimeUnit.MILLISECONDS
            );
            if (graphiteEvent != null) {
              log.debug(
                  "sent [%s] with value [%s] and time [%s]",
                  graphiteEvent.getEventPath(),
                  graphiteEvent.getValue(),
                  graphiteEvent.getTimestamp()
              );
              graphite.send(
                  graphiteEvent.getEventPath(),
                  graphiteEvent.getValue(),
                  graphiteEvent.getTimestamp()
              );
            }
          }
          catch (InterruptedException | IOException e) {
            log.error(e, e.getMessage());
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
              break;
            } else if (e instanceof SocketException) {
              // This is antagonistic to general Closeable contract in Java,
              // it is needed to allow re-connection in case of the socket is closed due long period of inactivity
              graphite.close();
              log.warn("Trying to re-connect to graphite server");
              graphite.connect();
            }
          }
        }
      }
      catch (Exception e) {
        log.error(e, e.getMessage());
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public void flush()
  {
    if (started.get()) {
      Future future = exec.schedule(new ConsumerRunnable(), 0, TimeUnit.MILLISECONDS);
      try {
        future.get(FLUSH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException | ExecutionException | TimeoutException e) {
        if (e instanceof InterruptedException) {
          throw new RuntimeException("interrupted flushing elements from queue", e);
        }
        log.error(e, e.getMessage());
      }
    }

  }

  @Override
  public void close()
  {
    flush();
    started.set(false);
    exec.shutdown();
  }

  protected static String sanitize(String namespace)
  {
    return sanitize(namespace, false);
  }

  protected static String sanitize(String namespace, Boolean replaceSlashToDot)
  {
    String sanitizedNamespace = DOT_OR_WHITESPACE_PATTERN.matcher(namespace).replaceAll("_");
    if (replaceSlashToDot) {
      sanitizedNamespace = sanitizedNamespace.replace('/', '.');
    }
    return sanitizedNamespace;
  }
}
