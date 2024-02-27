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

package org.apache.druid.emitter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.emitter.kafka.KafkaEmitterConfig.EventType;
import org.apache.druid.java.util.common.MemoryBoundLinkedBlockingQueue;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.SegmentMetadataEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.log.RequestLogEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaEmitter implements Emitter
{
  private static Logger log = new Logger(KafkaEmitter.class);

  private static final int DEFAULT_SEND_INTERVAL_SECONDS = 10;
  private static final int DEFAULT_SEND_LOST_INTERVAL_MINUTES = 5;
  private static final int DEFAULT_RETRIES = 3;
  private final AtomicLong metricLost;
  private final AtomicLong alertLost;
  private final AtomicLong requestLost;
  private final AtomicLong segmentMetadataLost;
  private final AtomicLong invalidLost;

  private final KafkaEmitterConfig config;
  private final Producer<String, String> producer;
  private final ObjectMapper jsonMapper;
  private final MemoryBoundLinkedBlockingQueue<String> metricQueue;
  private final MemoryBoundLinkedBlockingQueue<String> alertQueue;
  private final MemoryBoundLinkedBlockingQueue<String> requestQueue;
  private final MemoryBoundLinkedBlockingQueue<String> segmentMetadataQueue;
  private final ScheduledExecutorService scheduler;

  protected int sendInterval = DEFAULT_SEND_INTERVAL_SECONDS;

  public KafkaEmitter(KafkaEmitterConfig config, ObjectMapper jsonMapper)
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.producer = setKafkaProducer();
    // same with kafka producer's buffer.memory
    long queueMemoryBound = Long.parseLong(this.config.getKafkaProducerConfig()
                                                      .getOrDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"));
    this.metricQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.alertQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.requestQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.segmentMetadataQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    // need one thread per scheduled task. Scheduled tasks are per eventType and 1 for reporting the lost events
    int numOfThreads = config.getEventTypes().size() + 1;
    this.scheduler = Executors.newScheduledThreadPool(numOfThreads);
    this.metricLost = new AtomicLong(0L);
    this.alertLost = new AtomicLong(0L);
    this.requestLost = new AtomicLong(0L);
    this.segmentMetadataLost = new AtomicLong(0L);
    this.invalidLost = new AtomicLong(0L);
  }

  private Callback setProducerCallback(AtomicLong lostCounter)
  {
    return (recordMetadata, e) -> {
      if (e != null) {
        log.debug("Event send failed [%s]", e.getMessage());
        lostCounter.incrementAndGet();
      }
    };
  }

  @VisibleForTesting
  protected Producer<String, String> setKafkaProducer()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.RETRIES_CONFIG, DEFAULT_RETRIES);
      props.putAll(config.getKafkaProducerConfig());
      props.putAll(config.getKafkaProducerSecrets().getConfig());

      return new KafkaProducer<>(props);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  public void start()
  {
    Set<EventType> eventTypes = config.getEventTypes();
    if (eventTypes.contains(EventType.METRICS)) {
      scheduler.schedule(this::sendMetricToKafka, sendInterval, TimeUnit.SECONDS);
    }
    if (eventTypes.contains(EventType.ALERTS)) {
      scheduler.schedule(this::sendAlertToKafka, sendInterval, TimeUnit.SECONDS);
    }
    if (eventTypes.contains(EventType.REQUESTS)) {
      scheduler.schedule(this::sendRequestToKafka, sendInterval, TimeUnit.SECONDS);
    }
    if (eventTypes.contains(EventType.SEGMENT_METADATA)) {
      scheduler.schedule(this::sendSegmentMetadataToKafka, sendInterval, TimeUnit.SECONDS);
    }
    scheduler.scheduleWithFixedDelay(() -> {
      log.info("Message lost counter: metricLost=[%d], alertLost=[%d], requestLost=[%d], segmentMetadataLost=[%d], invalidLost=[%d]",
          metricLost.get(),
          alertLost.get(),
          requestLost.get(),
          segmentMetadataLost.get(),
          invalidLost.get()
      );
    }, DEFAULT_SEND_LOST_INTERVAL_MINUTES, DEFAULT_SEND_LOST_INTERVAL_MINUTES, TimeUnit.MINUTES);
    log.info("Starting Kafka Emitter.");
  }

  private void sendMetricToKafka()
  {
    sendToKafka(config.getMetricTopic(), metricQueue, setProducerCallback(metricLost));
  }

  private void sendAlertToKafka()
  {
    sendToKafka(config.getAlertTopic(), alertQueue, setProducerCallback(alertLost));
  }

  private void sendRequestToKafka()
  {
    sendToKafka(config.getRequestTopic(), requestQueue, setProducerCallback(requestLost));
  }

  private void sendSegmentMetadataToKafka()
  {
    sendToKafka(config.getSegmentMetadataTopic(), segmentMetadataQueue, setProducerCallback(segmentMetadataLost));
  }

  private void sendToKafka(final String topic, MemoryBoundLinkedBlockingQueue<String> recordQueue, Callback callback)
  {
    MemoryBoundLinkedBlockingQueue.ObjectContainer<String> objectToSend;
    try {
      while (true) {
        objectToSend = recordQueue.take();
        producer.send(new ProducerRecord<>(topic, objectToSend.getData()), callback);
      }
    }
    catch (Throwable e) {
      if (e instanceof InterruptedException && e.getMessage() == null) {
        log.info("Normal exit.");
        return;
      }
      log.warn(e, "Exception while getting record from queue or producer send, Events would not be emitted anymore.");
    }
  }

  @Override
  public void emit(final Event event)
  {
    if (event != null) {
      try {
        EventMap map = event.toMap();
        map = addExtraDimensionsToEvent(map);

        String resultJson = jsonMapper.writeValueAsString(map);

        MemoryBoundLinkedBlockingQueue.ObjectContainer<String> objectContainer = new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(
            resultJson,
            StringUtils.toUtf8(resultJson).length
        );

        Set<EventType> eventTypes = config.getEventTypes();
        if (event instanceof ServiceMetricEvent) {
          if (!eventTypes.contains(EventType.METRICS) || !metricQueue.offer(objectContainer)) {
            metricLost.incrementAndGet();
          }
        } else if (event instanceof AlertEvent) {
          if (!eventTypes.contains(EventType.ALERTS) || !alertQueue.offer(objectContainer)) {
            alertLost.incrementAndGet();
          }
        } else if (event instanceof RequestLogEvent) {
          if (!eventTypes.contains(EventType.REQUESTS) || !requestQueue.offer(objectContainer)) {
            requestLost.incrementAndGet();
          }
        } else if (event instanceof SegmentMetadataEvent) {
          if (!eventTypes.contains(EventType.SEGMENT_METADATA) || !segmentMetadataQueue.offer(objectContainer)) {
            segmentMetadataLost.incrementAndGet();
          }
        } else {
          invalidLost.incrementAndGet();
        }
      }
      catch (JsonProcessingException e) {
        invalidLost.incrementAndGet();
        log.warn(e, "Exception while serializing event");
      }
    }
  }

  private EventMap addExtraDimensionsToEvent(EventMap map)
  {
    if (config.getClusterName() != null || config.getExtraDimensions() != null) {
      EventMap.Builder eventMapBuilder = map.asBuilder();
      if (config.getClusterName() != null) {
        eventMapBuilder.put("clusterName", config.getClusterName());
      }
      if (config.getExtraDimensions() != null) {
        eventMapBuilder.putAll(config.getExtraDimensions());
      }
      map = eventMapBuilder.build();
    }
    return map;
  }

  @Override
  public void flush()
  {
    producer.flush();
  }

  @Override
  @LifecycleStop
  public void close()
  {
    scheduler.shutdownNow();
    producer.close();
  }

  public long getMetricLostCount()
  {
    return metricLost.get();
  }

  public long getAlertLostCount()
  {
    return alertLost.get();
  }

  public long getRequestLostCount()
  {
    return requestLost.get();
  }

  public long getInvalidLostCount()
  {
    return invalidLost.get();
  }

  public long getSegmentMetadataLostCount()
  {
    return segmentMetadataLost.get();
  }
}
