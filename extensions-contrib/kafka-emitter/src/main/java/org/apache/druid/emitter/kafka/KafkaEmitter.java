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
import org.apache.druid.emitter.kafka.MemoryBoundLinkedBlockingQueue.ObjectContainer;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.log.RequestLogEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaEmitter implements Emitter
{
  private static Logger log = new Logger(KafkaEmitter.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int QUEUES_SIZE = 4; // metric, alert, request, invalid props to: mkuthan in PR #6493 (for all new additions)
  private final AtomicLong metricLost;
  private final AtomicLong alertLost;
  private final AtomicLong requestLost;
  private final AtomicLong invalidLost;

  private final KafkaEmitterConfig config;
  private final Producer<String, String> producer;
  private final Callback producerCallback;
  private final ObjectMapper jsonMapper;
  private final MemoryBoundLinkedBlockingQueue<String> metricQueue;
  private final MemoryBoundLinkedBlockingQueue<String> alertQueue;
  private final MemoryBoundLinkedBlockingQueue<String> requestQueue;
  private final ScheduledExecutorService scheduler;

  public KafkaEmitter(KafkaEmitterConfig config, ObjectMapper jsonMapper)
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.producer = setKafkaProducer();
    this.producerCallback = setProducerCallback();
    // same with kafka producer's buffer.memory
    long queueMemoryBound = Long.parseLong(this.config.getKafkaProducerConfig()
                                                      .getOrDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"));
    this.metricQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.alertQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.requestQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.scheduler = Executors.newScheduledThreadPool(QUEUES_SIZE);
    this.metricLost = new AtomicLong(0L);
    this.alertLost = new AtomicLong(0L);
    this.requestLost = new AtomicLong(0L);
    this.invalidLost = new AtomicLong(0L);
  }

  private Callback setProducerCallback()
  {
    return (recordMetadata, e) -> {
      if (e != null) {
        log.debug("Event send failed [%s]", e.getMessage());
        if (recordMetadata.topic().equals(config.getMetricTopic())) {
          metricLost.incrementAndGet();
        } else if (recordMetadata.topic().equals(config.getAlertTopic())) {
          alertLost.incrementAndGet();
        } else if (recordMetadata.topic().equals(config.getRequestTopic())) {
          requestLost.incrementAndGet();
        } else {
          invalidLost.incrementAndGet();
        }
      }
    };
  }

  private Producer<String, String> setKafkaProducer()
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

      return new KafkaProducer<>(props);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  @LifecycleStart
  public void start()
  {
    scheduler.scheduleWithFixedDelay(this::sendMetricToKafka, 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(this::sendAlertToKafka, 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(this::sendRequestToKafka, 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(() -> {
      log.info("Message lost counter: metricLost=[%d], alertLost=[%d], requestLost=[%d], invalidLost=[%d]",
               metricLost.get(), alertLost.get(), requestLost.get(), invalidLost.get());
    }, 5, 5, TimeUnit.MINUTES);
    log.info("Starting Kafka Emitter.");
  }

  private void sendMetricToKafka()
  {
    sendToKafka(config.getMetricTopic(), metricQueue);
  }

  private void sendAlertToKafka()
  {
    sendToKafka(config.getAlertTopic(), alertQueue);
  }

  private void sendRequestToKafka()
  {
    sendToKafka(config.getRequestTopic(), requestQueue);
  }

  private void sendToKafka(final String topic, MemoryBoundLinkedBlockingQueue<String> recordQueue)
  {
    ObjectContainer<String> objectToSend;
    try {
      while (true) {
        objectToSend = recordQueue.take();
        producer.send(new ProducerRecord<>(topic, objectToSend.getData()), producerCallback);
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Failed to take record from queue!");
    }
  }

  @Override
  public void emit(final Event event)
  {
    if (event != null) {
      final Map<String, Object> eventMap = jsonMapper.convertValue(event, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);

      if (config.getClusterName() != null) {
        eventMap.put("clusterName", config.getClusterName());
      }

      try {
        String resultJson = jsonMapper.writeValueAsString(eventMap);

        ObjectContainer<String> objectContainer = new ObjectContainer<>(
            resultJson,
            StringUtils.toUtf8(resultJson).length
        );
        if (event instanceof ServiceMetricEvent) {
          if (!metricQueue.offer(objectContainer)) {
            metricLost.incrementAndGet();
          }
        } else if (event instanceof AlertEvent) {
          if (!alertQueue.offer(objectContainer)) {
            alertLost.incrementAndGet();
          }
        } else if (event instanceof RequestLogEvent) {
          if (!requestQueue.offer(objectContainer)) {
            requestLost.incrementAndGet();
          }
        } else {
          invalidLost.incrementAndGet();
        }
      }
      catch (JsonProcessingException e) {
        invalidLost.incrementAndGet();
      }
    }
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
}
