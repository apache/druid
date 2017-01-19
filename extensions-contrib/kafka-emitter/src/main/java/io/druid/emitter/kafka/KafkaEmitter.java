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

package io.druid.emitter.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;

import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaEmitter implements Emitter {
  private static Logger log = new Logger(KafkaEmitter.class);

  private final static String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  private final static String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

  private final KafkaEmitterConfig config;
  private final Producer<String, String> producer;
  private final ObjectMapper jsonMapper;


  public KafkaEmitter(KafkaEmitterConfig config, ObjectMapper jsonMapper) {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.producer = getKafkaProducer(config);

  }

  private Producer<String, String> getKafkaProducer(KafkaEmitterConfig config) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIALIZER);

    return new KafkaProducer<>(props);
  }

  @Override
  @LifecycleStart
  public void start() {
    log.info("Starting Kafka Emitter.");
  }

  @Override
  public void emit(Event event) {
    if(event instanceof ServiceMetricEvent) {
      if(event == null) {
        return;
      }
    }
    try {
      TypeReference<Map<String,String>> typeRef = new TypeReference<Map<String,String>>() {};
      HashMap<String, String> result = jsonMapper.readValue(jsonMapper.writeValueAsString(event), typeRef);
      result.put("clustername", config.getClusterName());
      producer.send(new ProducerRecord<String, String>(config.getTopic(), jsonMapper.writeValueAsString(result)));
    } catch (Exception e) {
      log.warn(e, "Failed to generate json");
    }
  }

  @Override
  public void flush() throws IOException {
    producer.flush();
  }

  @Override
  @LifecycleStop
  public void close() throws IOException {
    producer.close();
  }
}
