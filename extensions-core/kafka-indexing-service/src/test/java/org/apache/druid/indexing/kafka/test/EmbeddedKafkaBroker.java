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

package org.apache.druid.indexing.kafka.test;

import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.kafka.KafkaContainer;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Embedded Kafka broker for unit tests, backed by Testcontainers {@link KafkaContainer}.
 */
public class EmbeddedKafkaBroker implements Closeable
{
  private static final String KAFKA_IMAGE =
      System.getProperty("druid.testing.kafka.image", "apache/kafka:4.2.0");

  private final KafkaContainer container;

  public EmbeddedKafkaBroker()
  {
    this(Map.of());
  }

  /**
   * @param brokerEnv extra env vars forwarded to the container; the apache/kafka
   *                  image maps {@code KAFKA_FOO_BAR} to broker config {@code foo.bar}.
   */
  public EmbeddedKafkaBroker(Map<String, String> brokerEnv)
  {
    this.container = new KafkaContainer(KAFKA_IMAGE);
    if (brokerEnv != null && !brokerEnv.isEmpty()) {
      container.withEnv(brokerEnv);
    }
  }

  public void start()
  {
    container.start();
  }

  public String getBootstrapServerUrl()
  {
    return container.getBootstrapServers();
  }

  public int getPort()
  {
    final String url = getBootstrapServerUrl();
    final int colon = url.lastIndexOf(':');
    return Integer.parseInt(url.substring(colon + 1));
  }

  public Map<String, Object> producerProperties()
  {
    final Map<String, Object> props = new HashMap<>();
    commonClientProperties(props);
    props.put("key.serializer", ByteArraySerializer.class.getName());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("acks", "all");
    props.put("enable.idempotence", "true");
    props.put("transactional.id", String.valueOf(ThreadLocalRandom.current().nextInt()));
    return props;
  }

  public KafkaProducer<byte[], byte[]> newProducer()
  {
    return new KafkaProducer<>(producerProperties());
  }

  public Map<String, Object> consumerProperties()
  {
    final Map<String, Object> props = new HashMap<>(KafkaConsumerConfigs.getConsumerProperties());
    props.put("bootstrap.servers", getBootstrapServerUrl());
    return props;
  }

  public Admin newAdminClient()
  {
    final Map<String, Object> props = new HashMap<>();
    commonClientProperties(props);
    return Admin.create(props);
  }

  private void commonClientProperties(Map<String, Object> props)
  {
    props.put("bootstrap.servers", getBootstrapServerUrl());
  }

  @Override
  public void close()
  {
    container.stop();
  }
}
