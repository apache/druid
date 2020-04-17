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

package org.apache.druid.testing.utils;

import org.apache.druid.indexer.TaskIdUtils;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaEventWriter implements StreamEventWriter
{
  private static final String TEST_PROPERTY_PREFIX = "kafka.test.property.";
  private KafkaProducer<String, String> producer;
  private boolean txnEnabled;
  private List<Future<RecordMetadata>> pendingWriteRecords = new ArrayList<>();

  public KafkaEventWriter(IntegrationTestingConfig config, boolean txnEnabled)
  {
    Properties properties = new Properties();
    addFilteredProperties(config, properties);
    properties.setProperty("bootstrap.servers", config.getKafkaHost());
    properties.setProperty("acks", "all");
    properties.setProperty("retries", "3");
    properties.setProperty("key.serializer", ByteArraySerializer.class.getName());
    properties.setProperty("value.serializer", ByteArraySerializer.class.getName());
    this.txnEnabled = txnEnabled;
    if (txnEnabled) {
      properties.setProperty("enable.idempotence", "true");
      properties.setProperty("transactional.id", TaskIdUtils.getRandomId());
    }
    this.producer = new KafkaProducer<>(
        properties,
        new StringSerializer(),
        new StringSerializer()
    );
  }

  @Override
  public void write(String topic, List<String> events) throws Exception
  {
    if (txnEnabled) {
      producer.initTransactions();
      producer.beginTransaction();
    }

    for (String event : events)
    {
      write(topic, event);
    }

    if (txnEnabled) {
      producer.commitTransaction();
    }
  }

  /**
   * This method does not handle transaction. For transaction functionality use
   * {@link #write(String, List<String) write} method
   */
  @Override
  public void write(String topic, String event) throws Exception
  {
    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, event));
    pendingWriteRecords.add(future);
  }

  @Override
  public void shutdown()
  {
    producer.close();
  }

  @Override
  public void flush() throws Exception
  {
    for (Future<RecordMetadata> future : pendingWriteRecords) {
      future.get();
    }
    pendingWriteRecords.clear();
  }

  private void addFilteredProperties(IntegrationTestingConfig config, Properties properties)
  {
    for (Map.Entry<String, String> entry : config.getProperties().entrySet()) {
      if (entry.getKey().startsWith(TEST_PROPERTY_PREFIX)) {
        properties.setProperty(entry.getKey().substring(TEST_PROPERTY_PREFIX.length()), entry.getValue());
      }
    }
  }
}
