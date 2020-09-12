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

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaEventWriter implements StreamEventWriter
{
  private final KafkaProducer<String, byte[]> producer;
  private final boolean txnEnabled;
  private final List<Future<RecordMetadata>> pendingWriteRecords = new ArrayList<>();

  public KafkaEventWriter(IntegrationTestingConfig config, boolean txnEnabled)
  {
    Properties properties = new Properties();
    KafkaUtil.addPropertiesFromTestConfig(config, properties);
    properties.setProperty("bootstrap.servers", config.getKafkaHost());
    properties.setProperty("acks", "all");
    properties.setProperty("retries", "3");
    properties.setProperty("key.serializer", ByteArraySerializer.class.getName());
    properties.setProperty("value.serializer", ByteArraySerializer.class.getName());
    this.txnEnabled = txnEnabled;
    if (txnEnabled) {
      properties.setProperty("enable.idempotence", "true");
      properties.setProperty("transactional.id", IdUtils.getRandomId());
    }
    this.producer = new KafkaProducer<>(
        properties,
        new StringSerializer(),
        new ByteArraySerializer()
    );
    if (txnEnabled) {
      producer.initTransactions();
    }
  }

  @Override
  public boolean supportTransaction()
  {
    return true;
  }

  @Override
  public boolean isTransactionEnabled()
  {
    return txnEnabled;
  }

  @Override
  public void initTransaction()
  {
    if (txnEnabled) {
      producer.beginTransaction();
    } else {
      throw new IllegalStateException("Kafka writer was initialized with transaction disabled");
    }
  }

  @Override
  public void commitTransaction()
  {
    if (txnEnabled) {
      producer.commitTransaction();
    } else {
      throw new IllegalStateException("Kafka writer was initialized with transaction disabled");
    }
  }

  @Override
  public void write(String topic, byte[] event)
  {
    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, event));
    pendingWriteRecords.add(future);
  }

  @Override
  public void close()
  {
    flush();
    producer.close();
  }

  @Override
  public void flush()
  {
    Exception e = null;
    for (Future<RecordMetadata> future : pendingWriteRecords) {
      try {
        future.get();
      }
      catch (InterruptedException | ExecutionException ex) {
        if (ex instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        if (e == null) {
          e = ex;
        } else {
          e.addSuppressed(ex);
        }
      }
    }
    pendingWriteRecords.clear();
    if (e != null) {
      throw new RuntimeException(e);
    }
  }
}
