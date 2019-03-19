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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.utils.RandomIdUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaRecordSupplier implements RecordSupplier<Integer, Long>
{
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final Map<String, Object> consumerProperties;
  private final ObjectMapper sortingMapper;
  private boolean closed;

  public KafkaRecordSupplier(
      Map<String, Object> consumerProperties,
      ObjectMapper sortingMapper
  )
  {
    this.consumerProperties = consumerProperties;
    this.sortingMapper = sortingMapper;
    this.consumer = getKafkaConsumer();
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    consumer.assign(streamPartitions
                        .stream()
                        .map(x -> new TopicPartition(x.getStream(), x.getPartitionId()))
                        .collect(Collectors.toSet()));
  }

  @Override
  public void seek(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    consumer.seek(new TopicPartition(partition.getStream(), partition.getPartitionId()), sequenceNumber);
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> partitions)
  {
    consumer.seekToBeginning(partitions
                                 .stream()
                                 .map(e -> new TopicPartition(e.getStream(), e.getPartitionId()))
                                 .collect(Collectors.toList()));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> partitions)
  {
    consumer.seekToEnd(partitions
                           .stream()
                           .map(e -> new TopicPartition(e.getStream(), e.getPartitionId()))
                           .collect(Collectors.toList()));
  }

  @Override
  public Set<StreamPartition<Integer>> getAssignment()
  {
    Set<TopicPartition> topicPartitions = consumer.assignment();
    return topicPartitions
        .stream()
        .map(e -> new StreamPartition<>(e.topic(), e.partition()))
        .collect(Collectors.toSet());
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<Integer, Long>> poll(long timeout)
  {
    List<OrderedPartitionableRecord<Integer, Long>> polledRecords = new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(timeout))) {
      polledRecords.add(new OrderedPartitionableRecord<>(
          record.topic(),
          record.partition(),
          record.offset(),
          record.value() == null ? null : ImmutableList.of(record.value())
      ));
    }
    return polledRecords;
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    Long currPos = consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
    seekToLatest(Collections.singleton(partition));
    Long nextPos = consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    Long currPos = consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
    seekToEarliest(Collections.singleton(partition));
    Long nextPos = consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getPosition(StreamPartition<Integer> partition)
  {
    return consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
  }

  @Override
  public Set<Integer> getPartitionIds(String stream)
  {
    List<PartitionInfo> partitions = consumer.partitionsFor(stream);
    if (partitions == null) {
      throw new ISE("Topic [%s] is not found in KafkaConsumer's list of topics", stream);
    }
    return partitions.stream().map(PartitionInfo::partition).collect(Collectors.toSet());
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    consumer.close();
  }

  public static void addConsumerPropertiesFromConfig(
      Properties properties,
      ObjectMapper configMapper,
      Map<String, Object> consumerProperties
  )
  {
    // Extract passwords before SSL connection to Kafka
    for (Map.Entry<String, Object> entry : consumerProperties.entrySet()) {
      String propertyKey = entry.getKey();
      if (propertyKey.equals(KafkaSupervisorIOConfig.TRUST_STORE_PASSWORD_KEY)
          || propertyKey.equals(KafkaSupervisorIOConfig.KEY_STORE_PASSWORD_KEY)
          || propertyKey.equals(KafkaSupervisorIOConfig.KEY_PASSWORD_KEY)) {
        PasswordProvider configPasswordProvider = configMapper.convertValue(
            entry.getValue(),
            PasswordProvider.class
        );
        properties.setProperty(propertyKey, configPasswordProvider.getPassword());
      } else {
        properties.setProperty(propertyKey, String.valueOf(entry.getValue()));
      }
    }
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer()
  {
    final Properties props = new Properties();

    props.setProperty("metadata.max.age.ms", "10000");
    props.setProperty("group.id", StringUtils.format("kafka-supervisor-%s", RandomIdUtils.getRandomId()));

    addConsumerPropertiesFromConfig(props, sortingMapper, consumerProperties);

    props.setProperty("enable.auto.commit", "false");

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

}
