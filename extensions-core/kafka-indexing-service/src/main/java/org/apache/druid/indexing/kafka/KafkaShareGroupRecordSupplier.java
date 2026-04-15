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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.AcknowledgeType;
import org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wraps a {@link KafkaShareConsumer} to implement {@link AcknowledgingRecordSupplier}.
 *
 * The share consumer uses broker-managed offset tracking with explicit
 * acknowledgement. Records are delivered with acquisition locks; unacknowledged
 * records are redelivered after lock timeout.
 *
 * This supplier sets {@code group.id} to the share group name provided in config.
 */
public class KafkaShareGroupRecordSupplier
    implements AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>
{
  private static final Logger log = new Logger(KafkaShareGroupRecordSupplier.class);

  private final KafkaShareConsumer<byte[], byte[]> consumer;
  private boolean closed;

  public KafkaShareGroupRecordSupplier(
      Map<String, Object> consumerProperties,
      ObjectMapper sortingMapper,
      String groupId
  )
  {
    this(createShareConsumer(consumerProperties, sortingMapper, groupId));
  }

  @VisibleForTesting
  public KafkaShareGroupRecordSupplier(KafkaShareConsumer<byte[], byte[]> consumer)
  {
    this.consumer = consumer;
  }

  @Override
  public void subscribe(Set<String> topics)
  {
    consumer.subscribe(topics);
  }

  @Override
  public void unsubscribe()
  {
    consumer.unsubscribe();
  }

  @Override
  public Set<String> subscription()
  {
    return consumer.subscription();
  }

  @NotNull
  @Override
  public List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> poll(long timeoutMs)
  {
    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> polledRecords =
        new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(timeoutMs))) {
      polledRecords.add(new OrderedPartitionableRecord<>(
          record.topic(),
          new KafkaTopicPartition(true, record.topic(), record.partition()),
          record.offset(),
          record.value() == null ? null : ImmutableList.of(new KafkaRecordEntity(record)),
          record.timestamp()
      ));
    }
    return polledRecords;
  }

  @Override
  public void acknowledge(KafkaTopicPartition partitionId, Long offset)
  {
    acknowledge(partitionId, offset, AcknowledgeType.ACCEPT);
  }

  @Override
  public void acknowledge(KafkaTopicPartition partitionId, Long offset, AcknowledgeType type)
  {
    final String topic = partitionId.topic().orElseThrow(
        () -> new IllegalArgumentException("Cannot acknowledge record without topic")
    );
    consumer.acknowledge(
        new ConsumerRecord<>(topic, partitionId.partition(), offset, null, null),
        toKafkaAcknowledgeType(type)
    );
  }

  @Override
  public void acknowledge(
      Map<KafkaTopicPartition, Collection<Long>> offsets,
      AcknowledgeType type
  )
  {
    final org.apache.kafka.clients.consumer.AcknowledgeType kafkaType = toKafkaAcknowledgeType(type);
    for (Map.Entry<KafkaTopicPartition, Collection<Long>> entry : offsets.entrySet()) {
      final KafkaTopicPartition partition = entry.getKey();
      final String topic = partition.topic().orElseThrow(
          () -> new IllegalArgumentException("Cannot acknowledge record without topic")
      );
      for (Long offset : entry.getValue()) {
        consumer.acknowledge(
            new ConsumerRecord<>(topic, partition.partition(), offset, null, null),
            kafkaType
        );
      }
    }
  }

  @Override
  public Map<KafkaTopicPartition, Optional<Exception>> commitSync()
  {
    final Map<TopicIdPartition, Optional<KafkaException>> result = consumer.commitSync();
    final Map<KafkaTopicPartition, Optional<Exception>> mapped = new HashMap<>();
    for (Map.Entry<TopicIdPartition, Optional<KafkaException>> entry : result.entrySet()) {
      final TopicIdPartition tip = entry.getKey();
      mapped.put(
          new KafkaTopicPartition(true, tip.topic(), tip.partition()),
          entry.getValue().map(e -> (Exception) e)
      );
    }
    return mapped;
  }

  @Override
  public Set<KafkaTopicPartition> getPartitionIds(String stream)
  {
    // Share consumer does not expose partitionsFor; use admin client if needed.
    // For Phase 1, return empty set as the broker manages assignment.
    return Collections.emptySet();
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    try {
      consumer.close();
    }
    catch (Exception e) {
      log.warn(e, "Exception closing KafkaShareConsumer");
    }
  }

  @Nonnull
  private static org.apache.kafka.clients.consumer.AcknowledgeType toKafkaAcknowledgeType(AcknowledgeType type)
  {
    switch (type) {
      case ACCEPT:
        return org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT;
      case RELEASE:
        return org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE;
      case REJECT:
        return org.apache.kafka.clients.consumer.AcknowledgeType.REJECT;
      default:
        throw new IllegalArgumentException("Unknown acknowledge type: " + type);
    }
  }

  private static KafkaShareConsumer<byte[], byte[]> createShareConsumer(
      Map<String, Object> consumerProperties,
      ObjectMapper sortingMapper,
      String groupId
  )
  {
    final Properties props = new Properties();
    KafkaRecordSupplier.addConsumerPropertiesFromConfig(props, sortingMapper, consumerProperties);
    props.setProperty("group.id", groupId);
    props.setProperty("share.acknowledgement.mode", "explicit");

    final ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(KafkaShareGroupRecordSupplier.class.getClassLoader());
      return new KafkaShareConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }
}
