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

/**
 * Adapts {@link KafkaShareConsumer} to {@link AcknowledgingRecordSupplier}.
 * Delivery state lives on the broker; the supplier sets {@code group.id} to
 * the configured share group name.
 */
public class KafkaShareGroupRecordSupplier
    implements AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>
{
  private static final Logger log = new Logger(KafkaShareGroupRecordSupplier.class);

  private final KafkaShareConsumer<byte[], byte[]> consumer;

  /**
   * Records returned by the most recent {@link #poll(long)}, retained so
   * {@link #acknowledge} can pass the original {@link ConsumerRecord} to the
   * Kafka client. The {@code (topic, partition, offset)} ack variant is fragile
   * once the share-fetch buffer rolls over (KIP-932).
   */
  private final Map<RecordKey, ConsumerRecord<byte[], byte[]>> deliveredRecords = new HashMap<>();
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
    deliveredRecords.clear();
    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> polledRecords =
        new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(timeoutMs))) {
      deliveredRecords.put(new RecordKey(record.topic(), record.partition(), record.offset()), record);
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
    final ConsumerRecord<byte[], byte[]> record = deliveredRecords.get(
        new RecordKey(topic, partitionId.partition(), offset)
    );
    if (record == null) {
      throw new IllegalStateException(StringUtils.format(
          "Cannot acknowledge unknown record at topic[%s] partition[%d] offset[%d]; "
          + "either it was not delivered by the most recent poll() or it has already been acknowledged.",
          topic, partitionId.partition(), offset
      ));
    }
    consumer.acknowledge(record, toKafkaAcknowledgeType(type));
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
        final ConsumerRecord<byte[], byte[]> record = deliveredRecords.get(
            new RecordKey(topic, partition.partition(), offset)
        );
        if (record == null) {
          throw new IllegalStateException(StringUtils.format(
              "Cannot acknowledge unknown record at topic[%s] partition[%d] offset[%d]; "
              + "either it was not delivered by the most recent poll() or it has already been acknowledged.",
              topic, partition.partition(), offset
          ));
        }
        consumer.acknowledge(record, kafkaType);
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
    // Share consumer does not expose partition assignment; broker manages it.
    return Collections.emptySet();
  }

  @Override
  public void wakeup()
  {
    consumer.wakeup();
  }

  @Override
  public Optional<Integer> acquisitionLockTimeoutMs()
  {
    return consumer.acquisitionLockTimeoutMs();
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
      case RENEW:
        return org.apache.kafka.clients.consumer.AcknowledgeType.RENEW;
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
    final Map<String, Object> sanitized = ShareGroupConsumerProperties.sanitize(consumerProperties);

    final Properties props = new Properties();
    KafkaRecordSupplier.addConsumerPropertiesFromConfig(props, sortingMapper, sanitized);
    ShareGroupConsumerProperties.sanitize(props);
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

  private static final class RecordKey
  {
    private final String topic;
    private final int partition;
    private final long offset;

    RecordKey(String topic, int partition, long offset)
    {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RecordKey)) {
        return false;
      }
      final RecordKey that = (RecordKey) o;
      return partition == that.partition && offset == that.offset && topic.equals(that.topic);
    }

    @Override
    public int hashCode()
    {
      int result = topic.hashCode();
      result = 31 * result + partition;
      result = 31 * result + Long.hashCode(offset);
      return result;
    }
  }
}
