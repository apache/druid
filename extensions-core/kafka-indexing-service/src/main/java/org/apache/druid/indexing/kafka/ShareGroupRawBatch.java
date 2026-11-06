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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class ShareGroupRawBatch
{
  private final String topic;
  private final int partition;
  private final List<ShareGroupRawRecord> records;

  ShareGroupRawBatch(String topic, int partition, List<ShareGroupRawRecord> records)
  {
    Preconditions.checkArgument(partition >= 0, "partition must not be negative");
    Preconditions.checkArgument(!records.isEmpty(), "records must not be empty");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.partition = partition;
    this.records = List.copyOf(records);
  }

  static ShareGroupRawBatch fromOrderedRecords(
      List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records
  )
  {
    Preconditions.checkArgument(!records.isEmpty(), "records must not be empty");
    final String topic = records.get(0).getStream();
    final int partition = records.get(0).getPartitionId().partition();
    final List<ShareGroupRawRecord> rawRecords = new ArrayList<>(records.size());
    for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : records) {
      Preconditions.checkArgument(topic.equals(record.getStream()), "records must have one topic");
      Preconditions.checkArgument(
          partition == record.getPartitionId().partition(),
          "records must have one partition"
      );
      final ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecord(record);
      Preconditions.checkArgument(topic.equals(consumerRecord.topic()), "raw record topic does not match");
      Preconditions.checkArgument(partition == consumerRecord.partition(), "raw record partition does not match");
      Preconditions.checkArgument(
          record.getSequenceNumber() == consumerRecord.offset(),
          "raw record offset does not match"
      );
      rawRecords.add(ShareGroupRawRecord.from(consumerRecord));
    }
    return new ShareGroupRawBatch(topic, partition, rawRecords);
  }

  List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> toOrderedRecords()
  {
    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> orderedRecords =
        new ArrayList<>(records.size());
    for (ShareGroupRawRecord record : records) {
      orderedRecords.add(KafkaShareGroupRecord.from(record.toConsumerRecord(topic, partition)));
    }
    return orderedRecords;
  }

  String getTopic()
  {
    return topic;
  }

  int getPartition()
  {
    return partition;
  }

  List<ShareGroupRawRecord> getRecords()
  {
    return records;
  }

  long getFirstOffset()
  {
    return records.stream().mapToLong(ShareGroupRawRecord::getOffset).min().orElseThrow();
  }

  long getLastOffset()
  {
    return records.stream().mapToLong(ShareGroupRawRecord::getOffset).max().orElseThrow();
  }

  long estimatedSize()
  {
    long size = topic.length() * 2L + Integer.BYTES * 2L;
    for (ShareGroupRawRecord record : records) {
      size += record.estimatedSize();
    }
    return size;
  }

  static long estimatedRecordSize(
      OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record
  )
  {
    return ShareGroupRawRecord.from(consumerRecord(record)).estimatedSize();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ShareGroupRawBatch that = (ShareGroupRawBatch) o;
    return partition == that.partition && topic.equals(that.topic) && records.equals(that.records);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(topic, partition, records);
  }

  private static ConsumerRecord<byte[], byte[]> consumerRecord(
      OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record
  )
  {
    if (record instanceof KafkaShareGroupRecord) {
      return ((KafkaShareGroupRecord) record).getConsumerRecord();
    }
    for (ByteEntity entity : record.getData()) {
      if (entity instanceof KafkaRecordEntity) {
        return ((KafkaRecordEntity) entity).getRecord();
      }
    }
    throw new IllegalArgumentException("Kafka record metadata is not available for " + ShareGroupRecordIdentity.from(record));
  }
}
