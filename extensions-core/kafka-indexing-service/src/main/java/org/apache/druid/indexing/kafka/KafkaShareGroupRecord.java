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

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

final class KafkaShareGroupRecord
    extends OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>
{
  private final ConsumerRecord<byte[], byte[]> consumerRecord;

  private KafkaShareGroupRecord(ConsumerRecord<byte[], byte[]> consumerRecord)
  {
    super(
        consumerRecord.topic(),
        new KafkaTopicPartition(true, consumerRecord.topic(), consumerRecord.partition()),
        consumerRecord.offset(),
        consumerRecord.value() == null
        ? ImmutableList.of()
        : ImmutableList.of(new KafkaRecordEntity(consumerRecord)),
        consumerRecord.timestamp()
    );
    this.consumerRecord = consumerRecord;
  }

  static KafkaShareGroupRecord from(ConsumerRecord<byte[], byte[]> consumerRecord)
  {
    return new KafkaShareGroupRecord(consumerRecord);
  }

  ConsumerRecord<byte[], byte[]> getConsumerRecord()
  {
    return consumerRecord;
  }
}
