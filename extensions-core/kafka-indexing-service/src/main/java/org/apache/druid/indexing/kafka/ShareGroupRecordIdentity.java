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

import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;

import java.util.Objects;

final class ShareGroupRecordIdentity
{
  private final String topic;
  private final int partition;
  private final long offset;

  ShareGroupRecordIdentity(String topic, int partition, long offset)
  {
    this.topic = Objects.requireNonNull(topic, "topic");
    this.partition = partition;
    this.offset = offset;
  }

  static ShareGroupRecordIdentity from(
      OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record
  )
  {
    return new ShareGroupRecordIdentity(
        record.getStream(),
        record.getPartitionId().partition(),
        record.getSequenceNumber()
    );
  }

  String getTopic()
  {
    return topic;
  }

  int getPartition()
  {
    return partition;
  }

  long getOffset()
  {
    return offset;
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
    final ShareGroupRecordIdentity that = (ShareGroupRecordIdentity) o;
    return partition == that.partition && offset == that.offset && topic.equals(that.topic);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(topic, partition, offset);
  }

  @Override
  public String toString()
  {
    return topic + ':' + partition + ':' + offset;
  }
}
