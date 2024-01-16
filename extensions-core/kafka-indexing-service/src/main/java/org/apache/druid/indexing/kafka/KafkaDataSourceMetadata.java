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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.java.util.common.IAE;

import java.util.Comparator;

public class KafkaDataSourceMetadata extends SeekableStreamDataSourceMetadata<KafkaTopicPartition, Long> implements Comparable<KafkaDataSourceMetadata>
{

  @JsonCreator
  public KafkaDataSourceMetadata(
      @JsonProperty("partitions") SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> kafkaPartitions
  )
  {
    super(kafkaPartitions);
  }

  @Override
  public DataSourceMetadata asStartMetadata()
  {
    final SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> sequenceNumbers = getSeekableStreamSequenceNumbers();
    if (sequenceNumbers instanceof SeekableStreamEndSequenceNumbers) {
      return createConcreteDataSourceMetaData(
          ((SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long>) sequenceNumbers).asStartPartitions(true)
      );
    } else {
      return this;
    }
  }

  @Override
  protected SeekableStreamDataSourceMetadata<KafkaTopicPartition, Long> createConcreteDataSourceMetaData(
      SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> seekableStreamSequenceNumbers
  )
  {
    return new KafkaDataSourceMetadata(seekableStreamSequenceNumbers);
  }

  @Override
  // This method is to compare KafkaDataSourceMetadata.
  // It compares this and other SeekableStreamSequenceNumbers using naturalOrder comparator.
  public int compareTo(KafkaDataSourceMetadata other)
  {
    if (!getClass().equals(other.getClass())) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getName(),
          other.getClass().getName()
      );
    }
    return getSeekableStreamSequenceNumbers().compareTo(other.getSeekableStreamSequenceNumbers(), Comparator.naturalOrder());
  }
}
