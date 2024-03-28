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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaDataSourceMetadata extends SeekableStreamDataSourceMetadata<KafkaTopicPartition, Long> implements Comparable<KafkaDataSourceMetadata>
{
  private static final Logger LOGGER = new Logger(KafkaDataSourceMetadata.class);

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

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    if (!getClass().equals(other.getClass())) {
      return false;
    }
    if (plus(other).equals(other.plus(this))) {
      return true;
    }
    return plusTopicPartition((KafkaDataSourceMetadata) other)
        .equals(((KafkaDataSourceMetadata) other).plusTopicPartition(this));
  }

  @Override
  public boolean matchesOld(DataSourceMetadata old)
  {
    if (!getClass().equals(old.getClass())) {
      return false;
    }
    if (plus(old).equals(old.plus(this))) {
      return true;
    }
    // assume other is older or make a new version of this function
    KafkaDataSourceMetadata oldMetadata = (KafkaDataSourceMetadata) old;
    final SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> sequenceNumbers = getSeekableStreamSequenceNumbers();
    final SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> oldSequenceNumbers = oldMetadata.getSeekableStreamSequenceNumbers();

    final Map<TopicPartition, Long> topicAndPartitionToSequenceNumber = sequenceNumbers.getPartitionSequenceNumberMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            e -> {
              KafkaTopicPartition kafkaTopicPartition = e.getKey();
              return kafkaTopicPartition.asTopicPartition(sequenceNumbers.getStream());
            },
            e -> e.getValue()
        ));

    return oldSequenceNumbers.getPartitionSequenceNumberMap().entrySet().stream().noneMatch(
        e -> {
          KafkaTopicPartition kafkaTopicPartition = e.getKey();
          TopicPartition topicPartition = kafkaTopicPartition.asTopicPartition(oldSequenceNumbers.getStream());
          Long sequenceOffset = topicAndPartitionToSequenceNumber.get(topicPartition);
          long oldSequenceOffset = e.getValue();
          if (sequenceOffset == null || !sequenceOffset.equals(oldSequenceOffset)) {
            LOGGER.info(
                "sequenceOffset found for currently computed and stored metadata does not match for "
                + "topicPartition: [%s].  currentSequenceOffset: [%s], oldSequenceOffset: [%s]",
                topicPartition,
                sequenceOffset,
                oldSequenceOffset
            );
            return true;
          }
          return false;
        }
    );
  }

  private Map<TopicPartition, Long> plusTopicPartition(KafkaDataSourceMetadata other)
  {
    final SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> offsets = getSeekableStreamSequenceNumbers();
    final SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> otherOffsets = other.getSeekableStreamSequenceNumbers();
    final Map<TopicPartition, Long> topicPartitionToOffset = buildTopicPartitionToOffset(offsets);
    final Map<TopicPartition, Long> topicPartitionToOtherOffset = buildTopicPartitionToOffset(
        otherOffsets);

    topicPartitionToOffset.putAll(topicPartitionToOtherOffset);

    return topicPartitionToOffset;

  }

  private Map<TopicPartition, Long> buildTopicPartitionToOffset(
      SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> sequenceNumbers)
  {
    return sequenceNumbers.getPartitionSequenceNumberMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            e -> {
              KafkaTopicPartition kafkaTopicPartition = e.getKey();
              return kafkaTopicPartition.asTopicPartition(sequenceNumbers.getStream());
            },
            e -> e.getValue()
        ));
  }
}
