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
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CollectionUtils;
import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;
import java.util.Map;

public class KafkaDataSourceMetadata extends SeekableStreamDataSourceMetadata<KafkaTopicPartition, Long> implements Comparable<KafkaDataSourceMetadata>
{
  private static final Logger LOGGER = new Logger(KafkaDataSourceMetadata.class);

  @JsonCreator
  public KafkaDataSourceMetadata(
      @JsonProperty("partitions") SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> kafkaPartitions
  )
  {
    super(kafkaPartitions == null
        ? null
        : kafkaPartitions instanceof SeekableStreamStartSequenceNumbers
            ?
            new KafkaSeekableStreamStartSequenceNumbers(
                kafkaPartitions.getStream(),
                ((SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long>) kafkaPartitions).getTopic(),
                kafkaPartitions.getPartitionSequenceNumberMap(),
                ((SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long>) kafkaPartitions).getPartitionOffsetMap(),
                ((SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long>) kafkaPartitions).getExclusivePartitions()
            )
            : new KafkaSeekableStreamEndSequenceNumbers(
                kafkaPartitions.getStream(),
                ((SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long>) kafkaPartitions).getTopic(),
                kafkaPartitions.getPartitionSequenceNumberMap(),
                ((SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long>) kafkaPartitions).getPartitionOffsetMap()
            ));
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
    KafkaDataSourceMetadata thisPlusOther = (KafkaDataSourceMetadata) plus(other);
    if (thisPlusOther.equals(other.plus(this))) {
      return true;
    }

    // check that thisPlusOther contains all metadata from other, and that there is no inconsistency or loss
    KafkaDataSourceMetadata otherMetadata = (KafkaDataSourceMetadata) other;
    final SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> otherSequenceNumbers = otherMetadata.getSeekableStreamSequenceNumbers();
    if (!getSeekableStreamSequenceNumbers().isMultiTopicPartition() && !otherSequenceNumbers.isMultiTopicPartition()) {
      return false;
    }
    final SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> mergedSequenceNumbers = thisPlusOther.getSeekableStreamSequenceNumbers();

    final Map<TopicPartition, Long> topicAndPartitionToSequenceNumber = CollectionUtils.mapKeys(
        mergedSequenceNumbers.getPartitionSequenceNumberMap(),
        k -> k.asTopicPartition(mergedSequenceNumbers.getStream())
    );

    boolean allOtherFoundAndConsistent = otherSequenceNumbers.getPartitionSequenceNumberMap().entrySet().stream().noneMatch(
        e -> {
          KafkaTopicPartition kafkaTopicPartition = e.getKey();
          TopicPartition topicPartition = kafkaTopicPartition.asTopicPartition(otherSequenceNumbers.getStream());
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

    boolean allThisFoundAndConsistent = this.getSeekableStreamSequenceNumbers().getPartitionSequenceNumberMap().entrySet().stream().noneMatch(
        e -> {
          KafkaTopicPartition kafkaTopicPartition = e.getKey();
          TopicPartition topicPartition = kafkaTopicPartition.asTopicPartition(this.getSeekableStreamSequenceNumbers().getStream());
          Long oldSequenceOffset = topicAndPartitionToSequenceNumber.get(topicPartition);
          long sequenceOffset = e.getValue();
          if (oldSequenceOffset == null || !oldSequenceOffset.equals(sequenceOffset)) {
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

    return allOtherFoundAndConsistent && allThisFoundAndConsistent;
  }
}
