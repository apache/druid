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

import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaSeekableStreamStartSequenceNumbers extends SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long>
{
  private final boolean isMultiTopicPartition;

  public KafkaSeekableStreamStartSequenceNumbers(
      String stream,
      String topic,
      Map<KafkaTopicPartition, Long> partitionSequenceNumberMap,
      Map<KafkaTopicPartition, Long> partitionOffsetMap,
      @Nullable Set<KafkaTopicPartition> exclusivePartitions
  )
  {
    super(stream, topic, partitionSequenceNumberMap, partitionOffsetMap, exclusivePartitions);
    // how to know it topicPattern if the partitionSequenceNumberMap is empty?
    isMultiTopicPartition = !partitionSequenceNumberMap.isEmpty() && partitionSequenceNumberMap.keySet()
        .stream()
        .findFirst()
        .get()
        .isMultiTopicPartition();
  }

  @Override
  public boolean isMultiTopicPartition()
  {
    return isMultiTopicPartition;
  }

  @Override
  public SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> plus(
      SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> other
  )
  {
    if (this.getClass() != other.getClass()) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getName(),
          other.getClass().getName()
      );
    }

    KafkaSeekableStreamStartSequenceNumbers that = (KafkaSeekableStreamStartSequenceNumbers) other;

    if (!this.isMultiTopicPartition() && !that.isMultiTopicPartition()) {
      return super.plus(other);
    }


    String thisTopic = getStream();
    String thatTopic = that.getStream();
    final Map<KafkaTopicPartition, Long> newMap;
    final Set<KafkaTopicPartition> newExclusivePartitions = new HashSet<>();
    if (!isMultiTopicPartition()) {
      // going from topicPattern to single topic
      newMap = new HashMap<>(getPartitionSequenceNumberMap().entrySet()
          .stream()
          .collect(Collectors.toMap(
              e -> new KafkaTopicPartition(false, thisTopic, e.getKey().partition()),
              e -> e.getValue()
          )));
      newMap.putAll(that.getPartitionSequenceNumberMap().entrySet().stream()
          .filter(e -> {
            if (e.getKey().topic().isPresent()) {
              return e.getKey().topic().get().equals(thisTopic);
            } else {
              return thatTopic.equals(thisTopic);
            }
          })
          .collect(Collectors.toMap(
              e -> new KafkaTopicPartition(false, thisTopic, e.getKey().partition()),
              Map.Entry::getValue
          )));

      // A partition is exclusive if it's
      // 1) exclusive in "this" and it's not in "other"'s partitionSequenceNumberMap or
      // 2) exclusive in "other"
      getPartitionSequenceNumberMap().forEach(
          (partitionId, sequenceOffset) -> {
            KafkaTopicPartition partitonIdToSearch = new KafkaTopicPartition(true, thisTopic, partitionId.partition());
            if (getExclusivePartitions().contains(partitionId) && !that.getPartitionSequenceNumberMap().containsKey(partitonIdToSearch)) {
              newExclusivePartitions.add(new KafkaTopicPartition(false, this.getStream(), partitionId.partition()));
            }
          }
      );
      newExclusivePartitions.addAll(that.getExclusivePartitions());
    } else {
      // going from single topic or topicPattern to topicPattern
      newMap = new HashMap<>(getPartitionSequenceNumberMap().entrySet()
          .stream()
          .collect(Collectors.toMap(
              e -> new KafkaTopicPartition(true, e.getKey().asTopicPartition(thisTopic).topic(), e.getKey().partition()),
              e -> e.getValue()
          )));
      Pattern pattern = Pattern.compile(thisTopic);
      newMap.putAll(that.getPartitionSequenceNumberMap().entrySet().stream()
          .filter(e -> {
            if (e.getKey().topic().isPresent()) {
              return pattern.matcher(e.getKey().topic().get()).matches();
            } else {
              return pattern.matcher(thatTopic).matches();
            }
          })
          .collect(Collectors.toMap(
              e -> new KafkaTopicPartition(true, e.getKey().asTopicPartition(thatTopic).topic(), e.getKey().partition()),
              Map.Entry::getValue
          )));

      // A partition is exclusive if it's
      // 1) exclusive in "this" and it's not in "other"'s partitionSequenceNumberMap or
      // 2) exclusive in "other"
      getPartitionSequenceNumberMap().forEach(
          (partitionId, sequenceOffset) -> {
            KafkaTopicPartition partitonIdToSearch = new KafkaTopicPartition(true, thisTopic, partitionId.partition());
            boolean thatTopicMatchesThisTopicPattern = partitionId.topic().isPresent() ? pattern.matcher(partitionId.topic().get()).matches() : pattern.matcher(thatTopic).matches();
            if (getExclusivePartitions().contains(partitionId) && (!thatTopicMatchesThisTopicPattern || !that.getPartitionSequenceNumberMap().containsKey(partitonIdToSearch))) {
              newExclusivePartitions.add(new KafkaTopicPartition(false, this.getStream(), partitionId.partition()));
            }
          }
      );
      newExclusivePartitions.addAll(that.getExclusivePartitions());
    }

    return new SeekableStreamStartSequenceNumbers<>(getStream(), newMap, newExclusivePartitions);
  }
}
