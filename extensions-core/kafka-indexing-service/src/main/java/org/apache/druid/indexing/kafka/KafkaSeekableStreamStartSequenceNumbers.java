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

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Represents the kafka based start sequenceNumber per partition of a sequence. This class is needed because
 * of special handling that must be done for multi-topic partitions to ensure that offsets are preserved.
 * <p>
 * Do not register this class as a subtype of base class in Jackson. We want this class to be serialized
 * when written to DB as a {@link SeekableStreamStartSequenceNumbers}. Do not create instances of this class
 * directly from jackson mapper.
 */
@JsonTypeName(SeekableStreamStartSequenceNumbers.TYPE)
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
    validateSequenceNumbersBaseType(other);

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

      // start with existing sequence numbers which in this case will be all single topic.
      newMap = new HashMap<>(getPartitionSequenceNumberMap());

      // add all sequence numbers from other where the topic name matches this topic. Transform to single topic
      // as in this case we will be returning a single topic based sequence map.
      newMap.putAll(that.getPartitionSequenceNumberMap().entrySet().stream()
          .filter(e -> {
            if (e.getKey().topic().isPresent()) {
              return e.getKey().topic().get().equals(thisTopic);
            } else {
              // this branch shouldn't really be hit since other should be multi-topic here, but adding this
              // just in case.
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
            KafkaTopicPartition multiTopicPartitonIdToSearch = new KafkaTopicPartition(true, thisTopic, partitionId.partition());
            if (getExclusivePartitions().contains(partitionId) && !that.getPartitionSequenceNumberMap().containsKey(multiTopicPartitonIdToSearch)) {
              newExclusivePartitions.add(new KafkaTopicPartition(false, this.getStream(), partitionId.partition()));
            }
          }
      );
      newExclusivePartitions.addAll(that.getExclusivePartitions());
    } else {
      // going from single topic or topicPattern to topicPattern

      // start with existing sequence numbers and transform them to multit-topic keys, as the returned
      // sequence numbers will be multi-topic based.
      newMap = CollectionUtils.mapKeys(
          getPartitionSequenceNumberMap(),
          k -> new KafkaTopicPartition(
              true,
              k.asTopicPartition(thisTopic).topic(),
              k.partition()
          )
      );

      // add all sequence numbers from other where the topic name matches the pattern of this topic regex. Transform to
      // multi-topic as in this case we will be returning a multi-topic based sequence map.
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
            KafkaTopicPartition multiTopicPartitonIdToSearch = new KafkaTopicPartition(true, thisTopic, partitionId.partition());
            boolean thatTopicMatchesThisTopicPattern = partitionId.topic().isPresent() ? pattern.matcher(partitionId.topic().get()).matches() : pattern.matcher(thatTopic).matches();
            if (getExclusivePartitions().contains(partitionId) && (!thatTopicMatchesThisTopicPattern || !that.getPartitionSequenceNumberMap().containsKey(multiTopicPartitonIdToSearch))) {
              newExclusivePartitions.add(new KafkaTopicPartition(true, this.getStream(), partitionId.partition()));
            }
          }
      );
      newExclusivePartitions.addAll(that.getExclusivePartitions());
    }

    return new SeekableStreamStartSequenceNumbers<>(getStream(), newMap, newExclusivePartitions);
  }

  @Override
  public SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> minus(
      SeekableStreamSequenceNumbers<KafkaTopicPartition, Long> other
  )
  {
    validateSequenceNumbersBaseType(other);

    final KafkaSeekableStreamStartSequenceNumbers otherStart =
        (KafkaSeekableStreamStartSequenceNumbers) other;

    if (!this.isMultiTopicPartition() && !otherStart.isMultiTopicPartition()) {
      return super.minus(other);
    }

    final Map<KafkaTopicPartition, Long> newMap = new HashMap<>();
    final Set<KafkaTopicPartition> newExclusivePartitions = new HashSet<>();
    String thatTopic = otherStart.getStream();

    // remove partitions present in "that" from "this", check for exact match, multi-topic match, or single-topic match
    for (Map.Entry<KafkaTopicPartition, Long> entry : getPartitionSequenceNumberMap().entrySet()) {
      String thisTopic = entry.getKey().asTopicPartition(getStream()).topic();
      boolean otherContainsThis = otherStart.getPartitionSequenceNumberMap().containsKey(entry.getKey());
      boolean otherContainsThisMultiTopic = otherStart.getPartitionSequenceNumberMap()
          .containsKey(new KafkaTopicPartition(true, thisTopic, entry.getKey().partition()));
      boolean otherContainsThisSingleTopic = (thatTopic.equals(thisTopic) && otherStart.getPartitionSequenceNumberMap()
          .containsKey(new KafkaTopicPartition(false, null, entry.getKey().partition())));
      if (!otherContainsThis && !otherContainsThisMultiTopic && !otherContainsThisSingleTopic) {
        newMap.put(entry.getKey(), entry.getValue());
        // A partition is exclusive if it's exclusive in "this" and not in "other"'s partitionSequenceNumberMap
        if (getExclusivePartitions().contains(entry.getKey())) {
          newExclusivePartitions.add(entry.getKey());
        }
      }
    }

    return new SeekableStreamStartSequenceNumbers<>(
        getStream(),
        newMap,
        newExclusivePartitions
    );
  }
}
