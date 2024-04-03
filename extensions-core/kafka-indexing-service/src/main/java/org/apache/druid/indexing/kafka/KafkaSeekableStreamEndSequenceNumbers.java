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
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.java.util.common.IAE;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaSeekableStreamEndSequenceNumbers extends SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long>
{

  private final boolean isMultiTopicPartition;

  public KafkaSeekableStreamEndSequenceNumbers(
      String stream,
      String topic,
      Map<KafkaTopicPartition, Long> partitionSequenceNumberMap,
      Map<KafkaTopicPartition, Long> partitionOffsetMap
  )
  {
    super(stream, topic, partitionSequenceNumberMap, partitionOffsetMap);
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

    KafkaSeekableStreamEndSequenceNumbers that = (KafkaSeekableStreamEndSequenceNumbers) other;

    if (!this.isMultiTopicPartition() && !that.isMultiTopicPartition()) {
      return super.plus(other);
    }

    String thisTopic = getStream();
    String thatTopic = that.getStream();
    final Map<KafkaTopicPartition, Long> newMap;
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
    }

    return new SeekableStreamEndSequenceNumbers<>(getStream(), newMap);
  }
}
