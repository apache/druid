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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * class that encapsulates a partitionIdToSequenceNumberMap of partitionId -> sequenceNumber.
 * To be backward compatible with both Kafka and Kinesis datasource metadata when
 * serializing and deserializing json, redundant constructor fields stream, topic,
 * partitionSequenceNumberMap and partitionOffsetMap are created. Only one of topic, stream
 * should have a non-null value and only one of partitionOffsetMap and partitionSequenceNumberMap
 * should have a non-null value.
 *
 * Redundant getters are used for proper Jackson serialization/deserialization when processing terminologies
 * used by Kafka and Kinesis (i.e. topic vs. stream)
 *
 * @param <PartitionIdType>    partition id type
 * @param <SequenceOffsetType> sequence number type
 */
public class SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType> implements
    SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType>
{
  // stream/topic
  private final String stream;
  // partitionId -> sequence number
  private final Map<PartitionIdType, SequenceOffsetType> partitionSequenceNumberMap;
  private final Set<PartitionIdType> exclusivePartitions;

  @JsonCreator
  public SeekableStreamStartSequenceNumbers(
      @JsonProperty("stream") final String stream,
      @JsonProperty("partitionSequenceNumberMap")
      final Map<PartitionIdType, SequenceOffsetType> partitionSequenceNumberMap,
      @JsonProperty("exclusivePartitions") @Nullable final Set<PartitionIdType> exclusivePartitions
  )
  {
    this.stream = Preconditions.checkNotNull(stream, "stream");
    this.partitionSequenceNumberMap = Preconditions.checkNotNull(
        partitionSequenceNumberMap,
        "partitionIdToSequenceNumberMap"
    );
    // exclusiveOffset can be null if this class is deserialized from metadata store. Note that only end offsets are
    // stored in metadata store.
    // The default is true because there was only Kafka indexing service before in which the end offset is always
    // exclusive.
    this.exclusivePartitions = exclusivePartitions == null ? Collections.emptySet() : exclusivePartitions;
  }

  @Override
  @JsonProperty
  public String getStream()
  {
    return stream;
  }

  @Override
  @JsonProperty
  public Map<PartitionIdType, SequenceOffsetType> getPartitionSequenceNumberMap()
  {
    return partitionSequenceNumberMap;
  }

  @Override
  public SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> plus(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other
  )
  {
    if (this.getClass() != other.getClass()) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    final SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType> otherEnd =
        (SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType>) other;

    if (stream.equals(otherEnd.stream)) {
      // Same stream, merge sequences.
      final Map<PartitionIdType, SequenceOffsetType> newMap = new HashMap<>(partitionSequenceNumberMap);
      newMap.putAll(otherEnd.partitionSequenceNumberMap);

      final Set<PartitionIdType> newExclusivePartitions = new HashSet<>();

      partitionSequenceNumberMap.forEach(
          (partitionId, sequenceOffset) -> {
            if (exclusivePartitions.contains(partitionId)
                && !otherEnd.partitionSequenceNumberMap.containsKey(partitionId)) {
              newExclusivePartitions.add(partitionId);
            }
          }
      );
      newExclusivePartitions.addAll(otherEnd.exclusivePartitions);

      return new SeekableStreamStartSequenceNumbers<>(
          stream,
          newMap,
          newExclusivePartitions
      );
    } else {
      return null;
    }
  }

  @Override
  public SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> minus(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other
  )
  {
    if (this.getClass() != other.getClass()) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    final SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType> otherEnd =
        (SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType>) other;

    if (stream.equals(otherEnd.stream)) {
      // Same stream, remove partitions present in "that" from "this"
      final Map<PartitionIdType, SequenceOffsetType> newMap = new HashMap<>();
      final Set<PartitionIdType> newExclusivePartitions = new HashSet<>();

      for (Entry<PartitionIdType, SequenceOffsetType> entry : partitionSequenceNumberMap.entrySet()) {
        if (!otherEnd.partitionSequenceNumberMap.containsKey(entry.getKey())) {
          newMap.put(entry.getKey(), entry.getValue());
          if (exclusivePartitions.contains(entry.getKey())) {
            newExclusivePartitions.add(entry.getKey());
          }
        }
      }

      return new SeekableStreamStartSequenceNumbers<>(
          stream,
          newMap,
          newExclusivePartitions
      );
    } else {
      return this;
    }
  }

  @JsonProperty
  public Set<PartitionIdType> getExclusivePartitions()
  {
    return exclusivePartitions;
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
    SeekableStreamStartSequenceNumbers<?, ?> that = (SeekableStreamStartSequenceNumbers<?, ?>) o;
    return Objects.equals(stream, that.stream) &&
           Objects.equals(partitionSequenceNumberMap, that.partitionSequenceNumberMap) &&
           Objects.equals(exclusivePartitions, that.exclusivePartitions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stream, partitionSequenceNumberMap, exclusivePartitions);
  }

  @Override
  public String toString()
  {
    return "SeekableStreamStartSequenceNumbers{" +
           "stream='" + stream + '\'' +
           ", partitionSequenceNumberMap=" + partitionSequenceNumberMap +
           ", exclusivePartitions=" + exclusivePartitions +
           '}';
  }
}
