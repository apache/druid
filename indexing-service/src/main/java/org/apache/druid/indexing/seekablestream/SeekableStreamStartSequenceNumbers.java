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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the start sequenceNumber per partition of a sequence. This class keeps an additional set of
 * {@link #exclusivePartitions} for Kinesis indexing service in where each start offset can be either inclusive
 * or exclusive.
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
      // kept for backward compatibility
      @JsonProperty("topic") final String topic,
      @JsonProperty("partitionSequenceNumberMap")
      final Map<PartitionIdType, SequenceOffsetType> partitionSequenceNumberMap,
      // kept for backward compatibility
      @JsonProperty("partitionOffsetMap") final Map<PartitionIdType, SequenceOffsetType> partitionOffsetMap,
      @JsonProperty("exclusivePartitions") @Nullable final Set<PartitionIdType> exclusivePartitions
  )
  {
    this.stream = stream == null ? topic : stream;
    this.partitionSequenceNumberMap = partitionOffsetMap == null ? partitionSequenceNumberMap : partitionOffsetMap;

    Preconditions.checkNotNull(this.stream, "stream");
    Preconditions.checkNotNull(this.partitionSequenceNumberMap, "partitionIdToSequenceNumberMap");

    // exclusiveOffset can be null if this class is deserialized from metadata store. Note that only end offsets are
    // stored in metadata store.
    // The default is true because there was only Kafka indexing service before in which the end offset is always
    // exclusive.
    this.exclusivePartitions = exclusivePartitions == null ? Collections.emptySet() : exclusivePartitions;
  }

  public SeekableStreamStartSequenceNumbers(
      String stream,
      Map<PartitionIdType, SequenceOffsetType> partitionSequenceNumberMap,
      Set<PartitionIdType> exclusivePartitions
  )
  {
    this(stream, null, partitionSequenceNumberMap, null, exclusivePartitions);
  }

  @Override
  @JsonProperty
  public String getStream()
  {
    return stream;
  }

  /**
   * Identical to {@link #getStream()}. Here for backwards compatibility, so a serialized
   * SeekableStreamStartSequenceNumbers can be read by older Druid versions as a KafkaPartitions object.
   */
  @JsonProperty
  public String getTopic()
  {
    return stream;
  }

  @Override
  @JsonProperty
  public Map<PartitionIdType, SequenceOffsetType> getPartitionSequenceNumberMap()
  {
    return partitionSequenceNumberMap;
  }

  /**
   * Identical to {@link #getPartitionSequenceNumberMap()} ()}. Here for backwards compatibility, so a serialized
   * SeekableStreamStartSequenceNumbers can be read by older Druid versions as a KafkaPartitions object.
   */
  @JsonProperty
  public Map<PartitionIdType, SequenceOffsetType> getPartitionOffsetMap()
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
          this.getClass().getName(),
          other.getClass().getName()
      );
    }

    final SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType> otherStart =
        (SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType>) other;

    if (stream.equals(otherStart.stream)) {
      // Same stream, merge sequences.
      final Map<PartitionIdType, SequenceOffsetType> newMap = new HashMap<>(partitionSequenceNumberMap);
      newMap.putAll(otherStart.partitionSequenceNumberMap);

      // A partition is exclusive if it's
      // 1) exclusive in "this" and it's not in "other"'s partitionSequenceNumberMap or
      // 2) exclusive in "other"
      final Set<PartitionIdType> newExclusivePartitions = new HashSet<>();
      partitionSequenceNumberMap.forEach(
          (partitionId, sequenceOffset) -> {
            if (exclusivePartitions.contains(partitionId)
                && !otherStart.partitionSequenceNumberMap.containsKey(partitionId)) {
              newExclusivePartitions.add(partitionId);
            }
          }
      );
      newExclusivePartitions.addAll(otherStart.exclusivePartitions);

      return new SeekableStreamStartSequenceNumbers<>(
          stream,
          newMap,
          newExclusivePartitions
      );
    } else {
      // Different stream, prefer "other".
      return other;
    }
  }

  @Override
  public int compareTo(SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other, Comparator<SequenceOffsetType> comparator)
  {
    if (this.getClass() != other.getClass()) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getName(),
          other.getClass().getName()
      );
    }

    final SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType> otherStart =
        (SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType>) other;

    if (stream.equals(otherStart.stream)) {
      //Same stream, compare the offset
      boolean res = false;
      for (Map.Entry<PartitionIdType, SequenceOffsetType> entry : partitionSequenceNumberMap.entrySet()) {
        PartitionIdType partitionId = entry.getKey();
        SequenceOffsetType sequenceOffset = entry.getValue();
        if (otherStart.partitionSequenceNumberMap.get(partitionId) != null && comparator.compare(sequenceOffset, otherStart.partitionSequenceNumberMap.get(partitionId)) > 0) {
          res = true;
          break;
        }
      }
      if (res) {
        return 1;
      }
    }
    return 0;
  }

  @Override
  public SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> minus(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other
  )
  {
    if (this.getClass() != other.getClass()) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getName(),
          other.getClass().getName()
      );
    }

    final SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType> otherStart =
        (SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType>) other;

    if (stream.equals(otherStart.stream)) {
      // Same stream, remove partitions present in "that" from "this"
      final Map<PartitionIdType, SequenceOffsetType> newMap = new HashMap<>();
      final Set<PartitionIdType> newExclusivePartitions = new HashSet<>();

      for (Entry<PartitionIdType, SequenceOffsetType> entry : partitionSequenceNumberMap.entrySet()) {
        if (!otherStart.partitionSequenceNumberMap.containsKey(entry.getKey())) {
          newMap.put(entry.getKey(), entry.getValue());
          // A partition is exclusive if it's exclusive in "this" and not in "other"'s partitionSequenceNumberMap
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
      // Different stream, prefer "this".
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
