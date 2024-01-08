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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Represents the end sequenceNumber per partition of a sequence. Note that end sequenceNumbers are always
 * exclusive/inclusive in Kafka/Kinesis indexing service, respectively.
 *
 * To be backward compatible with both Kafka and Kinesis datasource metadata when
 * serializing and deserializing json, redundant constructor fields stream, topic,
 * partitionSequenceNumberMap and partitionOffsetMap are created. Only one of topic, stream
 * should have a non-null value and only one of partitionOffsetMap and partitionSequenceNumberMap
 * should have a non-null value.
 *
 * Redundant getters are used for proper Jackson serialization/deserialization when processing terminologies
 * used by Kafka and Kinesis (i.e. topic vs. stream)
 */
public class SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType> implements
    SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType>
{
  // stream/topic
  private final String stream;
  // partitionId -> sequence number
  private final Map<PartitionIdType, SequenceOffsetType> partitionSequenceNumberMap;

  @JsonCreator
  public SeekableStreamEndSequenceNumbers(
      @JsonProperty("stream") final String stream,
      // kept for backward compatibility
      @JsonProperty("topic") final String topic,
      @JsonProperty("partitionSequenceNumberMap")
      final Map<PartitionIdType, SequenceOffsetType> partitionSequenceNumberMap,
      // kept for backward compatibility
      @JsonProperty("partitionOffsetMap") final Map<PartitionIdType, SequenceOffsetType> partitionOffsetMap
  )
  {
    this.stream = stream == null ? topic : stream;
    this.partitionSequenceNumberMap = partitionOffsetMap == null ? partitionSequenceNumberMap : partitionOffsetMap;

    Preconditions.checkNotNull(this.stream, "stream");
    Preconditions.checkNotNull(this.partitionSequenceNumberMap, "partitionIdToSequenceNumberMap");
  }

  public SeekableStreamEndSequenceNumbers(
      final String stream,
      final Map<PartitionIdType, SequenceOffsetType> partitionSequenceNumberMap
  )
  {
    this(stream, null, partitionSequenceNumberMap, null);
  }

  /**
   * Converts this end sequence numbers into start sequence numbers. This conversion is required when checking two
   * sequence numbers are "matched" in {@code IndexerSQLMetadataStorageCoordinator#updateDataSourceMetadataWithHandle}
   * because only sequences numbers of the same type can be compared.
   *
   * @param isExclusiveEndOffset flag that end offsets are exclusive. Should be true for Kafka and false for Kinesis.
   */
  public SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType> asStartPartitions(
      boolean isExclusiveEndOffset
  )
  {
    return new SeekableStreamStartSequenceNumbers<>(
        stream,
        partitionSequenceNumberMap,
        // All start offsets are supposed to be opposite
        isExclusiveEndOffset ? Collections.emptySet() : partitionSequenceNumberMap.keySet()
    );
  }

  @Override
  @JsonProperty
  public String getStream()
  {
    return stream;
  }

  /**
   * Identical to {@link #getStream()}. Here for backwards compatibility, so a serialized
   * SeekableStreamEndSequenceNumbers can be read by older Druid versions as a KafkaPartitions object.
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

    final SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType> otherEnd =
        (SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType>) other;

    if (stream.equals(otherEnd.stream)) {
      // Same stream, merge sequences.
      final Map<PartitionIdType, SequenceOffsetType> newMap = new HashMap<>(partitionSequenceNumberMap);
      newMap.putAll(otherEnd.partitionSequenceNumberMap);
      return new SeekableStreamEndSequenceNumbers<>(stream, newMap);
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

    final SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType> otherStart =
        (SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType>) other;

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

    final SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType> otherEnd =
        (SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType>) other;

    if (stream.equals(otherEnd.stream)) {
      // Same stream, remove partitions present in "that" from "this"
      final Map<PartitionIdType, SequenceOffsetType> newMap = new HashMap<>();

      for (Entry<PartitionIdType, SequenceOffsetType> entry : partitionSequenceNumberMap.entrySet()) {
        if (!otherEnd.partitionSequenceNumberMap.containsKey(entry.getKey())) {
          newMap.put(entry.getKey(), entry.getValue());
        }
      }

      return new SeekableStreamEndSequenceNumbers<>(stream, newMap);
    } else {
      // Different stream, prefer "this".
      return this;
    }
  }

  /**
   * Identical to {@link #getPartitionSequenceNumberMap()} ()}. Here for backwards compatibility, so a serialized
   * SeekableStreamEndSequenceNumbers can be read by older Druid versions as a KafkaPartitions object.
   */
  @JsonProperty
  public Map<PartitionIdType, SequenceOffsetType> getPartitionOffsetMap()
  {
    return partitionSequenceNumberMap;
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
    SeekableStreamEndSequenceNumbers<?, ?> that = (SeekableStreamEndSequenceNumbers<?, ?>) o;
    return Objects.equals(stream, that.stream) &&
           Objects.equals(partitionSequenceNumberMap, that.partitionSequenceNumberMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stream, partitionSequenceNumberMap);
  }

  @Override
  public String toString()
  {
    return "SeekableStreamEndSequenceNumbers{" +
           "stream='" + stream + '\'' +
           ", partitionSequenceNumberMap=" + partitionSequenceNumberMap +
           '}';
  }
}
