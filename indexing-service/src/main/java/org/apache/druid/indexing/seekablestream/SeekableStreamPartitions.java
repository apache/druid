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
import com.google.common.collect.ImmutableMap;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Objects;

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
public class SeekableStreamPartitions<PartitionIdType, SequenceOffsetType>
{
  // this special marker is used by the KinesisSupervisor to set the endOffsets
  // of newly created indexing tasks. This is necessary because streaming tasks do not
  // have endPartitionOffsets. This marker signals to the task that it should continue
  // to ingest data until taskDuration has elapsed or the task was stopped or paused or killed
  public static final String NO_END_SEQUENCE_NUMBER = "NO_END_SEQUENCE_NUMBER";

  // stream/topic
  private final String stream;
  // partitionId -> sequence number
  private final Map<PartitionIdType, SequenceOffsetType> partitionIdToSequenceNumberMap;

  @JsonCreator
  public SeekableStreamPartitions(
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
    this.partitionIdToSequenceNumberMap = ImmutableMap.copyOf(partitionOffsetMap == null
                                                              ? partitionSequenceNumberMap
                                                              : partitionOffsetMap);
    Preconditions.checkArgument(this.stream != null);
    Preconditions.checkArgument(partitionIdToSequenceNumberMap != null);
  }

  // constructor for backward compatibility
  public SeekableStreamPartitions(
      @NotNull final String stream,
      final Map<PartitionIdType, SequenceOffsetType> partitionOffsetMap
  )
  {
    this(
        Preconditions.checkNotNull(stream, "stream"),
        null,
        Preconditions.checkNotNull(partitionOffsetMap, "partitionOffsetMap"),
        null
    );
  }

  @JsonProperty
  public String getStream()
  {
    return stream;
  }

  /**
   * Identical to {@link #getStream()}. Here for backwards compatibility, so a serialized SeekableStreamPartitions can
   * be read by older Druid versions as a KafkaPartitions object.
   */
  @JsonProperty
  public String getTopic()
  {
    return stream;
  }

  @JsonProperty
  public Map<PartitionIdType, SequenceOffsetType> getPartitionSequenceNumberMap()
  {
    return partitionIdToSequenceNumberMap;
  }

  /**
   * Identical to {@link #getPartitionSequenceNumberMap()} ()}. Here for backwards compatibility, so a serialized
   * SeekableStreamPartitions can be read by older Druid versions as a KafkaPartitions object.
   */
  @JsonProperty
  public Map<PartitionIdType, SequenceOffsetType> getPartitionOffsetMap()
  {
    return partitionIdToSequenceNumberMap;
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
    SeekableStreamPartitions that = (SeekableStreamPartitions) o;
    return Objects.equals(stream, that.stream) &&
           Objects.equals(partitionIdToSequenceNumberMap, that.partitionIdToSequenceNumberMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stream, partitionIdToSequenceNumberMap);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "stream='" + stream + '\'' +
           ", partitionSequenceNumberMap=" + partitionIdToSequenceNumberMap +
           '}';
  }
}
