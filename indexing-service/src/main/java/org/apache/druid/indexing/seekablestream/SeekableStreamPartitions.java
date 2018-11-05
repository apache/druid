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
import com.google.errorprone.annotations.DoNotCall;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Objects;

/**
 * class that encapsulates a map of partitionId -> sequenceNumber.
 * To be backward compatible with both Kafka and Kinesis datasource metadata when
 * deserializing json. Redundant constructor fields stream, topic and
 * partitionSequenceNumberMap and partitionOffsetMap are created. Only one of topic, stream
 * should have a non-null value and only one of partitionOffsetMap and partitionSequenceNumberMap
 * should have a non-null value.
 * <p>
 * Redundant getters
 * are used for proper Jackson serialization/deserialization when processing terminologies
 * used by Kafka and kinesis (i.e. topic vs. name)
 *
 * @param <PartitionType> partition id type
 * @param <SequenceType>  sequence number type
 */
public class SeekableStreamPartitions<PartitionType, SequenceType>
{
  public static final String NO_END_SEQUENCE_NUMBER = "NO_END_SEQUENCE_NUMBER";

  // stream/topic
  private final String name;
  // partitionId -> sequence number
  private final Map<PartitionType, SequenceType> map;

  @JsonCreator
  public SeekableStreamPartitions(
      @JsonProperty("stream") final String stream,
      @JsonProperty("topic") final String topic,
      @JsonProperty("partitionSequenceNumberMap") final Map<PartitionType, SequenceType> partitionSequenceNumberMap,
      @JsonProperty("partitionOffsetMap") final Map<PartitionType, SequenceType> partitionOffsetMap
  )
  {
    this.name = stream == null ? topic : stream;
    this.map = ImmutableMap.copyOf(partitionOffsetMap == null
                                   ? partitionSequenceNumberMap
                                   : partitionOffsetMap);
    Preconditions.checkArgument(this.name != null);
    Preconditions.checkArgument(map != null);
  }

  // constructor for backward compatibility
  public SeekableStreamPartitions(@NotNull final String id, final Map<PartitionType, SequenceType> partitionOffsetMap)
  {
    this(id, null, partitionOffsetMap, null);
  }

  @JsonProperty
  public String getStream()
  {
    return name;
  }

  @DoNotCall
  @JsonProperty
  public String getTopic()
  {
    return name;
  }

  @JsonProperty
  public Map<PartitionType, SequenceType> getPartitionSequenceNumberMap()
  {
    return map;
  }

  @DoNotCall
  @JsonProperty
  public Map<PartitionType, SequenceType> getPartitionOffsetMap()
  {
    return map;
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
    return Objects.equals(name, that.name) &&
           Objects.equals(map, that.map);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, map);
  }

  @Override
  public String toString()
  {
    return "SeekableStreamPartitions{" +
           "name/topic='" + name + '\'' +
           ", partitionSequenceNumberMap/partitionOffsetMap=" + map +
           '}';
  }
}
