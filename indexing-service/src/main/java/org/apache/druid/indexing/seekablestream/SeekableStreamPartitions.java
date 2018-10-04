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

public class SeekableStreamPartitions<T1, T2>
{
  public static final String NO_END_SEQUENCE_NUMBER = "NO_END_SEQUENCE_NUMBER";

  private final String id;
  private final Map<T1, T2> map;

  @JsonCreator
  public SeekableStreamPartitions(
      @JsonProperty("stream") final String stream,
      @JsonProperty("topic") final String topic,
      @JsonProperty("partitionSequenceNumberMap") final Map<T1, T2> partitionSequenceNumberMap,
      @JsonProperty("partitionOffsetMap") final Map<T1, T2> partitionOffsetMap
  )
  {
    this.id = stream == null ? topic : stream;
    this.map = ImmutableMap.copyOf(partitionOffsetMap == null
                                   ? partitionSequenceNumberMap
                                   : partitionOffsetMap);
    Preconditions.checkArgument(id != null);
    Preconditions.checkArgument(map != null);
    // Validate map
    for (Map.Entry<T1, T2> entry : map.entrySet()) {
      Preconditions.checkArgument(
          entry.getValue() != null,
          String.format(
              "partition id[%s] sequence/offset number[%s] invalid",
              entry.getKey(),
              entry.getValue()
          )
      );
    }
  }

  // for backward compatibility
  public SeekableStreamPartitions(@NotNull final String id, final Map<T1, T2> partitionOffsetMap)
  {
    this(id, null, partitionOffsetMap, null);
  }

  public String getId()
  {
    return id;
  }

  @JsonProperty
  public String getStream()
  {
    return id;
  }

  @JsonProperty
  public String getTopic()
  {
    return id;
  }

  public Map<T1, T2> getMap()
  {
    return map;
  }

  @JsonProperty
  public Map<T1, T2> getPartitionSequenceNumberMap()
  {
    return map;
  }

  @JsonProperty
  public Map<T1, T2> getPartitionOffsetMap()
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
    return Objects.equals(id, that.id) &&
           Objects.equals(map, that.map);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, map);
  }

  @Override
  public String toString()
  {
    return "SeekableStreamPartitions{" +
           "stream/topic='" + id + '\'' +
           ", partitionSequenceNumberMap/partitionOffsetMap=" + map +
           '}';
  }
}
