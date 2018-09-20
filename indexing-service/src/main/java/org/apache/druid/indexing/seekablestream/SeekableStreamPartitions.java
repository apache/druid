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

import java.util.Map;
import java.util.Objects;

// TODO: may consider deleting Kinesis and KafaPartitions classes and just use this instead
public abstract class SeekableStreamPartitions<T1, T2>
{
  private final String id;
  private final Map<T1, T2> partitionSequenceMap;

  @JsonCreator
  public SeekableStreamPartitions(
      @JsonProperty("id") final String id,
      @JsonProperty("partitionSequenceMap") final Map<T1, T2> partitionOffsetMap
  )
  {
    this.id = id;
    this.partitionSequenceMap = ImmutableMap.copyOf(partitionOffsetMap);
    // Validate partitionSequenceNumberMap
    for (Map.Entry<T1, T2> entry : partitionOffsetMap.entrySet()) {
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

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public Map<T1, T2> getPartitionSequenceMap()
  {
    return partitionSequenceMap;
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
           Objects.equals(partitionSequenceMap, that.partitionSequenceMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, partitionSequenceMap);
  }

  @Override
  public String toString()
  {
    return "SeekableStreamPartitions{" +
           "stream/topic='" + id + '\'' +
           ", partitionSequenceMap=" + partitionSequenceMap +
           '}';
  }

  public abstract T2 getNoEndSequenceNumber();
}
