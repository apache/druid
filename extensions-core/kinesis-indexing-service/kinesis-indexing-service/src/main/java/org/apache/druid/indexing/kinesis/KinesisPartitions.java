/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

public class KinesisPartitions
{
  public static final String NO_END_SEQUENCE_NUMBER = "NO_END_SEQUENCE_NUMBER";

  private final String stream;
  private final Map<String, String> partitionSequenceNumberMap;

  @JsonCreator
  public KinesisPartitions(
      @JsonProperty("stream") final String stream,
      @JsonProperty("partitionSequenceNumberMap") final Map<String, String> partitionSequenceNumberMap
  )
  {
    this.stream = stream;
    this.partitionSequenceNumberMap = ImmutableMap.copyOf(partitionSequenceNumberMap);

    // Validate partitionSequenceNumberMap
    for (Map.Entry<String, String> entry : partitionSequenceNumberMap.entrySet()) {
      Preconditions.checkArgument(
          entry.getValue() != null,
          String.format(
              "partition[%s] sequenceNumber[%s] invalid",
              entry.getKey(),
              entry.getValue()
          )
      );
    }
  }

  @JsonProperty
  public String getStream()
  {
    return stream;
  }

  @JsonProperty
  public Map<String, String> getPartitionSequenceNumberMap()
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
    KinesisPartitions that = (KinesisPartitions) o;
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
    return "KinesisPartitions{" +
           "stream='" + stream + '\'' +
           ", partitionSequenceNumberMap=" + partitionSequenceNumberMap +
           '}';
  }
}
