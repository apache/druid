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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;

import java.util.Map;
import java.util.Objects;

public class KafkaPartitions
{
  private final String topic;
  private final Map<Integer, Long> partitionOffsetMap;

  @JsonCreator
  public KafkaPartitions(
      @JsonProperty("topic") final String topic,
      @JsonProperty("partitionOffsetMap") final Map<Integer, Long> partitionOffsetMap
  )
  {
    this.topic = topic;
    this.partitionOffsetMap = ImmutableMap.copyOf(partitionOffsetMap);

    // Validate partitionOffsetMap
    for (Map.Entry<Integer, Long> entry : partitionOffsetMap.entrySet()) {
      Preconditions.checkArgument(
          entry.getValue() >= 0,
          StringUtils.format(
              "partition[%d] offset[%d] invalid",
              entry.getKey(),
              entry.getValue()
          )
      );
    }
  }

  @JsonProperty
  public String getTopic()
  {
    return topic;
  }

  @JsonProperty
  public Map<Integer, Long> getPartitionOffsetMap()
  {
    return partitionOffsetMap;
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
    KafkaPartitions that = (KafkaPartitions) o;
    return Objects.equals(topic, that.topic) &&
           Objects.equals(partitionOffsetMap, that.partitionOffsetMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(topic, partitionOffsetMap);
  }

  @Override
  public String toString()
  {
    return "KafkaPartitions{" +
           "topic='" + topic + '\'' +
           ", partitionOffsetMap=" + partitionOffsetMap +
           '}';
  }
}
