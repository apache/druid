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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.util.Objects;

public class SingleDimensionShardSpecFactory implements ShardSpecFactory
{
  private final String partitionDimension;
  private final int numBuckets;
  @Nullable
  private final String start;
  @Nullable
  private final String end;

  @JsonCreator
  public SingleDimensionShardSpecFactory(
      @JsonProperty("partitionDimension") String partitionDimension,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("start") @Nullable String start,
      @JsonProperty("end") @Nullable String end
  )
  {
    this.partitionDimension = partitionDimension;
    this.numBuckets = numBuckets;
    this.start = start;
    this.end = end;
  }

  @JsonProperty
  public String getPartitionDimension()
  {
    return partitionDimension;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty
  public @Nullable String getStart()
  {
    return start;
  }

  @JsonProperty
  public @Nullable String getEnd()
  {
    return end;
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, @Nullable ShardSpec specOfPreviousMaxPartitionId)
  {
    final int partitionId;
    if (specOfPreviousMaxPartitionId != null) {
      assert specOfPreviousMaxPartitionId instanceof SingleDimensionShardSpec;
      final SingleDimensionShardSpec prevSpec = (SingleDimensionShardSpec) specOfPreviousMaxPartitionId;
      partitionId = prevSpec.getPartitionNum() + 1;
    } else {
      partitionId = 0;
    }
    return create(objectMapper, partitionId);
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, int partitionId)
  {
    // TODO: numBuckets should be added to SingleDimensionShardSpec in a follow-up PR.
    return new SingleDimensionShardSpec(partitionDimension, start, end, partitionId);
  }

  @Override
  public Class<? extends ShardSpec> getShardSpecClass()
  {
    return SingleDimensionShardSpec.class;
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
    SingleDimensionShardSpecFactory that = (SingleDimensionShardSpecFactory) o;
    return numBuckets == that.numBuckets &&
           Objects.equals(partitionDimension, that.partitionDimension) &&
           Objects.equals(start, that.start) &&
           Objects.equals(end, that.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionDimension, numBuckets, start, end);
  }
}
