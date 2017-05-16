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

package io.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;


public abstract class AbstractPartitionsSpec implements PartitionsSpec
{
  private static final double DEFAULT_OVERSIZE_THRESHOLD = 1.5;
  private static final long DEFAULT_TARGET_PARTITION_SIZE = -1;

  private final long targetPartitionSize;
  private final long maxPartitionSize;
  private final boolean assumeGrouped;
  private final int numShards;

  public AbstractPartitionsSpec(
      Long targetPartitionSize,
      Long maxPartitionSize,
      Boolean assumeGrouped,
      Integer numShards
  )
  {
    this.targetPartitionSize = targetPartitionSize == null ? DEFAULT_TARGET_PARTITION_SIZE : targetPartitionSize;
    this.maxPartitionSize = maxPartitionSize == null
                            ? (long) (this.targetPartitionSize * DEFAULT_OVERSIZE_THRESHOLD)
                            : maxPartitionSize;
    this.assumeGrouped = assumeGrouped == null ? false : assumeGrouped;
    this.numShards = numShards == null ? -1 : numShards;
    Preconditions.checkArgument(
        this.targetPartitionSize == -1 || this.numShards == -1,
        "targetPartitionsSize and shardCount both cannot be set"
    );
  }

  @Override
  @JsonProperty
  public long getTargetPartitionSize()
  {
    return targetPartitionSize;
  }

  @Override
  @JsonProperty
  public long getMaxPartitionSize()
  {
    return maxPartitionSize;
  }

  @Override
  @JsonProperty
  public boolean isAssumeGrouped()
  {
    return assumeGrouped;
  }

  @Override
  public boolean isDeterminingPartitions()
  {
    return targetPartitionSize > 0;
  }

  @Override
  public int getNumShards()
  {
    return numShards;
  }
}
