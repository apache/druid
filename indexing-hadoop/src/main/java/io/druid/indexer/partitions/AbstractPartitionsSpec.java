/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;


public abstract class AbstractPartitionsSpec implements PartitionsSpec
{
  private static final double DEFAULT_OVERSIZE_THRESHOLD = 1.5;
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
    this.targetPartitionSize = targetPartitionSize == null ? -1 : targetPartitionSize;
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

  @JsonProperty
  public long getTargetPartitionSize()
  {
    return targetPartitionSize;
  }

  @JsonProperty
  public long getMaxPartitionSize()
  {
    return maxPartitionSize;
  }

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
