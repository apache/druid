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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class PartitionsSpec
{
  private static final double DEFAULT_OVERSIZE_THRESHOLD = 1.5;

  @Nullable
  private final String partitionDimension;

  private final long targetPartitionSize;

  private final long maxPartitionSize;

  private final boolean assumeGrouped;

  @JsonCreator
  public PartitionsSpec(
      @JsonProperty("partitionDimension") @Nullable String partitionDimension,
      @JsonProperty("targetPartitionSize") @Nullable Long targetPartitionSize,
      @JsonProperty("maxPartitionSize") @Nullable Long maxPartitionSize,
      @JsonProperty("assumeGrouped") @Nullable Boolean assumeGrouped
  )
  {
    this.partitionDimension = partitionDimension;
    this.targetPartitionSize = targetPartitionSize == null ? -1 : targetPartitionSize;
    this.maxPartitionSize = maxPartitionSize == null
                            ? (long) (this.targetPartitionSize * DEFAULT_OVERSIZE_THRESHOLD)
                            : maxPartitionSize;
    this.assumeGrouped = assumeGrouped == null ? false : assumeGrouped;
  }

  @JsonIgnore
  public boolean isDeterminingPartitions()
  {
    return targetPartitionSize > 0;
  }

  @JsonProperty
  @Nullable
  public String getPartitionDimension()
  {
    return partitionDimension;
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
}
