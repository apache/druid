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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link ShardSpec} for range partitioning based on a single dimension
 */
public class SingleDimensionShardSpec implements ShardSpec
{
  public static final int UNKNOWN_NUM_CORE_PARTITIONS = -1;

  private final String dimension;
  @Nullable
  private final String start;
  @Nullable
  private final String end;
  private final int partitionNum;
  private final int numCorePartitions;

  /**
   * @param dimension    partition dimension
   * @param start        inclusive start of this range
   * @param end          exclusive end of this range
   * @param partitionNum unique ID for this shard
   */
  @JsonCreator
  public SingleDimensionShardSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("start") @Nullable String start,
      @JsonProperty("end") @Nullable String end,
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("numCorePartitions") @Nullable Integer numCorePartitions // nullable for backward compatibility
  )
  {
    Preconditions.checkArgument(partitionNum >= 0, "partitionNum >= 0");
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.start = start;
    this.end = end;
    this.partitionNum = partitionNum;
    this.numCorePartitions = numCorePartitions == null ? UNKNOWN_NUM_CORE_PARTITIONS : numCorePartitions;
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return dimension;
  }

  @Nullable
  @JsonProperty("start")
  public String getStart()
  {
    return start;
  }

  @Nullable
  @JsonProperty("end")
  public String getEnd()
  {
    return end;
  }

  @Override
  @JsonProperty("partitionNum")
  public int getPartitionNum()
  {
    return partitionNum;
  }

  @Override
  @JsonProperty
  public int getNumCorePartitions()
  {
    return numCorePartitions;
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
  {
    return createLookup(shardSpecs);
  }

  static ShardSpecLookup createLookup(List<? extends ShardSpec> shardSpecs)
  {
    return (long timestamp, InputRow row) -> {
      for (ShardSpec spec : shardSpecs) {
        if (spec.isInChunk(timestamp, row)) {
          return spec;
        }
      }
      throw new ISE("row[%s] doesn't fit in any shard[%s]", row, shardSpecs);
    };
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return ImmutableList.of(dimension);
  }

  private Range<String> getRange()
  {
    Range<String> range;
    if (start == null && end == null) {
      range = Range.all();
    } else if (start == null) {
      range = Range.atMost(end);
    } else if (end == null) {
      range = Range.atLeast(start);
    } else {
      range = Range.closed(start, end);
    }
    return range;
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    RangeSet<String> rangeSet = domain.get(dimension);
    if (rangeSet == null) {
      return true;
    }
    return !rangeSet.subRangeSet(getRange()).isEmpty();
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    if (numCorePartitions == UNKNOWN_NUM_CORE_PARTITIONS) {
      return new StringPartitionChunk<>(start, end, partitionNum, obj);
    } else {
      return new NumberedPartitionChunk<>(partitionNum, numCorePartitions, obj);
    }
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return isInChunk(dimension, start, end, inputRow);
  }

  private static boolean checkValue(@Nullable String start, @Nullable String end, String value)
  {
    if (value == null) {
      return start == null;
    }

    if (start == null) {
      return end == null || value.compareTo(end) < 0;
    }

    return value.compareTo(start) >= 0 &&
           (end == null || value.compareTo(end) < 0);
  }

  public static boolean isInChunk(
      String dimension,
      @Nullable String start,
      @Nullable String end,
      InputRow inputRow
  )
  {
    final List<String> values = inputRow.getDimension(dimension);

    if (values == null || values.size() != 1) {
      return checkValue(start, end, null);
    } else {
      return checkValue(start, end, values.get(0));
    }
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
    SingleDimensionShardSpec shardSpec = (SingleDimensionShardSpec) o;
    return partitionNum == shardSpec.partitionNum &&
           numCorePartitions == shardSpec.numCorePartitions &&
           Objects.equals(dimension, shardSpec.dimension) &&
           Objects.equals(start, shardSpec.start) &&
           Objects.equals(end, shardSpec.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, start, end, partitionNum, numCorePartitions);
  }

  @Override
  public String toString()
  {
    return "SingleDimensionShardSpec{" +
           "dimension='" + dimension + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           ", partitionNum=" + partitionNum +
           ", numCorePartitions=" + numCorePartitions +
           '}';
  }
}
