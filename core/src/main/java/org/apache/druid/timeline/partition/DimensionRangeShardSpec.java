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
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link ShardSpec} for partitioning based on ranges of one or more dimensions.
 */
public class DimensionRangeShardSpec implements ShardSpec
{
  public static final int UNKNOWN_NUM_CORE_PARTITIONS = -1;

  private final List<String> dimensions;
  @Nullable
  private final StringTuple start;
  @Nullable
  private final StringTuple end;
  private final int partitionNum;
  private final int numCorePartitions;

  private final String firstDimStart;
  private final String firstDimEnd;

  /**
   * @param dimensions   partition dimensions
   * @param start        inclusive start of this range
   * @param end          exclusive end of this range
   * @param partitionNum unique ID for this shard
   */
  @JsonCreator
  public DimensionRangeShardSpec(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("start") @Nullable StringTuple start,
      @JsonProperty("end") @Nullable StringTuple end,
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("numCorePartitions") @Nullable Integer numCorePartitions // nullable for backward compatibility
  )
  {
    Preconditions.checkArgument(partitionNum >= 0, "partitionNum >= 0");
    Preconditions.checkArgument(
        dimensions != null && !dimensions.isEmpty(),
        "dimensions should be non-null and non-empty"
    );

    this.dimensions = dimensions;
    this.start = start;
    this.end = end;
    this.partitionNum = partitionNum;
    this.numCorePartitions = numCorePartitions == null ? UNKNOWN_NUM_CORE_PARTITIONS : numCorePartitions;
    this.firstDimStart = getFirstValueOrNull(start);
    this.firstDimEnd = getFirstValueOrNull(end);
  }

  @JsonProperty("dimensions")
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Nullable
  @JsonProperty("start")
  public StringTuple getStartTuple()
  {
    return start;
  }

  @Nullable
  @JsonProperty("end")
  public StringTuple getEndTuple()
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

  public boolean isNumCorePartitionsUnknown()
  {
    return numCorePartitions == UNKNOWN_NUM_CORE_PARTITIONS;
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
  {
    return createLookup(shardSpecs);
  }

  private static ShardSpecLookup createLookup(List<? extends ShardSpec> shardSpecs)
  {
    return (long timestamp, InputRow row) -> {
      for (ShardSpec spec : shardSpecs) {
        if (((DimensionRangeShardSpec) spec).isInChunk(row)) {
          return spec;
        }
      }
      throw new ISE("row[%s] doesn't fit in any shard[%s]", row, shardSpecs);
    };
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return Collections.unmodifiableList(dimensions);
  }

  private Range<String> getFirstDimRange()
  {
    Range<String> range;
    if (firstDimStart == null && firstDimEnd == null) {
      range = Range.all();
    } else if (firstDimStart == null) {
      range = Range.atMost(firstDimEnd);
    } else if (firstDimEnd == null) {
      range = Range.atLeast(firstDimStart);
    } else {
      range = Range.closed(firstDimStart, firstDimEnd);
    }
    return range;
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    RangeSet<String> rangeSet = domain.get(dimensions.get(0));
    if (rangeSet == null) {
      return true;
    }
    return !rangeSet.subRangeSet(getFirstDimRange()).isEmpty();
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    if (isNumCorePartitionsUnknown()) {
      return StringPartitionChunk.make(start, end, partitionNum, obj);
    } else {
      return new NumberedPartitionChunk<>(partitionNum, numCorePartitions, obj);
    }
  }

  private boolean isInChunk(InputRow inputRow)
  {
    return isInChunk(dimensions, start, end, inputRow);
  }

  public static boolean isInChunk(
      List<String> dimensions,
      @Nullable StringTuple start,
      @Nullable StringTuple end,
      InputRow inputRow
  )
  {
    final String[] inputDimensionValues = new String[dimensions.size()];
    for (int i = 0; i < dimensions.size(); ++i) {
      // Get the values of this dimension, treat multiple values as null
      List<String> values = inputRow.getDimension(dimensions.get(i));
      inputDimensionValues[i] = values != null && values.size() == 1 ? values.get(0) : null;
    }
    final StringTuple inputRowTuple = StringTuple.create(inputDimensionValues);

    int inputVsStart = inputRowTuple.compareTo(start);
    int inputVsEnd = inputRowTuple.compareTo(end);

    return (inputVsStart >= 0 || start == null)
           && (inputVsEnd < 0 || end == null);
  }

  private static String getFirstValueOrNull(StringTuple values)
  {
    return values != null && values.size() > 0 ? values.get(0) : null;
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
    DimensionRangeShardSpec shardSpec = (DimensionRangeShardSpec) o;
    return partitionNum == shardSpec.partitionNum &&
           numCorePartitions == shardSpec.numCorePartitions &&
           Objects.equals(dimensions, shardSpec.dimensions) &&
           Objects.equals(start, shardSpec.start) &&
           Objects.equals(end, shardSpec.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions, start, end, partitionNum, numCorePartitions);
  }

  @Override
  public String toString()
  {
    return "DimensionRangeShardSpec{" +
           "dimensions='" + dimensions + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           ", partitionNum=" + partitionNum +
           ", numCorePartitions=" + numCorePartitions +
           '}';
  }
}
