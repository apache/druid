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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.StringTuple;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link ShardSpec} for range partitioning based on a single dimension
 */
public class SingleDimensionShardSpec extends DimensionRangeShardSpec
{
  public static final int UNKNOWN_NUM_CORE_PARTITIONS = -1;

  private final String dimension;
  @Nullable
  private final String start;
  @Nullable
  private final String end;

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
    super(
        dimension == null ? Collections.emptyList() : Collections.singletonList(dimension),
        start == null ? null : StringTuple.create(start),
        end == null ? null : StringTuple.create(end),
        partitionNum,
        numCorePartitions
    );
    this.dimension = dimension;
    this.start = start;
    this.end = end;
  }

  /**
   * Returns a Map to be used for serializing objects of this class. This is to
   * ensure that a new field added in {@link DimensionRangeShardSpec} does
   * not get serialized when serializing a {@code SingleDimensionShardSpec}.
   *
   * @return A map containing only the keys {@code "dimension"}, {@code "start"},
   * {@code "end"}, {@code "partitionNum"} and {@code "numCorePartitions"}.
   */
  @JsonValue
  public Map<String, Object> getSerializableObject()
  {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("start", start);
    jsonMap.put("end", end);
    jsonMap.put("dimension", dimension);
    jsonMap.put("partitionNum", getPartitionNum());
    jsonMap.put("numCorePartitions", getNumCorePartitions());

    return jsonMap;
  }

  public String getDimension()
  {
    return dimension;
  }

  @Nullable
  public String getStart()
  {
    return start;
  }

  @Nullable
  public String getEnd()
  {
    return end;
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
    if (isNumCorePartitionsUnknown()) {
      return StringPartitionChunk.makeForSingleDimension(start, end, getPartitionNum(), obj);
    } else {
      return new NumberedPartitionChunk<>(getPartitionNum(), getNumCorePartitions(), obj);
    }
  }

  @Override
  public String getType()
  {
    return Type.SINGLE;
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
    return getPartitionNum() == shardSpec.getPartitionNum() &&
           getNumCorePartitions() == shardSpec.getNumCorePartitions() &&
           Objects.equals(dimension, shardSpec.dimension) &&
           Objects.equals(start, shardSpec.start) &&
           Objects.equals(end, shardSpec.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, start, end, getPartitionNum(), getNumCorePartitions());
  }

  @Override
  public String toString()
  {
    return "SingleDimensionShardSpec{" +
           "dimension='" + dimension + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           ", partitionNum=" + getPartitionNum() +
           ", numCorePartitions=" + getNumCorePartitions() +
           '}';
  }
}
