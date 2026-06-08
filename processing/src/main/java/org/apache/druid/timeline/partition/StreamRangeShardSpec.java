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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link NumberedShardSpec} that additionally declares, per dimension, the set of values a streaming segment
 * contains ({@link #partitionFilters}), letting the broker prune segments whose values cannot match a query filter
 * before compaction. A dimension absent from {@link #partitionFilters} is not pruned on.
 */
public class StreamRangeShardSpec extends NumberedShardSpec
{
  /**
   * Maps dimension name → exhaustive list of values that can appear in this shard for that dimension.
   * An absent dimension means "all values possible" (no pruning on that dimension).
   */
  private final Map<String, List<String>> partitionFilters;

  @JsonCreator
  public StreamRangeShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("partitionFilters") @Nullable Map<String, List<String>> partitionFilters
  )
  {
    super(partitionNum, partitions);
    this.partitionFilters = partitionFilters == null ? Collections.emptyMap() : partitionFilters;
  }

  @JsonProperty("partitionFilters")
  public Map<String, List<String>> getPartitionFilters()
  {
    return partitionFilters;
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return ImmutableList.copyOf(partitionFilters.keySet());
  }

  /**
   * Returns false only when the query filter explicitly constrains a dimension that this shard declares,
   * and none of this shard's allowed values for that dimension fall within the filter domain.
   *
   * <p>A null entry in a dimension's allowed-values list denotes a row whose value was null/missing. Druid encodes a
   * null match in the query domain as the range {@code (-inf, "")} (see e.g. {@code NullFilter}), so a declared null
   * is tested against the domain as {@link Range#lessThan} {@code ""} rather than as a point value. Every other value
   * (including the empty string {@code ""}) is tested as a singleton point, keeping null and {@code ""} distinct.
   *
   * @return true if segment needs to be considered for query, false if it can be pruned
   */
  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    if (partitionFilters.isEmpty()) {
      return true;
    }

    for (Map.Entry<String, List<String>> entry : partitionFilters.entrySet()) {
      final String dimension = entry.getKey();
      final List<String> allowedValues = entry.getValue();

      final RangeSet<String> domainRangeSet = domain.get(dimension);
      if (domainRangeSet == null || domainRangeSet.isEmpty()) {
        // Query doesn't constrain this dimension — cannot prune on it.
        continue;
      }

      boolean anyMatch = false;
      for (String value : allowedValues) {
        // Null is represented in the domain as the range (-inf, ""); any other value as a singleton point.
        final Range<String> valueRange = value == null ? Range.lessThan("") : Range.singleton(value);
        if (!domainRangeSet.subRangeSet(valueRange).isEmpty()) {
          anyMatch = true;
          break;
        }
      }
      if (!anyMatch) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String getType()
  {
    return Type.STREAM_RANGE;
  }

  @Override
  public ShardSpec withPartitionNum(int partitionNum)
  {
    return new StreamRangeShardSpec(partitionNum, getNumCorePartitions(), partitionFilters);
  }

  @Override
  public ShardSpec withCorePartitions(int partitions)
  {
    return new StreamRangeShardSpec(getPartitionNum(), partitions, partitionFilters);
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
    if (!super.equals(o)) {
      return false;
    }
    StreamRangeShardSpec that = (StreamRangeShardSpec) o;
    return Objects.equals(partitionFilters, that.partitionFilters);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), partitionFilters);
  }

  @Override
  public String toString()
  {
    return "StreamRangeShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getNumCorePartitions() +
           ", partitionFilters=" + partitionFilters +
           '}';
  }
}
