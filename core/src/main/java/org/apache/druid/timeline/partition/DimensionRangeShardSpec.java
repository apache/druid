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
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.data.input.StringTuple;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link ShardSpec} for partitioning based on ranges of one or more dimensions.
 */
public class DimensionRangeShardSpec extends BaseDimensionRangeShardSpec
{
  public static final int UNKNOWN_NUM_CORE_PARTITIONS = -1;

  private final int partitionNum;
  private final int numCorePartitions;

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
    super(dimensions, start, end);
    Preconditions.checkArgument(partitionNum >= 0, "partitionNum >= 0");
    Preconditions.checkArgument(
        dimensions != null && !dimensions.isEmpty(),
        "dimensions should be non-null and non-empty"
    );

    this.partitionNum = partitionNum;
    this.numCorePartitions = numCorePartitions == null ? UNKNOWN_NUM_CORE_PARTITIONS : numCorePartitions;
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
  public List<String> getDomainDimensions()
  {
    return Collections.unmodifiableList(dimensions);
  }

  /**
   * Set[:i] is the cartesian product of Set[0],...,Set[i - 1]
   * EffectiveDomain[:i] is defined as QueryDomain[:i] INTERSECTION SegmentRange[:i]
   *
   * i = 1
   * If EffectiveDomain[:i] == {start[:i]} || EffectiveDomain == {end[:i]}:
   *  if i == index.dimensions.size:
   *    ACCEPT segment
   *  else:
   *    REPEAT with i = i + 1
   *else if EffectiveDomain[:i] == {}:
   *  PRUNE segment
   *else:
   *  ACCEPT segment
   *
   *
   * Example: Index on (Hour, Minute, Second). Index.size is 3
   * I)
   * start = (3, 25, 10)
   * end = (5, 10, 30)
   * query domain = {3} * [0, 10] * {10, 20, 30, 40}
   * EffectiveDomain[:1] == {3} == start[:1]
   * EffectiveDomain[:2] == {3} * ([0, 10] INTERSECTION [25, INF))
   *                     == {} -> PRUNE
   *
   * II)
   * start = (3, 25, 10)
   * end = (5, 15, 30)
   * query domain = {4} * [0, 10] * {10, 20, 30, 40}
   * EffectiveDomain[:1] == {4} (!= {} && != start[:1] && != {end[:1]}) -> ACCEPT
   *
   * III)
   * start = (3, 25, 10)
   * end = (5, 15, 30)
   * query domain = {5} * [0, 10] * {10, 20, 30, 40}
   * EffectiveDomain[:1] == {5} == end[:1]
   * EffectiveDomain[:2] == {5} * ([0, 10] INTERSECTION (-INF, 15])
   *                     == {5} * [0, 10] (! ={} && != {end[:2]}) -> ACCEPT
   *
   * IV)
   * start = (3, 25, 10)
   * end = (5, 15, 30)
   * query domain = {5} * [15, 40] * {10, 20, 30, 40}
   * EffectiveDomain[:1] == {5} == end[:1]
   * EffectiveDomain[:2] == {5} * ([15, 40] INTERSECTION (-INF, 15])
   *                     == {5} * {15} == {end[:2]}
   * EffectiveDomain[:3] == {5} * {15} * ({10, 20, 30, 40} * (-INF, 30])
   *                     == {5} * {15} * {10, 20, 30} != {}  -> ACCEPT
   *
   * V)
   * start = (3, 25, 10)
   * end = (5, 15, 30)
   * query domain = {5} * [15, 40] * {50}
   * EffectiveDomain[:1] == {5} == end[:1]
   * EffectiveDomain[:2] == {5} * ([15, 40] INTERSECTION (-INF, 15])
   *                     == {5} * {15} == {end[:2]}
   * EffectiveDomain[:3] == {5} * {15} * ({40} * (-INF, 30])
   *                     == {5} * {15} * {}
   *                     == {} -> PRUNE
   *
   * @param domain The domain inferred from the query. Assumed to be non-emtpy
   * @return true if segment needs to be considered for query, false if it can be pruned
   */
  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    final StringTuple segmentStart = start == null ? new StringTuple(new String[dimensions.size()]) : start;
    final StringTuple segmentEnd = end == null ? new StringTuple(new String[dimensions.size()]) : end;

    // Indicates if the effective domain is equivalent to {start} till the previous dimension
    boolean effectiveDomainIsStart = true;
    // Indicates if the effective domain is equivalent to {end} till the previous dimension
    boolean effectiveDomainIsEnd = true;

    for (int i = 0; i < dimensions.size(); i++) {
      String dimension = dimensions.get(i);
      RangeSet<String> queryDomainForDimension = domain.get(dimension);
      if (queryDomainForDimension == null) {
        queryDomainForDimension = TreeRangeSet.create();
        queryDomainForDimension.add(Range.all());
      }

      // Compute the segment's range for given dimension based on its start, end and boundary conditions
      Range<String> rangeTillSegmentBoundary = Range.all();
      if (effectiveDomainIsStart && segmentStart.get(i) != null) {
        rangeTillSegmentBoundary = rangeTillSegmentBoundary.intersection(Range.atLeast(segmentStart.get(i)));
      }
      if (effectiveDomainIsEnd && segmentEnd.get(i) != null) {
        rangeTillSegmentBoundary = rangeTillSegmentBoundary.intersection(Range.atMost(segmentEnd.get(i)));
      }

      // EffectiveDomain[i] = QueryDomain[i] INTERSECTION SegmentRange[i]
      RangeSet<String> effectiveDomainForDimension = queryDomainForDimension.subRangeSet(rangeTillSegmentBoundary);

      // Create an iterator to use for checking if the RangeSet is empty or is a singleton. This is significantly
      // faster than using isEmpty() and equals(), because those methods call size() internally, which iterates
      // the entire RangeSet.
      final Iterator<Range<String>> effectiveDomainRangeIterator = effectiveDomainForDimension.asRanges().iterator();

      // Prune segment because query domain is out of segment range
      if (!effectiveDomainRangeIterator.hasNext()) {
        return false;
      }

      final Range<String> firstRange = effectiveDomainRangeIterator.next();
      final boolean effectiveDomainIsSingleRange = !effectiveDomainRangeIterator.hasNext();

      // Effective domain contained only one Range.
      // If it's a singleton and lies only on the boundaries -> consider next dimensions
      effectiveDomainIsStart = effectiveDomainIsStart
                               && effectiveDomainIsSingleRange
                               && segmentStart.get(i) != null
                               && firstRange.equals(Range.singleton(segmentStart.get(i)));

      effectiveDomainIsEnd = effectiveDomainIsEnd
                             && effectiveDomainIsSingleRange
                             && segmentEnd.get(i) != null
                             && firstRange.equals(Range.singleton(segmentEnd.get(i)));

      // EffectiveDomain lies within the boundaries as well -> cannot prune based on next dimensions
      if (!effectiveDomainIsStart && !effectiveDomainIsEnd) {
        return true;
      }
    }
    return true;
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

  @Override
  public String getType()
  {
    return Type.RANGE;
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
