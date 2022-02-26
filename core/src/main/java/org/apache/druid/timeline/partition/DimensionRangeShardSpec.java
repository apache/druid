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

  /**
   * Check if a given domain of Strings is a singleton set containing the given value
   * @param rangeSet Domain of Strings
   * @param val Value of String
   * @return rangeSet == {val}
   */
  private boolean isRangeSetSingletonWithVal(RangeSet<String> rangeSet, String val)
  {
    if (val == null) {
      return false;
    }
    return rangeSet.asRanges().equals(
        Collections.singleton(Range.singleton(val))
    );
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
      // Prune segment because query domain is out of segment range
      if (effectiveDomainForDimension.isEmpty()) {
        return false;
      }

      // EffectiveDomain is singleton and lies only on the boundaries -> consider next dimensions
      effectiveDomainIsStart = effectiveDomainIsStart
                                && isRangeSetSingletonWithVal(effectiveDomainForDimension, segmentStart.get(i));
      effectiveDomainIsEnd = effectiveDomainIsEnd
                           && isRangeSetSingletonWithVal(effectiveDomainForDimension, segmentEnd.get(i));

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
