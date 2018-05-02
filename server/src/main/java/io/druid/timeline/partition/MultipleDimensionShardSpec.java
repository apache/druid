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

package io.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Class uses getters/setters to work around http://jira.codehaus.org/browse/MSHADE-92
 *
 * Adjust to JsonCreator and final fields when resolved.
 */
public class MultipleDimensionShardSpec implements ShardSpec
{
  public static final Joiner commaJoiner = Joiner.on(",").useForNull("");

  private List<String> dimensions;
  private Range range;
  private List<Range> dimensionMinMax;
  private int partitionNum;

  public MultipleDimensionShardSpec()
  {
    this(null, null, null, -1);
  }

  public MultipleDimensionShardSpec(
      List<String> dimensions,
      Range range,
      List<Range> dimensionMinMax,
      int partitionNum
  )
  {
    this.dimensions = dimensions;
    this.range = range;
    this.dimensionMinMax = dimensionMinMax;
    this.partitionNum = partitionNum;
  }

  @JsonProperty("dimensions")
  public List<String> getDimensions()
  {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions)
  {
    this.dimensions = dimensions;
  }

  @JsonProperty("range")
  public Range getRange()
  {
    return range;
  }

  public void setRange(Range range)
  {
    this.range = range;
  }

  @Override
  @JsonProperty("partitionNum")
  public int getPartitionNum()
  {
    return partitionNum;
  }

  public void setPartitionNum(int partitionNum)
  {
    this.partitionNum = partitionNum;
  }

  @JsonProperty("dimensionMinMax")
  public List<Range> getDimensionMinMax()
  {
    return dimensionMinMax;
  }

  public void setDimensionMinMax(List<Range> dimensionMinMax)
  {
    this.dimensionMinMax = dimensionMinMax;
  }

  @Override
  public ShardSpecLookup getLookup(final List<ShardSpec> shardSpecs)
  {
    return (long timestamp, InputRow row) -> {
      for (ShardSpec spec : shardSpecs) {
        if (spec.isInChunk(timestamp, row)) {
          return spec;
        }
      }
      throw new ISE("row[%s] doessn't fit in any shard[%s]", row, shardSpecs);
    };
  }

  @Override
  public Map<String, RangeSet<String>> getDomain()
  {
    if (dimensionMinMax == null || dimensionMinMax.isEmpty()) {
      return ImmutableMap.of();
    }

    Map<String, RangeSet<String>> domainMap = new HashMap<>();
    IntStream
        .range(0, dimensions.size())
        .forEach(i -> {
          RangeSet<String> rangeSet = TreeRangeSet.create();
          rangeSet.add(dimensionMinMax.get(i));
          domainMap.put(dimensions.get(i), rangeSet);
        });

    return domainMap;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new RangePartitionChunk<>(range, partitionNum, obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    List<String> dimVals = dimensions
        .stream()
        .map(dim -> (inputRow.getDimension(dim).size() == 0) ?
            null : Iterables.getOnlyElement(inputRow.getDimension(dim)))
        .collect(Collectors.toList());

    Range dimRange = createSingletonRange(dimVals);

    if (dimRange == null) {
      if (!range.hasLowerBound() || !range.hasUpperBound()) {
        return true;
      }

      return false;
    }

    return range.encloses(dimRange);
  }


  @Override
  public String toString()
  {
    return "MultipleDimensionShardSpec{" +
        " dimension='" + dimensions + '\'' +
        ", Range='" + range + '\'' +
        ", dimensionMinMax=" + dimensionMinMax + '\'' +
        ", partitionNum=" + partitionNum + '\'' +
        '}';
  }


  public static Range computeRange(List<String> start, List<String> end)
  {
    boolean isStartUnBounded = start == null || start.stream().allMatch(Predicates.isNull()::apply);
    boolean isEndUnBounded = end == null || end.stream().allMatch(Predicates.isNull()::apply);

    if (isStartUnBounded && isEndUnBounded) {
      return Range.all();
    } else if (isStartUnBounded) {
      return Range.lessThan(commaJoiner.join(end));
    } else if (isEndUnBounded) {
      return Range.atLeast(commaJoiner.join(start));
    }

    return Range.closedOpen(commaJoiner.join(start), commaJoiner.join(end));
  }

  public static Range computeRange(Range start, List<String> endList)
  {
    Range end = createSingletonRange(endList);

    boolean isStartUnBounded = start == null || !start.hasLowerBound();
    boolean isEndUnBounded = end == null;

    if (isStartUnBounded && isEndUnBounded) {
      return Range.all();
    } else if (isStartUnBounded) {
      return Range.lessThan(end.upperEndpoint());
    } else if (isEndUnBounded) {
      return Range.atLeast(start.lowerEndpoint());
    } else {
      return start.span(end);
    }
  }

  public static Range createSingletonRange(List<String> values)
  {
    if (values == null || values.isEmpty() || values.stream().allMatch(Predicates.isNull()::apply)) {
      return null;
    }

    return Range.singleton(commaJoiner.join(values));
  }
}
