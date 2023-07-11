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

import com.google.common.collect.Ordering;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public abstract class BaseDimensionRangeShardSpec implements ShardSpec
{
  protected final List<String> dimensions;
  @Nullable
  protected final StringTuple start;
  @Nullable
  protected final StringTuple end;

  protected BaseDimensionRangeShardSpec(
      List<String> dimensions,
      @Nullable StringTuple start,
      @Nullable StringTuple end
  )
  {
    this.dimensions = dimensions;
    this.start = start;
    this.end = end;
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
  {
    return createLookup(dimensions, shardSpecs);
  }

  private static ShardSpecLookup createLookup(List<String> dimensions, List<? extends ShardSpec> shardSpecs)
  {
    BaseDimensionRangeShardSpec[] rangeShardSpecs = new BaseDimensionRangeShardSpec[shardSpecs.size()];
    for (int i = 0; i < shardSpecs.size(); i++) {
      rangeShardSpecs[i] = (BaseDimensionRangeShardSpec) shardSpecs.get(i);
    }
    final Comparator<StringTuple> startComparator = Comparators.naturalNullsFirst();
    final Comparator<StringTuple> endComparator = Ordering.natural().nullsLast();

    final Comparator<BaseDimensionRangeShardSpec> shardSpecComparator = Comparator
        .comparing((BaseDimensionRangeShardSpec spec) -> spec.start, startComparator)
        .thenComparing(spec -> spec.end, endComparator);

    Arrays.sort(rangeShardSpecs, shardSpecComparator);

    return (long timestamp, InputRow row) -> {
      StringTuple inputRowTuple = getInputRowTuple(dimensions, row);
      int startIndex = 0;
      int endIndex = shardSpecs.size() - 1;
      while (startIndex <= endIndex) {
        int mid = (startIndex + endIndex) >>> 1;
        BaseDimensionRangeShardSpec rangeShardSpec = rangeShardSpecs[mid];
        if (startComparator.compare(inputRowTuple, rangeShardSpec.start) < 0) {
          endIndex = mid - 1;
        } else if (endComparator.compare(inputRowTuple, rangeShardSpec.end) < 0) {
          return rangeShardSpec;
        } else {
          startIndex = mid + 1;
        }
      }
      throw new ISE("row[%s] doesn't fit in any shard[%s]", row, shardSpecs);
    };
  }

  protected static StringTuple getInputRowTuple(List<String> dimensions, InputRow inputRow)
  {
    final String[] inputDimensionValues = new String[dimensions.size()];
    for (int i = 0; i < dimensions.size(); ++i) {
      // Get the values of this dimension, treat multiple values as null
      List<String> values = inputRow.getDimension(dimensions.get(i));
      inputDimensionValues[i] = values != null && values.size() == 1 ? values.get(0) : null;
    }

    return StringTuple.create(inputDimensionValues);
  }
}
