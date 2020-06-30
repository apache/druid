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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class SingleDimensionShardSpecTest
{
  @Test
  public void testIsInChunk()
  {
    Map<SingleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>> tests = ImmutableMap.<SingleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>>builder()
        .put(
            makeSpec(null, null),
            makeListOfPairs(
                true, null,
                true, "a",
                true, "h",
                true, "p",
                true, "y"
            )
        )
        .put(
            makeSpec(null, "m"),
            makeListOfPairs(
                true, null,
                true, "a",
                true, "h",
                false, "p",
                false, "y"
            )
        )
        .put(
            makeSpec("a", "h"),
            makeListOfPairs(
                false, null,
                true, "a",
                false, "h",
                false, "p",
                false, "y"
            )
        )
        .put(
            makeSpec("d", "u"),
            makeListOfPairs(
                false, null,
                false, "a",
                true, "h",
                true, "p",
                false, "y"
            )
        )
        .put(
            makeSpec("h", null),
            makeListOfPairs(
                false, null,
                false, "a",
                true, "h",
                true, "p",
                true, "y"
            )
        )
        .build();

    for (Map.Entry<SingleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>> entry : tests.entrySet()) {
      SingleDimensionShardSpec spec = entry.getKey();
      for (Pair<Boolean, Map<String, String>> pair : entry.getValue()) {
        final InputRow inputRow = new MapBasedInputRow(
            0,
            ImmutableList.of("billy"),
            Maps.transformValues(pair.rhs, input -> input)
        );
        Assert.assertEquals(StringUtils.format("spec[%s], row[%s]", spec, inputRow), pair.lhs, spec.isInChunk(inputRow.getTimestampFromEpoch(), inputRow));
      }
    }
  }

  @Test
  public void testPossibleInDomain()
  {
    Map<String, RangeSet<String>> domain1 = ImmutableMap.of("dim1", rangeSet(ImmutableList.of(Range.lessThan("abc"))));
    Map<String, RangeSet<String>> domain2 = ImmutableMap.of("dim1", rangeSet(ImmutableList.of(Range.singleton("e"))),
                                                            "dim2", rangeSet(ImmutableList.of(Range.singleton("na")))
    );
    ShardSpec shard1 = makeSpec("dim1", null, "abc");
    ShardSpec shard2 = makeSpec("dim1", "abc", "def");
    ShardSpec shard3 = makeSpec("dim1", "def", null);
    ShardSpec shard4 = makeSpec("dim2", null, "hello");
    ShardSpec shard5 = makeSpec("dim2", "hello", "jk");
    ShardSpec shard6 = makeSpec("dim2", "jk", "na");
    ShardSpec shard7 = makeSpec("dim2", "na", null);
    Assert.assertTrue(shard1.possibleInDomain(domain1));
    Assert.assertFalse(shard2.possibleInDomain(domain1));
    Assert.assertFalse(shard3.possibleInDomain(domain1));
    Assert.assertTrue(shard4.possibleInDomain(domain1));
    Assert.assertTrue(shard5.possibleInDomain(domain1));
    Assert.assertTrue(shard6.possibleInDomain(domain1));
    Assert.assertTrue(shard7.possibleInDomain(domain1));
    Assert.assertFalse(shard1.possibleInDomain(domain2));
    Assert.assertFalse(shard2.possibleInDomain(domain2));
    Assert.assertTrue(shard3.possibleInDomain(domain2));
    Assert.assertFalse(shard4.possibleInDomain(domain2));
    Assert.assertFalse(shard5.possibleInDomain(domain2));
    Assert.assertTrue(shard6.possibleInDomain(domain2));
    Assert.assertTrue(shard7.possibleInDomain(domain2));
  }

  @Test
  public void testSharePartitionSpace()
  {
    final SingleDimensionShardSpec shardSpec = makeSpec("start", "end");
    Assert.assertTrue(shardSpec.sharePartitionSpace(NumberedPartialShardSpec.instance()));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new HashBasedNumberedPartialShardSpec(null, 0, 1)));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new SingleDimensionPartialShardSpec("dim", 0, null, null, 1)));
    Assert.assertFalse(shardSpec.sharePartitionSpace(new NumberedOverwritePartialShardSpec(0, 2, 1)));
  }

  private static RangeSet<String> rangeSet(List<Range<String>> ranges)
  {
    ImmutableRangeSet.Builder<String> builder = ImmutableRangeSet.builder();
    for (Range<String> range : ranges) {
      builder.add(range);
    }
    return builder.build();
  }

  private SingleDimensionShardSpec makeSpec(String start, String end)
  {
    return makeSpec("billy", start, end);
  }

  private SingleDimensionShardSpec makeSpec(String dimension, String start, String end)
  {
    return new SingleDimensionShardSpec(dimension, start, end, 0, SingleDimensionShardSpec.UNKNOWN_NUM_CORE_PARTITIONS);
  }

  private Map<String, String> makeMap(String value)
  {
    return value == null ? ImmutableMap.of() : ImmutableMap.of("billy", value);
  }

  private List<Pair<Boolean, Map<String, String>>> makeListOfPairs(Object... arguments)
  {
    Preconditions.checkState(arguments.length % 2 == 0);

    final ArrayList<Pair<Boolean, Map<String, String>>> retVal = new ArrayList<>();

    for (int i = 0; i < arguments.length; i += 2) {
      retVal.add(Pair.of((Boolean) arguments[i], makeMap((String) arguments[i + 1])));
    }

    return retVal;
  }

}
