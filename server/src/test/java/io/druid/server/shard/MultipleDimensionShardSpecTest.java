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

package io.druid.server.shard;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.Pair;
import io.druid.timeline.partition.MultipleDimensionShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultipleDimensionShardSpecTest
{
  final List<String> dimensions = Arrays.asList("project", "event_id");

  @Test
  public void testIsInChunk()
  {
    Map<MultipleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>> tests = ImmutableMap.<MultipleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>>builder()
        .put(
            makeSpecV2(Arrays.asList(null, null), Arrays.asList(null, null)),
            makeList(
                true, null, null,
                true, "1", "10",
                true, "2", "12",
                true, null, "12",
                true, "1", null
            )
        )
        .put(
            makeSpecV2(Arrays.asList(null, null), Arrays.asList("20", "12")),
            makeList(
                true, null, null,
                true, "1", "10",
                true, "20", "11",
                true, "2", "12",
                true, null, "12",
                false, "20", "12"
            )
        )
        .put(
            makeSpecV2(Arrays.asList("20", "12"), Arrays.asList(null, null)),
            makeList(
                false, null, null,
                true, "20", "12",
                false, "20", null
            )
        )
        .put(
            makeSpecV2(Arrays.asList("2", "25"), Arrays.asList("20", "27")),
            makeList(
                false, null, null,
                false, "1", "10",
                false, "2", "12",
                true, "2", "26",
                true, "2", "25",
                false, "30", "10",
                true, "20", "26",
                false, "20", "27",
                false, null, "26",
                true, "20", "12"
            )
        )
        .put(
            makeSpecV2(Arrays.asList("3", "25"), Arrays.asList("5", "25")),
            makeList(
                false, null, null,
                false, "1", "10",
                false, "2", "12",
                true, "3", "25",
                true, "4", "-1",
                true, "5", "24",
                false, "3", null
            )
        )
        .put(
            makeSpecV2(Arrays.asList("95699", "15720794"), Arrays.asList(null, null)),
            makeList(
                true, "96588", "-1"
            )
        )
        .put(
            makeSpecV2(Arrays.asList("618988", "15594345"), Arrays.asList("621824", "13152975")),
            makeList(
                true, "621824", "-1",
                true, "621824", "13149927",
                false, "-1", "13149927"
            )
        )
        .put(
            makeSpecV2(Arrays.asList("0", null), Arrays.asList("1", null)),
            makeList(
                false, "1", "2"
            )
        )
        .put(
            makeSpecV2(Arrays.asList("1", null), Arrays.asList("10", "10")),
            makeList(
                true, "1", "2"
            )
        )
        .build();

    for (Map.Entry<MultipleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>> entry : tests.entrySet()) {
      MultipleDimensionShardSpec shardSpec = entry.getKey();
      for (Pair<Boolean, Map<String, String>> pair : entry.getValue()) {
        final InputRow inputRow = new MapBasedInputRow(
            0, dimensions, Maps.transformValues(
            pair.rhs, input -> input
        )
        );
        Assert.assertEquals(
            String.format("spec[%s], row[%s]", shardSpec, inputRow),
            pair.lhs,
            shardSpec.isInChunk(inputRow.getTimestampFromEpoch(), inputRow)
        );
      }
    }
  }

  @Test
  public void testDomain()
  {
    List<String> dims = ImmutableList.of("dim1", "dim2", "dim3");
    List<Range> rangeList = ImmutableList.of(
        Range.closed("1", "3").span(Range.singleton("4")),
        Range.singleton("100").span(Range.singleton("1")),
        Range.closed("2", "25")
    );
    MultipleDimensionShardSpec multiDim = new MultipleDimensionShardSpec(
        dims,
        Range.closedOpen("1,5,9", "4,1,2"),
        rangeList,
        1
    );

    Map<String, RangeSet<String>> domain = multiDim.getDomain();

    Map<String, RangeSet<String>> expected = new HashMap<>();
    int i = 0;
    for (String dim : dims) {
      RangeSet<String> rangeSet = TreeRangeSet.create();
      rangeSet.add(rangeList.get(i));
      expected.put(dim, rangeSet);
      i++;
    }

    Assert.assertEquals(
        String.format("domain[%s], rangeSet[%s]", domain, expected),
        expected,
        domain
    );
  }

  private MultipleDimensionShardSpec makeSpec(List<String> startList, List<String> endList)
  {
    return new MultipleDimensionShardSpec(
        dimensions,
        MultipleDimensionShardSpec.computeRange(startList, endList),
        null,
        0
    );
  }

  private MultipleDimensionShardSpec makeSpecV2(List<String> startList, List<String> endList)
  {
    return new MultipleDimensionShardSpec(
        dimensions,
        MultipleDimensionShardSpec.computeRange(startList, endList),
        null,
        0);
  }

  private Map<String, String> makeMap(String value1, String value2)
  {
    if (value1 == null && value2 == null) {
      return ImmutableMap.<String, String>of();
    }

    if (value1 == null) {
      return ImmutableMap.of(dimensions.get(1), value2);
    }

    if (value2 == null) {
      return ImmutableMap.of(dimensions.get(0), value1);
    }

    return ImmutableMap.of(dimensions.get(0), value1, dimensions.get(1), value2);
  }

  private List<Pair<Boolean, Map<String, String>>> makeList(Object... arguments)
  {
    Preconditions.checkState(arguments.length % 3 == 0);

    final ArrayList<Pair<Boolean, Map<String, String>>> retVal = Lists.newArrayList();

    for (int i = 0; i < arguments.length; i += 3) {
      retVal.add(Pair.of((Boolean) arguments[i], makeMap((String) arguments[i + 1], (String) arguments[i + 2])));
    }

    return retVal;
  }
}
