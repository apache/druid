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

package io.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import io.druid.timeline.partition.ShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DimFilterUtilsTest
{
  private static final Function<ShardSpec, ShardSpec> CONVERTER = new Function<ShardSpec, ShardSpec>()
  {
    @Nullable
    @Override
    public ShardSpec apply(@Nullable ShardSpec input)
    {
      return input;
    }
  };

  @Test
  public void testFilterShards()
  {
    DimFilter filter1 = EasyMock.createMock(DimFilter.class);
    EasyMock.expect(filter1.getDimensionRangeSet("dim1"))
            .andReturn(rangeSet(ImmutableList.of(Range.lessThan("abc"))))
            .anyTimes();
    EasyMock.expect(filter1.getDimensionRangeSet("dim2"))
            .andReturn(null)
            .anyTimes();

    DimFilter filter2 = EasyMock.createMock(DimFilter.class);
    EasyMock.expect(filter2.getDimensionRangeSet("dim1"))
            .andReturn(rangeSet(ImmutableList.of(Range.singleton("e"))))
            .anyTimes();
    EasyMock.expect(filter2.getDimensionRangeSet("dim2"))
            .andReturn(rangeSet(ImmutableList.of(Range.singleton("na"))))
            .anyTimes();

    ShardSpec shard1 = shardSpec("dim1", Range.atMost("abc"));
    ShardSpec shard2 = shardSpec("dim1", Range.closed("abc", "def"));
    ShardSpec shard3 = shardSpec("dim1", Range.atLeast("def"));
    ShardSpec shard4 = shardSpec("dim2", Range.atMost("hello"));
    ShardSpec shard5 = shardSpec("dim2", Range.closed("hello", "jk"));
    ShardSpec shard6 = shardSpec("dim2", Range.closed("jk", "na"));
    ShardSpec shard7 = shardSpec("dim2", Range.atLeast("na"));

    List<ShardSpec> shards = ImmutableList.of(shard1, shard2, shard3, shard4, shard5, shard6, shard7);
    EasyMock.replay(filter1, filter2, shard1, shard2, shard3, shard4, shard5, shard6, shard7);

    Set<ShardSpec> expected1 = ImmutableSet.of(shard1, shard4, shard5, shard6, shard7);
    assertFilterResult(filter1, shards, expected1);

    Set<ShardSpec> expected2 = ImmutableSet.of(shard3, shard6, shard7);
    assertFilterResult(filter2, shards, expected2);
  }

  private void assertFilterResult(DimFilter filter, Iterable<ShardSpec> input, Set<ShardSpec> expected)
  {
    Set<ShardSpec> result = DimFilterUtils.filterShards(filter, input, CONVERTER);
    Assert.assertEquals(expected, result);

    Map<String, Optional<RangeSet<String>>> dimensionRangeMap = Maps.newHashMap();
    result = Sets.newHashSet();
    for (ShardSpec shard : input) {
      result.addAll(DimFilterUtils.filterShards(filter, ImmutableList.of(shard), CONVERTER, dimensionRangeMap));
    }
    Assert.assertEquals(expected, result);
  }

  private static RangeSet<String> rangeSet(List<Range<String>> ranges)
  {
    ImmutableRangeSet.Builder<String> builder = ImmutableRangeSet.builder();
    for (Range<String> range : ranges) {
      builder.add(range);
    }
    return builder.build();
  }

  private static ShardSpec shardSpec(String dimension, Range<String> range)
  {
    ShardSpec shard = EasyMock.createMock(ShardSpec.class);
    EasyMock.expect(shard.getDomain())
            .andReturn(ImmutableMap.of(dimension, range))
            .anyTimes();
    return shard;
  }
}
