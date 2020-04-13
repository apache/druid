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

package org.apache.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.timeline.partition.ShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    ShardSpec shard1 = shardSpec("dim1", true);
    ShardSpec shard2 = shardSpec("dim1", false);
    ShardSpec shard3 = shardSpec("dim1", false);
    ShardSpec shard4 = shardSpec("dim2", false);
    ShardSpec shard5 = shardSpec("dim2", false);
    ShardSpec shard6 = shardSpec("dim2", false);
    ShardSpec shard7 = shardSpec("dim2", false);

    List<ShardSpec> shards = ImmutableList.of(shard1, shard2, shard3, shard4, shard5, shard6, shard7);
    EasyMock.replay(filter1, shard1, shard2, shard3, shard4, shard5, shard6, shard7);

    Set<ShardSpec> expected1 = ImmutableSet.of(shard1, shard4, shard5, shard6, shard7);
    assertFilterResult(filter1, shards, expected1);
  }

  private void assertFilterResult(DimFilter filter, Iterable<ShardSpec> input, Set<ShardSpec> expected)
  {
    Set<ShardSpec> result = DimFilterUtils.filterShards(filter, input, CONVERTER);
    Assert.assertEquals(expected, result);

    Map<String, Optional<RangeSet<String>>> dimensionRangeMap = new HashMap<>();
    result = new HashSet<>();
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

  private static ShardSpec shardSpec(String dimension, boolean contained)
  {
    ShardSpec shard = EasyMock.createMock(ShardSpec.class);
    EasyMock.expect(shard.getDomainDimensions())
            .andReturn(ImmutableList.of(dimension))
            .anyTimes();
    EasyMock.expect(shard.possibleInDomain(EasyMock.anyObject()))
            .andReturn(contained)
            .anyTimes();
    return shard;
  }
}
