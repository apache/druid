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

package io.druid.query.groupby.epinephelinae;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedRow;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.List;

public class LimitedBufferGrouperTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testLimitAndBufferSwapping()
  {
    final int limit = 100;
    final int keyBase = 100000;
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferGrouper<Integer> grouper = makeGrouper(columnSelectorFactory, 20000, 2, limit);
    final int numRows = 1000;

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.<String, Object>of("value", 10L)));
    for (int i = 0; i < numRows; i++) {
      Assert.assertTrue(String.valueOf(i + keyBase), grouper.aggregate(i + keyBase).isOk());
    }

    // bucket size is hash(int) + key(int) + aggs(2 longs) + heap offset(int) = 28 bytes
    // limit is 100 so heap occupies 101 * 4 bytes = 404 bytes
    // buffer is 20000 bytes, so table arena size is 20000 - 404 = 19596 bytes
    // table arena is split in halves when doing push down, so each half is 9798 bytes
    // each table arena half can hold 9798 / 28 = 349 buckets, with load factor of 0.5 max buckets per half is 174
    // First buffer swap occurs when we hit 174 buckets
    // Subsequent buffer swaps occur after every 74 buckets, since we keep 100 buckets due to the limit
    // With 1000 keys inserted, this results in one swap at the first 174 buckets, then 11 swaps afterwards.
    // After the last swap, we have 100 keys + 12 new keys inserted.
    Assert.assertEquals(12, grouper.getGrowthCount());
    Assert.assertEquals(112, grouper.getSize());
    Assert.assertEquals(349, grouper.getBuckets());
    Assert.assertEquals(174, grouper.getMaxSize());
    Assert.assertEquals(100, grouper.getLimit());

    // Aggregate slightly different row
    // Since these keys are smaller, they will evict the previous 100 top entries
    // First 100 of these new rows will be the expected results.
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.<String, Object>of("value", 11L)));
    for (int i = 0; i < numRows; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
    }

    // we added another 1000 unique keys
    // previous size is 112, so next swap occurs after 62 rows
    // after that, there are 1000 - 62 = 938 rows, 938 / 74 = 12 additional swaps after the first,
    // with 50 keys being added after the final swap.
    Assert.assertEquals(25, grouper.getGrowthCount());
    Assert.assertEquals(150, grouper.getSize());
    Assert.assertEquals(349, grouper.getBuckets());
    Assert.assertEquals(174, grouper.getMaxSize());
    Assert.assertEquals(100, grouper.getLimit());

    final List<Grouper.Entry<Integer>> expected = Lists.newArrayList();
    for (int i = 0; i < limit; i++) {
      expected.add(new Grouper.Entry<>(i, new Object[]{11L, 1L}));
    }

    Assert.assertEquals(expected, Lists.newArrayList(grouper.iterator(true)));
  }

  @Test
  public void testBufferTooSmall()
  {
    expectedException.expect(IAE.class);
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferGrouper<Integer> grouper = makeGrouper(columnSelectorFactory, 10, 2, 100);
  }

  @Test
  public void testMinBufferSize()
  {
    final int limit = 100;
    final int keyBase = 100000;
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferGrouper<Integer> grouper = makeGrouper(columnSelectorFactory, 11716, 2, limit);
    final int numRows = 1000;

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.<String, Object>of("value", 10L)));
    for (int i = 0; i < numRows; i++) {
      Assert.assertTrue(String.valueOf(i + keyBase), grouper.aggregate(i + keyBase).isOk());
    }

    // With minimum buffer size, after the first swap, every new key added will result in a swap
    Assert.assertEquals(899, grouper.getGrowthCount());
    Assert.assertEquals(101, grouper.getSize());
    Assert.assertEquals(202, grouper.getBuckets());
    Assert.assertEquals(101, grouper.getMaxSize());
    Assert.assertEquals(100, grouper.getLimit());

    // Aggregate slightly different row
    // Since these keys are smaller, they will evict the previous 100 top entries
    // First 100 of these new rows will be the expected results.
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.<String, Object>of("value", 11L)));
    for (int i = 0; i < numRows; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
    }

    Assert.assertEquals(1899, grouper.getGrowthCount());
    Assert.assertEquals(101, grouper.getSize());
    Assert.assertEquals(202, grouper.getBuckets());
    Assert.assertEquals(101, grouper.getMaxSize());
    Assert.assertEquals(100, grouper.getLimit());

    final List<Grouper.Entry<Integer>> expected = Lists.newArrayList();
    for (int i = 0; i < limit; i++) {
      expected.add(new Grouper.Entry<>(i, new Object[]{11L, 1L}));
    }

    Assert.assertEquals(expected, Lists.newArrayList(grouper.iterator(true)));
  }

  private static LimitedBufferGrouper<Integer> makeGrouper(
      TestColumnSelectorFactory columnSelectorFactory,
      int bufferSize,
      int initialBuckets,
      int limit
  )
  {
    LimitedBufferGrouper<Integer> grouper = new LimitedBufferGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(bufferSize)),
        GrouperTestUtil.intKeySerde(),
        columnSelectorFactory,
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("valueSum", "value"),
            new CountAggregatorFactory("count")
        },
        Integer.MAX_VALUE,
        0.5f,
        initialBuckets,
        limit,
        false
    );

    grouper.init();
    return grouper;
  }
}
