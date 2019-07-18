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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.CloserRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class BufferHashGrouperTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public CloserRule closerRule = new CloserRule(true);

  @Test
  public void testSimple()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final Grouper<Integer> grouper = new BufferHashGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(1000)),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new LongSumAggregatorFactory("valueSum", "value"),
                new CountAggregatorFactory("count")
            )
        ),
        Integer.MAX_VALUE,
        0,
        0,
        true
    );
    grouper.init();

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    grouper.aggregate(12);
    grouper.aggregate(6);
    grouper.aggregate(10);
    grouper.aggregate(6);
    grouper.aggregate(12);
    grouper.aggregate(12);

    final List<Grouper.Entry<Integer>> expected = ImmutableList.of(
        new Grouper.Entry<>(6, new Object[]{20L, 2L}),
        new Grouper.Entry<>(10, new Object[]{10L, 1L}),
        new Grouper.Entry<>(12, new Object[]{30L, 3L})
    );
    final List<Grouper.Entry<Integer>> unsortedEntries = Lists.newArrayList(grouper.iterator(false));
    final List<Grouper.Entry<Integer>> sortedEntries = Lists.newArrayList(grouper.iterator(true));

    Assert.assertEquals(expected, sortedEntries);
    Assert.assertEquals(
        expected,
        Ordering.from(
            new Comparator<Grouper.Entry<Integer>>()
            {
              @Override
              public int compare(Grouper.Entry<Integer> o1, Grouper.Entry<Integer> o2)
              {
                return Ints.compare(o1.getKey(), o2.getKey());
              }
            }
        ).sortedCopy(unsortedEntries)
    );
  }

  @Test
  public void testGrowing()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final Grouper<Integer> grouper = makeGrouper(columnSelectorFactory, 10000, 2, 0.75f);
    final int expectedMaxSize = NullHandling.replaceWithDefault() ? 219 : 210;

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    for (int i = 0; i < expectedMaxSize; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
    }
    Assert.assertFalse(grouper.aggregate(expectedMaxSize).isOk());

    // Aggregate slightly different row
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 11L)));
    for (int i = 0; i < expectedMaxSize; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
    }
    Assert.assertFalse(grouper.aggregate(expectedMaxSize).isOk());

    final List<Grouper.Entry<Integer>> expected = new ArrayList<>();
    for (int i = 0; i < expectedMaxSize; i++) {
      expected.add(new Grouper.Entry<>(i, new Object[]{21L, 2L}));
    }

    Assert.assertEquals(expected, Lists.newArrayList(grouper.iterator(true)));
  }

  @Test
  public void testGrowingOverflowingInteger()
  {
    // This test checks the bug reported in https://github.com/apache/incubator-druid/pull/4333 only when
    // NullHandling.replaceWithDefault() is true
    if (NullHandling.replaceWithDefault()) {
      final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
      // the buffer size below is chosen to test integer overflow in ByteBufferHashTable.adjustTableWhenFull().
      final Grouper<Integer> grouper = makeGrouper(columnSelectorFactory, 1_900_000_000, 2, 0.3f);
      final int expectedMaxSize = 15323979;

      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
      for (int i = 0; i < expectedMaxSize; i++) {
        Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
      }
      Assert.assertFalse(grouper.aggregate(expectedMaxSize).isOk());
    }
  }

  @Test
  public void testNoGrowing()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final Grouper<Integer> grouper = makeGrouper(columnSelectorFactory, 10000, Integer.MAX_VALUE, 0.75f);
    final int expectedMaxSize = NullHandling.replaceWithDefault() ? 267 : 258;

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    for (int i = 0; i < expectedMaxSize; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
    }
    Assert.assertFalse(grouper.aggregate(expectedMaxSize).isOk());

    // Aggregate slightly different row
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 11L)));
    for (int i = 0; i < expectedMaxSize; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
    }
    Assert.assertFalse(grouper.aggregate(expectedMaxSize).isOk());

    final List<Grouper.Entry<Integer>> expected = new ArrayList<>();
    for (int i = 0; i < expectedMaxSize; i++) {
      expected.add(new Grouper.Entry<>(i, new Object[]{21L, 2L}));
    }

    Assert.assertEquals(expected, Lists.newArrayList(grouper.iterator(true)));
  }

  private BufferHashGrouper<Integer> makeGrouper(
      TestColumnSelectorFactory columnSelectorFactory,
      int bufferSize,
      int initialBuckets,
      float maxLoadFactor
  )
  {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);

    final BufferHashGrouper<Integer> grouper = new BufferHashGrouper<>(
        Suppliers.ofInstance(buffer),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new LongSumAggregatorFactory("valueSum", "value"),
                new CountAggregatorFactory("count")
            )
        ),
        Integer.MAX_VALUE,
        maxLoadFactor,
        initialBuckets,
        true
    );
    grouper.init();
    return grouper;
  }
}
