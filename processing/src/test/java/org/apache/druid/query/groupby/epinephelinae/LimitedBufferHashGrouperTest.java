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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class LimitedBufferHashGrouperTest extends InitializedNullHandlingTest
{
  static final int LIMIT = 100;
  static final int KEY_BASE = 100000;
  static final int NUM_ROWS = 1000;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testLimitAndBufferSwapping()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferHashGrouper<IntKey> grouper = makeGrouper(columnSelectorFactory, 20000);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertTrue(String.valueOf(i + KEY_BASE), grouper.aggregate(new IntKey(i + KEY_BASE)).isOk());
    }
    if (NullHandling.replaceWithDefault()) {
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
    } else {
      // With Nullability enabled
      // bucket size is hash(int) + key(int) + aggs(2 longs + 1 bytes for Long Agg nullability) + heap offset(int) = 29 bytes
      // limit is 100 so heap occupies 101 * 4 bytes = 404 bytes
      // buffer is 20000 bytes, so table arena size is 20000 - 404 = 19596 bytes
      // table arena is split in halves when doing push down, so each half is 9798 bytes
      // each table arena half can hold 9798 / 29 = 337 buckets, with load factor of 0.5 max buckets per half is 168
      // First buffer swap occurs when we hit 168 buckets
      // Subsequent buffer swaps occur after every 68 buckets, since we keep 100 buckets due to the limit
      // With 1000 keys inserted, this results in one swap at the first 169 buckets, then 12 swaps afterwards.
      // After the last swap, we have 100 keys + 16 new keys inserted.
      Assert.assertEquals(13, grouper.getGrowthCount());
      Assert.assertEquals(116, grouper.getSize());
      Assert.assertEquals(337, grouper.getBuckets());
      Assert.assertEquals(168, grouper.getMaxSize());
    }

    Assert.assertEquals(100, grouper.getLimit());

    // Aggregate slightly different row
    // Since these keys are smaller, they will evict the previous 100 top entries
    // First 100 of these new rows will be the expected results.
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 11L)));
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(new IntKey(i)).isOk());
    }

    if (NullHandling.replaceWithDefault()) {
      // we added another 1000 unique keys
      // previous size is 112, so next swap occurs after 62 rows
      // after that, there are 1000 - 62 = 938 rows, 938 / 74 = 12 additional swaps after the first,
      // with 50 keys being added after the final swap.
      Assert.assertEquals(25, grouper.getGrowthCount());
      Assert.assertEquals(150, grouper.getSize());
      Assert.assertEquals(349, grouper.getBuckets());
      Assert.assertEquals(174, grouper.getMaxSize());
    } else {
      // With Nullable Aggregator
      // we added another 1000 unique keys
      // previous size is 116, so next swap occurs after 52 rows
      // after that, there are 1000 - 52 = 948 rows, 948 / 68 = 13 additional swaps after the first,
      // with 64 keys being added after the final swap.
      Assert.assertEquals(27, grouper.getGrowthCount());
      Assert.assertEquals(164, grouper.getSize());
      Assert.assertEquals(337, grouper.getBuckets());
      Assert.assertEquals(168, grouper.getMaxSize());
    }

    Assert.assertEquals(100, grouper.getLimit());

    final List<Pair<Integer, List<Object>>> expected = new ArrayList<>();
    for (int i = 0; i < LIMIT; i++) {
      expected.add(Pair.of(i, ImmutableList.of(11L, 1L)));
    }

    Assert.assertEquals(expected, entriesToList(grouper.iterator(true)));
    // iterate again, even though the min-max offset heap has been destroyed, it is replaced with a reverse sorted array
    Assert.assertEquals(expected, entriesToList(grouper.iterator(true)));
  }

  @Test
  public void testBufferTooSmall()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("LimitedBufferHashGrouper initialized with insufficient buffer capacity");
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    makeGrouper(columnSelectorFactory, 10);
  }

  @Test
  public void testMinBufferSize()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferHashGrouper<IntKey> grouper = makeGrouper(columnSelectorFactory, 12120);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertTrue(String.valueOf(i + KEY_BASE), grouper.aggregate(new IntKey(i + KEY_BASE)).isOk());
    }

    // With minimum buffer size, after the first swap, every new key added will result in a swap
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(224, grouper.getGrowthCount());
      Assert.assertEquals(104, grouper.getSize());
      Assert.assertEquals(209, grouper.getBuckets());
      Assert.assertEquals(104, grouper.getMaxSize());
    } else {
      Assert.assertEquals(899, grouper.getGrowthCount());
      Assert.assertEquals(101, grouper.getSize());
      Assert.assertEquals(202, grouper.getBuckets());
      Assert.assertEquals(101, grouper.getMaxSize());
    }
    Assert.assertEquals(100, grouper.getLimit());

    // Aggregate slightly different row
    // Since these keys are smaller, they will evict the previous 100 top entries
    // First 100 of these new rows will be the expected results.
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 11L)));
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(new IntKey(i)).isOk());
    }
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(474, grouper.getGrowthCount());
      Assert.assertEquals(104, grouper.getSize());
      Assert.assertEquals(209, grouper.getBuckets());
      Assert.assertEquals(104, grouper.getMaxSize());
    } else {
      Assert.assertEquals(1899, grouper.getGrowthCount());
      Assert.assertEquals(101, grouper.getSize());
      Assert.assertEquals(202, grouper.getBuckets());
      Assert.assertEquals(101, grouper.getMaxSize());
    }
    Assert.assertEquals(100, grouper.getLimit());

    final List<Pair<Integer, List<Object>>> expected = new ArrayList<>();
    for (int i = 0; i < LIMIT; i++) {
      expected.add(Pair.of(i, ImmutableList.of(11L, 1L)));
    }

    Assert.assertEquals(expected, entriesToList(grouper.iterator(true)));
    // iterate again, even though the min-max offset heap has been destroyed, it is replaced with a reverse sorted array
    Assert.assertEquals(expected, entriesToList(grouper.iterator(true)));
  }

  @Test
  public void testAggregateAfterIterated()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("attempted to add offset after grouper was iterated");

    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferHashGrouper<IntKey> grouper = makeGrouper(columnSelectorFactory, 12120);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertTrue(String.valueOf(i + KEY_BASE), grouper.aggregate(new IntKey(i + KEY_BASE)).isOk());
    }
    List<Grouper.Entry<IntKey>> iterated = Lists.newArrayList(grouper.iterator(true));
    Assert.assertEquals(LIMIT, iterated.size());

    // an attempt to aggregate with a new key will explode after the grouper has been iterated
    grouper.aggregate(new IntKey(KEY_BASE + NUM_ROWS + 1));
  }

  @Test
  public void testIteratorOrderByDim()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferHashGrouper<IntKey> grouper = makeGrouperWithOrderBy(
        columnSelectorFactory,
        "value",
        OrderByColumnSpec.Direction.ASCENDING
    );

    for (int i = 0; i < NUM_ROWS; i++) {
      // limited grouper iterator will always sort by keys in ascending order, even if the heap was sorted by values
      // so, we aggregate with keys and values both descending so that the results are not re-ordered by key
      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", NUM_ROWS - i + KEY_BASE)));
      Assert.assertTrue(
          String.valueOf(NUM_ROWS - i + KEY_BASE),
          grouper.aggregate(new IntKey(NUM_ROWS - i + KEY_BASE)).isOk()
      );
    }

    final CloseableIterator<Grouper.Entry<IntKey>> iterator = grouper.iterator(true);

    int i = 0;
    while (iterator.hasNext()) {
      final Grouper.Entry<IntKey> entry = iterator.next();
      Assert.assertEquals(KEY_BASE + i + 1L, entry.getValues()[0]);
      i++;
    }

    Assert.assertEquals(LIMIT, i);
  }

  @Test
  public void testIteratorOrderByDimDesc()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferHashGrouper<IntKey> grouper = makeGrouperWithOrderBy(
        columnSelectorFactory,
        "value",
        OrderByColumnSpec.Direction.DESCENDING
    );

    for (int i = 0; i < NUM_ROWS; i++) {
      // limited grouper iterator will always sort by keys in ascending order, even if the heap was sorted by values
      // so, we aggregate with keys and values both ascending so that the results are not re-ordered by key
      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", i + 1)));
      Assert.assertTrue(String.valueOf(i + KEY_BASE), grouper.aggregate(new IntKey(i + KEY_BASE)).isOk());
    }

    final CloseableIterator<Grouper.Entry<IntKey>> iterator = grouper.iterator(true);

    int i = 0;
    while (iterator.hasNext()) {
      final Grouper.Entry<IntKey> entry = iterator.next();
      Assert.assertEquals((long) NUM_ROWS - i, entry.getValues()[0]);
      i++;
    }
  }

  @Test
  public void testIteratorOrderByAggs()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferHashGrouper<IntKey> grouper = makeGrouperWithOrderBy(
        columnSelectorFactory,
        "valueSum",
        OrderByColumnSpec.Direction.ASCENDING
    );

    for (int i = 0; i < NUM_ROWS; i++) {
      // limited grouper iterator will always sort by keys in ascending order, even if the heap was sorted by values
      // so, we aggregate with keys and values both descending so that the results are not re-ordered by key
      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", NUM_ROWS - i)));
      Assert.assertTrue(
          String.valueOf(NUM_ROWS - i + KEY_BASE),
          grouper.aggregate(new IntKey(NUM_ROWS - i + KEY_BASE)).isOk()
      );
    }

    final CloseableIterator<Grouper.Entry<IntKey>> iterator = grouper.iterator(true);

    int i = 0;
    while (iterator.hasNext()) {
      final Grouper.Entry<IntKey> entry = iterator.next();
      Assert.assertEquals(i + 1L, entry.getValues()[0]);
      i++;
    }

    Assert.assertEquals(LIMIT, i);
  }

  @Test
  public void testIteratorOrderByAggsDesc()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final LimitedBufferHashGrouper<IntKey> grouper = makeGrouperWithOrderBy(
        columnSelectorFactory,
        "valueSum",
        OrderByColumnSpec.Direction.DESCENDING
    );

    for (int i = 0; i < NUM_ROWS; i++) {
      // limited grouper iterator will always sort by keys in ascending order, even if the heap was sorted by values
      // so, we aggregate with keys descending and values asending so that the results are not re-ordered by key
      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", i + 1)));
      Assert.assertTrue(
          String.valueOf(NUM_ROWS - i + KEY_BASE),
          grouper.aggregate(new IntKey(NUM_ROWS - i + KEY_BASE)).isOk()
      );
    }

    final CloseableIterator<Grouper.Entry<IntKey>> iterator = grouper.iterator(true);

    int i = 0;
    while (iterator.hasNext()) {
      final Grouper.Entry<IntKey> entry = iterator.next();
      Assert.assertEquals((long) NUM_ROWS - i, entry.getValues()[0]);
      i++;
    }

    Assert.assertEquals(LIMIT, i);
  }

  private static LimitedBufferHashGrouper<IntKey> makeGrouper(
      GroupByTestColumnSelectorFactory columnSelectorFactory,
      int bufferSize
  )
  {
    LimitedBufferHashGrouper<IntKey> grouper = new LimitedBufferHashGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(bufferSize)),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new LongSumAggregatorFactory("valueSum", "value"),
                new CountAggregatorFactory("count")
            )
        ),
        Integer.MAX_VALUE,
        0.5f,
        2,
        LIMIT,
        false
    );

    grouper.init();
    return grouper;
  }

  private static LimitedBufferHashGrouper<IntKey> makeGrouperWithOrderBy(
      GroupByTestColumnSelectorFactory columnSelectorFactory,
      String orderByColumn,
      OrderByColumnSpec.Direction direction
  )
  {
    final StringComparator stringComparator = "value".equals(orderByColumn)
                                              ? StringComparators.LEXICOGRAPHIC
                                              : StringComparators.NUMERIC;
    final DefaultLimitSpec orderBy = DefaultLimitSpec.builder()
                                                     .orderBy(
                                                         new OrderByColumnSpec(
                                                             orderByColumn,
                                                             direction,
                                                             stringComparator
                                                         )
                                                     )
                                                     .limit(LIMIT)
                                                     .build();

    LimitedBufferHashGrouper<IntKey> grouper = new LimitedBufferHashGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(12120)),
        new GroupByIshKeySerde(orderBy),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new LongSumAggregatorFactory("valueSum", "value"),
                new CountAggregatorFactory("count")
            )
        ),
        Integer.MAX_VALUE,
        0.5f,
        2,
        LIMIT,
        !orderBy.getColumns().get(0).getDimension().equals("value")
    );

    grouper.init();
    return grouper;
  }

  private static List<Pair<Integer, List<Object>>> entriesToList(final Iterator<Grouper.Entry<IntKey>> entryIterator)
  {
    final List<Pair<Integer, List<Object>>> retVal = new ArrayList<>();

    while (entryIterator.hasNext()) {
      final Grouper.Entry<IntKey> entry = entryIterator.next();
      retVal.add(
          Pair.of(
              entry.getKey().intValue(),
              ImmutableList.copyOf(Arrays.asList(entry.getValues()))
          )
      );
    }

    return retVal;
  }

  /**
   * key serde for more realistic ordering tests, similar to the {@link GroupByQueryEngine.GroupByEngineKeySerde} or
   * {@link RowBasedGrouperHelper.RowBasedKeySerde} which are likely to be used in practice by the group-by engine,
   * which also both use {@link GrouperBufferComparatorUtils} to make comparators
   */
  private static class GroupByIshKeySerde extends IntKeySerde
  {
    private final DefaultLimitSpec orderBy;

    public GroupByIshKeySerde(DefaultLimitSpec orderBy)
    {
      this.orderBy = orderBy;
    }

    @Override
    public Grouper.BufferComparator bufferComparator()
    {
      return GrouperBufferComparatorUtils.bufferComparator(
          false,
          false,
          1,
          new Grouper.BufferComparator[]{KEY_COMPARATOR}
      );
    }

    @Override
    public Grouper.BufferComparator bufferComparatorWithAggregators(
        AggregatorFactory[] aggregatorFactories,
        int[] aggregatorOffsets
    )
    {
      return GrouperBufferComparatorUtils.bufferComparatorWithAggregators(
          aggregatorFactories,
          aggregatorOffsets,
          orderBy,
          ImmutableList.of(DefaultDimensionSpec.of("value")),
          new Grouper.BufferComparator[]{KEY_COMPARATOR},
          false,
          false,
          Integer.BYTES
      );
    }
  }
}
