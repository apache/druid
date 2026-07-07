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
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class BufferHashGrouperTest extends InitializedNullHandlingTest
{
  @Test
  public void testSimple()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final Grouper<IntKey> grouper = new BufferHashGrouper<>(
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
    grouper.aggregate(new IntKey(12));
    grouper.aggregate(new IntKey(6));
    grouper.aggregate(new IntKey(10));
    grouper.aggregate(new IntKey(6));
    grouper.aggregate(new IntKey(12));
    grouper.aggregate(new IntKey(12));

    final List<Grouper.Entry<IntKey>> expected = ImmutableList.of(
        new ReusableEntry<>(new IntKey(6), new Object[]{20L, 2L}),
        new ReusableEntry<>(new IntKey(10), new Object[]{10L, 1L}),
        new ReusableEntry<>(new IntKey(12), new Object[]{30L, 3L})
    );

    GrouperTestUtil.assertEntriesEquals(expected.iterator(), grouper.iterator(true));

    GrouperTestUtil.assertEntriesEquals(
        expected.iterator(),
        GrouperTestUtil.sortedEntries(
            grouper.iterator(false) /* unsorted entries */,
            k -> new IntKey(k.intValue()),
            Comparator.comparing(IntKey::intValue)
        ).iterator()
    );
  }

  @Test
  public void testGrowing()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    try (final ResourceHolder<Grouper<IntKey>> grouperHolder = makeGrouper(columnSelectorFactory, 10000, 2, 0.75f)) {
      final Grouper<IntKey> grouper = grouperHolder.get();
      final int expectedMaxSize = 210;

      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
      for (int i = 0; i < expectedMaxSize; i++) {
        Assertions.assertTrue(grouper.aggregate(new IntKey(i)).isOk(), String.valueOf(i));
      }
      Assertions.assertFalse(grouper.aggregate(new IntKey(expectedMaxSize)).isOk());

      // Aggregate slightly different row
      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 11L)));
      for (int i = 0; i < expectedMaxSize; i++) {
        Assertions.assertTrue(grouper.aggregate(new IntKey(i)).isOk(), String.valueOf(i));
      }
      Assertions.assertFalse(grouper.aggregate(new IntKey(expectedMaxSize)).isOk());

      final List<Grouper.Entry<IntKey>> expected = new ArrayList<>();
      for (int i = 0; i < expectedMaxSize; i++) {
        expected.add(new ReusableEntry<>(new IntKey(i), new Object[]{21L, 2L}));
      }

      GrouperTestUtil.assertEntriesEquals(expected.iterator(), grouper.iterator(true));
    }
  }

  @Test
  public void testNoGrowing()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    try (final ResourceHolder<Grouper<IntKey>> grouperHolder =
             makeGrouper(columnSelectorFactory, 10000, Integer.MAX_VALUE, 0.75f)) {
      final Grouper<IntKey> grouper = grouperHolder.get();
      final int expectedMaxSize = 258;

      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
      for (int i = 0; i < expectedMaxSize; i++) {
        Assertions.assertTrue(grouper.aggregate(new IntKey(i)).isOk(), String.valueOf(i));
      }
      Assertions.assertFalse(grouper.aggregate(new IntKey(expectedMaxSize)).isOk());

      // Aggregate slightly different row
      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 11L)));
      for (int i = 0; i < expectedMaxSize; i++) {
        Assertions.assertTrue(grouper.aggregate(new IntKey(i)).isOk(), String.valueOf(i));
      }
      Assertions.assertFalse(grouper.aggregate(new IntKey(expectedMaxSize)).isOk());

      final List<Grouper.Entry<IntKey>> expected = new ArrayList<>();
      for (int i = 0; i < expectedMaxSize; i++) {
        expected.add(new ReusableEntry<>(new IntKey(i), new Object[]{21L, 2L}));
      }

      GrouperTestUtil.assertEntriesEquals(expected.iterator(), grouper.iterator(true));
    }
  }

  @Test
  public void testMaxMergeBufferUsedBytes()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final BufferHashGrouper<IntKey> grouper = new BufferHashGrouper<>(
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

    long initialUsage = grouper.getMaxMergeBufferUsedBytes();
    Assertions.assertEquals(0L, initialUsage);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));

    grouper.aggregate(new IntKey(1));
    final long expectedBucketSize = grouper.getMaxMergeBufferUsedBytes();

    grouper.aggregate(new IntKey(2));
    grouper.aggregate(new IntKey(3));

    Assertions.assertEquals(3L * expectedBucketSize, grouper.getMaxMergeBufferUsedBytes());

    grouper.aggregate(new IntKey(4));
    grouper.aggregate(new IntKey(5));

    Assertions.assertEquals(5L * expectedBucketSize, grouper.getMaxMergeBufferUsedBytes());

    grouper.reset();
    Assertions.assertEquals(0, grouper.getSize());
    Assertions.assertEquals(5L * expectedBucketSize, grouper.getMaxMergeBufferUsedBytes());

    grouper.aggregate(new IntKey(1));
    grouper.aggregate(new IntKey(6));
    grouper.aggregate(new IntKey(7));
    grouper.aggregate(new IntKey(8));
    grouper.aggregate(new IntKey(9));
    grouper.aggregate(new IntKey(10));

    Assertions.assertEquals(6L * expectedBucketSize, grouper.getMaxMergeBufferUsedBytes());

    grouper.close();
  }

  @Test
  public void testMaxSpillProximityAtSpillTrigger()
  {
    // A tiny fixed-size table (maxSizeForTesting=1) forces a spill trigger on the 2nd distinct key: no bucket can be
    // allocated even after growth attempts, so findBucketWithAutoGrowth returns -1 and pins proximity to exactly 1.0.
    // The invariant we care about: 1.0 corresponds to the real spill point, and is independent of bucket width /
    // offset-list overhead / integer truncation. Contrast with the byte-based numerator, which topped out below 1.0.
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    final BufferHashGrouper<IntKey> grouper = new BufferHashGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(1000)),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new LongSumAggregatorFactory("valueSum", "value"),
                new CountAggregatorFactory("count")
            )
        ),
        /* bufferGrouperMaxSize */ 1,
        0,
        0,
        true
    );
    grouper.init();

    // Before any aggregation, proximity is 0.0 (empty table).
    Assertions.assertEquals(0.0, grouper.getMaxSpillProximity(), 0.0);

    // First key fits. Proximity is still strictly below 1.0 (size < regrowthThreshold after growth).
    Assertions.assertTrue(grouper.aggregate(new IntKey(1)).isOk());
    Assertions.assertTrue(
        grouper.getMaxSpillProximity() < 1.0,
        "proximity should stay below 1.0 while the table can still accept more keys: " + grouper.getMaxSpillProximity()
    );

    // Second key triggers a spill (findBucketWithAutoGrowth returns -1). Proximity is pinned to exactly 1.0.
    Assertions.assertFalse(grouper.aggregate(new IntKey(2)).isOk());
    Assertions.assertEquals(1.0, grouper.getMaxSpillProximity(), 0.0);

    // Reset preserves the peak: the grouper spilled at some point in its life.
    grouper.reset();
    Assertions.assertEquals(1.0, grouper.getMaxSpillProximity(), 0.0);

    grouper.close();
  }

  @Test
  public void testMaxSpillProximityBelowOneWhenNoSpill()
  {
    // A generously-sized table that never rejects a bucket. Proximity should be strictly below 1.0 for the entire
    // aggregation. This is the "operator reads <1.0, so no spill happened" invariant.
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    final BufferHashGrouper<IntKey> grouper = new BufferHashGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(10_000)),
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

    for (int i = 0; i < 20; i++) {
      Assertions.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
      Assertions.assertTrue(
          grouper.getMaxSpillProximity() < 1.0,
          "no spill occurred; proximity must remain strictly < 1.0: " + grouper.getMaxSpillProximity()
      );
    }

    grouper.close();
  }

  @Test
  public void testMaxSpillProximityAtTerminalThresholdWithoutRejection()
  {
    // Adversarial "at exactly the spill point, but no further insert attempted" case. Fill the grouper until one more
    // insert would trigger a rejection, then stop calling aggregate. Because the table is at its terminal growth level
    // (arena exhausted, no room to enlarge regrowthThreshold), size == regrowthThreshold means the next insert would
    // fail — that's the spill point. Proximity must be exactly 1.0, not (T-1)/T. The base-class updateMax records 1.0
    // at the terminal level via isTerminalTableLevel().
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    final BufferHashGrouper<IntKey> grouper = new BufferHashGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(10_000)),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new LongSumAggregatorFactory("valueSum", "value"),
                new CountAggregatorFactory("count")
            )
        ),
        Integer.MAX_VALUE,
        0.75f,
        4,
        true
    );
    grouper.init();

    // Fill until the first rejection.
    int inserted = 0;
    while (inserted < 10_000 && grouper.aggregate(new IntKey(inserted)).isOk()) {
      inserted++;
    }
    // A rejection occurred, so the grouper is definitively at its spill trigger.
    Assertions.assertEquals(1.0, grouper.getMaxSpillProximity(), 0.0);

    // The stricter Case #5 check: rebuild and STOP one insert before the rejection. size == regrowthThreshold, but
    // aggregate() was never called after that, so findBucketWithAutoGrowth was never invoked with a rejection. Still,
    // being at the terminal level means proximity must be 1.0.
    grouper.close();

    final BufferHashGrouper<IntKey> parked = new BufferHashGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(10_000)),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new LongSumAggregatorFactory("valueSum", "value"),
                new CountAggregatorFactory("count")
            )
        ),
        Integer.MAX_VALUE,
        0.75f,
        4,
        true
    );
    parked.init();
    for (int i = 0; i < inserted; i++) {
      Assertions.assertTrue(parked.aggregate(new IntKey(i)).isOk());
    }
    // Confirm the arithmetic: `inserted` == regrowthThreshold_final, so after `inserted` successful inserts the parked
    // grouper's size sits exactly at getMaxSize() (i.e. regrowthThreshold_final). The next new-key aggregate() would
    // find no bucket and return not-ok — this IS the spill point.
    Assertions.assertEquals(inserted, parked.getSize());
    Assertions.assertEquals(inserted, parked.getMaxSize());
    // No further aggregate call. Grouper is parked exactly at its terminal threshold; the next insert WOULD spill.
    Assertions.assertEquals(1.0, parked.getMaxSpillProximity(), 0.0);
    parked.close();
  }

  @Test
  public void testMaxSpillProximityBeforeInitIsZero()
  {
    // Grouper never initialized: no hash table has been created, so proximity is 0.0 rather than NaN or a crash.
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final BufferHashGrouper<IntKey> grouper = new BufferHashGrouper<>(
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
    Assertions.assertEquals(0.0, grouper.getMaxSpillProximity(), 0.0);
  }

  private ResourceHolder<Grouper<IntKey>> makeGrouper(
      GroupByTestColumnSelectorFactory columnSelectorFactory,
      int bufferSize,
      int initialBuckets,
      float maxLoadFactor
  )
  {
    // Use off-heap allocation since one of our tests has a 1.9GB buffer. Heap size may be insufficient.
    final ResourceHolder<ByteBuffer> bufferHolder = ByteBufferUtils.allocateDirect(bufferSize);

    final BufferHashGrouper<IntKey> grouper = new BufferHashGrouper<>(
        bufferHolder::get,
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

    return new ResourceHolder<>()
    {
      @Override
      public BufferHashGrouper<IntKey> get()
      {
        return grouper;
      }

      @Override
      public void close()
      {
        grouper.close();
        bufferHolder.close();
      }
    };
  }
}
