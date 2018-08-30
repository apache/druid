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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.CloserRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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
        columnSelectorFactory,
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("valueSum", "value"),
            new CountAggregatorFactory("count")
        },
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
    final Grouper<Integer> grouper = makeGrouper(columnSelectorFactory, 10000, 2);
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

    final List<Grouper.Entry<Integer>> expected = Lists.newArrayList();
    for (int i = 0; i < expectedMaxSize; i++) {
      expected.add(new Grouper.Entry<>(i, new Object[]{21L, 2L}));
    }

    Assert.assertEquals(expected, Lists.newArrayList(grouper.iterator(true)));
  }

  @Test
  public void testGrowing2()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final Grouper<Integer> grouper = makeGrouper(columnSelectorFactory, 2_000_000_000, 2);
    final int expectedMaxSize = NullHandling.replaceWithDefault() ? 40988516 : 39141224;

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    for (int i = 0; i < expectedMaxSize; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
    }
    Assert.assertFalse(grouper.aggregate(expectedMaxSize).isOk());
  }

  @Test
  public void testGrowing3()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final Grouper<Integer> grouper = makeGrouper(columnSelectorFactory, Integer.MAX_VALUE, 2);
    final int expectedMaxSize = NullHandling.replaceWithDefault() ? 44938972 : 42955456;

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    for (int i = 0; i < expectedMaxSize; i++) {
      Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
    }
    Assert.assertFalse(grouper.aggregate(expectedMaxSize).isOk());
  }

  @Test
  public void testNoGrowing()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final Grouper<Integer> grouper = makeGrouper(columnSelectorFactory, 10000, Integer.MAX_VALUE);
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

    final List<Grouper.Entry<Integer>> expected = Lists.newArrayList();
    for (int i = 0; i < expectedMaxSize; i++) {
      expected.add(new Grouper.Entry<>(i, new Object[]{21L, 2L}));
    }

    Assert.assertEquals(expected, Lists.newArrayList(grouper.iterator(true)));
  }

  private BufferHashGrouper<Integer> makeGrouper(
      TestColumnSelectorFactory columnSelectorFactory,
      int bufferSize,
      int initialBuckets
  )
  {
    final MappedByteBuffer buffer;

    try {
      final File file = temporaryFolder.newFile();
      try (final FileChannel channel = new FileOutputStream(file).getChannel()) {
        channel.truncate(bufferSize);
      }
      buffer = Files.map(file, FileChannel.MapMode.READ_WRITE, bufferSize);
      closerRule.closeLater(() -> ByteBufferUtils.unmap(buffer));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    final BufferHashGrouper<Integer> grouper = new BufferHashGrouper<>(
        Suppliers.ofInstance(buffer),
        GrouperTestUtil.intKeySerde(),
        columnSelectorFactory,
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("valueSum", "value"),
            new CountAggregatorFactory("count")
        },
        Integer.MAX_VALUE,
        0.75f,
        initialBuckets,
        true
    );
    grouper.init();
    return grouper;
  }
}
