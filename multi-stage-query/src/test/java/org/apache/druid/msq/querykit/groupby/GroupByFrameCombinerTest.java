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

package org.apache.druid.msq.querykit.groupby;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.processor.FrameChannelMerger;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests for {@link GroupByFrameCombiner} used with {@link FrameChannelMerger}.
 *
 * The existing combiner tests in SuperSorterTest use {@link org.apache.druid.frame.processor.SummingFrameCombiner},
 * which creates a new ColumnSelectorFactory each call to getCombinedColumnSelectorFactory(). That avoids the bug
 * where dimension selectors become stale when the underlying frame cursor changes. This test uses the real
 * {@link GroupByFrameCombiner} to verify correct behavior across frame boundaries.
 */
public class GroupByFrameCombinerTest extends InitializedNullHandlingTest
{
  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("key_str", ColumnType.STRING)
                                                            .add("key_long", ColumnType.LONG)
                                                            .add("value", ColumnType.LONG)
                                                            .build();

  private static final List<KeyColumn> SORT_KEY =
      ImmutableList.of(
          new KeyColumn("key_str", KeyOrder.ASCENDING),
          new KeyColumn("key_long", KeyOrder.ASCENDING)
      );

  private static final RowSignature SORTABLE_SIGNATURE =
      FrameWriters.sortableSignature(SIGNATURE, SORT_KEY);

  /**
   * Two channels, five distinct keys, one row per frame. The combiner must switch frames between
   * every group, exercising the cursor-tracking logic for dimension selectors.
   */
  @Test
  public void testTwoChannelsOneRowPerFrame() throws Exception
  {
    final List<List<Object>> rows = runMergerWithCombiner(
        ImmutableList.of(
            ImmutableList.of(row("a", 100L, 1L), row("b", 200L, 2L), row("c", 300L, 3L), row("d", 400L, 4L), row("e", 500L, 5L)),
            ImmutableList.of(row("a", 100L, 10L), row("b", 200L, 20L), row("c", 300L, 30L), row("d", 400L, 40L), row("e", 500L, 50L))
        ),
        1 // one row per frame
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of("a", 100L, 11L),
            ImmutableList.of("b", 200L, 22L),
            ImmutableList.of("c", 300L, 33L),
            ImmutableList.of("d", 400L, 44L),
            ImmutableList.of("e", 500L, 55L)
        ),
        rows
    );
  }

  /**
   * Four channels, twenty distinct keys, one row per frame.
   */
  @Test
  public void testFourChannelsOneRowPerFrame() throws Exception
  {
    final int numChannels = 4;
    final int numKeys = 20;

    final List<List<Row>> channelData =
        IntStream.range(0, numChannels)
                 .mapToObj(ch -> IntStream.range(0, numKeys)
                                          .mapToObj(k -> row(StringUtils.format("key_%02d", k), k, 1L))
                                          .collect(Collectors.toList()))
                 .collect(Collectors.toList());

    final List<List<Object>> rows = runMergerWithCombiner(channelData, 1);

    Assert.assertEquals(numKeys, rows.size());
    for (int k = 0; k < numKeys; k++) {
      Assert.assertEquals(StringUtils.format("key_%02d", k), rows.get(k).get(0));
      Assert.assertEquals((long) k, rows.get(k).get(1));
      Assert.assertEquals((long) numChannels, rows.get(k).get(2));
    }
  }

  /**
   * Two channels, three distinct keys, multiple rows per frame. Some combining happens within a single frame
   * (same key appears in consecutive rows of the same frame) and some across frame boundaries.
   */
  @Test
  public void testTwoChannelsMultipleRowsPerFrame() throws Exception
  {
    final List<List<Object>> rows = runMergerWithCombiner(
        ImmutableList.of(
            ImmutableList.of(
                row("a", 1L, 10L),
                row("a", 1L, 20L),
                row("b", 2L, 30L),
                row("c", 3L, 40L),
                row("c", 3L, 50L)
            ),
            ImmutableList.of(
                row("a", 1L, 100L),
                row("b", 2L, 200L),
                row("b", 2L, 300L),
                row("c", 3L, 400L)
            )
        ),
        3 // multiple rows per frame
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of("a", 1L, 130L),
            ImmutableList.of("b", 2L, 530L),
            ImmutableList.of("c", 3L, 490L)
        ),
        rows
    );
  }

  private List<List<Object>> runMergerWithCombiner(
      final List<List<Row>> channelData,
      final int maxRowsPerFrame
  ) throws Exception
  {
    final FrameReader frameReader = FrameReader.create(SORTABLE_SIGNATURE);
    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final List<ReadableFrameChannel> channels = new ArrayList<>();
    for (final List<Row> data : channelData) {
      channels.add(makeFrameChannel(data, maxRowsPerFrame));
    }

    final FrameChannelMerger merger = new FrameChannelMerger(
        channels,
        frameReader,
        outputChannel.writable(),
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
            new ArenaMemoryAllocatorFactory(1_000_000),
            SORTABLE_SIGNATURE,
            Collections.emptyList(),
            false
        ),
        SORT_KEY,
        new GroupByFrameCombiner(
            SORTABLE_SIGNATURE,
            ImmutableList.of(new LongSumAggregatorFactory("value", "value")),
            2 // aggregatorStart: columns 0-1 are keys, column 2 is the aggregate
        ),
        null,
        -1
    );

    new FrameProcessorExecutor(MoreExecutors.newDirectExecutorService())
        .runFully(merger, null);

    final List<List<Object>> rows = new ArrayList<>();
    FrameTestUtil.readRowsFromFrameChannel(outputChannel.readable(), frameReader)
                 .forEach(rows::add);
    return rows;
  }

  private static Row row(final String key, final long keyLong, final long value)
  {
    return new MapBasedRow(0L, Map.of("key_str", key, "key_long", keyLong, "value", value));
  }

  private static ReadableFrameChannel makeFrameChannel(
      final List<Row> rows,
      final int maxRowsPerFrame
  ) throws IOException
  {
    final Sequence<Frame> frames = FrameSequenceBuilder
        .fromCursorFactory(new RowBasedCursorFactory<>(Sequences.simple(rows), RowAdapters.standardRow(), SIGNATURE))
        .maxRowsPerFrame(maxRowsPerFrame)
        .sortBy(SORT_KEY)
        .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(1_000_000)))
        .frameType(FrameType.latestRowBased())
        .frames();

    final BlockingQueueFrameChannel channel = new BlockingQueueFrameChannel(100);
    frames.forEach(frame -> {
      try {
        channel.writable().write(frame);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    channel.writable().close();
    return channel.readable();
  }
}
