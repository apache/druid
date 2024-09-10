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

package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.indexing.CountingWritableFrameChannel;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TooManyRowsInAWindowFault;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.test.LimitedFrameWriterFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.query.operator.window.ranking.WindowRowNumberProcessor;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WindowOperatorQueryFrameProcessorTest extends FrameProcessorTestBase
{
  @Test
  public void testBatchingOfPartitionByKeys_singleBatch() throws Exception
  {
    // With maxRowsMaterialized=100, we will get 1 frame:
    // [1, 1, 2, 2, 2, 3, 3]
    validateBatching(100, 1);
  }

  @Test
  public void testBatchingOfPartitionByKeys_multipleBatches_1() throws Exception
  {
    // With maxRowsMaterialized=5, we will get 2 frames:
    // [1, 1, 2, 2, 2]
    // [3, 3]
    validateBatching(5, 2);
  }

  @Test
  public void testBatchingOfPartitionByKeys_multipleBatches_2() throws Exception
  {
    // With maxRowsMaterialized=4, we will get 3 frames:
    // [1, 1]
    // [2, 2, 2]
    // [3, 3]
    validateBatching(4, 3);
  }

  @Test
  public void testBatchingOfPartitionByKeys_TooManyRowsInAWindowFault()
  {
    final RuntimeException e = Assert.assertThrows(
        RuntimeException.class,
        () -> validateBatching(2, 3)
    );
    MatcherAssert.assertThat(
        ((MSQException) e.getCause().getCause()).getFault(),
        CoreMatchers.instanceOf(TooManyRowsInAWindowFault.class)
    );
    Assert.assertTrue(e.getMessage().contains("TooManyRowsInAWindow: Too many rows in a window (requested = 3, max = 2)"));
  }

  public void validateBatching(int maxRowsMaterialized, int numFramesWritten) throws Exception
  {
    final ReadableInput factChannel = buildWindowTestInputChannel();

    RowSignature inputSignature = RowSignature.builder()
                                              .add("cityName", ColumnType.STRING)
                                              .add("added", ColumnType.LONG)
                                              .build();

    FrameReader frameReader = FrameReader.create(inputSignature);

    RowSignature outputSignature = RowSignature.builder()
                                               .addAll(inputSignature)
                                               .add("w0", ColumnType.LONG)
                                               .build();

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();
    ChannelCounters channelCounters = new ChannelCounters();
    final CountingWritableFrameChannel countingWritableFrameChannel = new CountingWritableFrameChannel(
        outputChannel.writable(),
        channelCounters,
        0
    );

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            Druids.newScanQueryBuilder()
                     .dataSource(new TableDataSource("test"))
                     .intervals(new LegacySegmentSpec(Intervals.ETERNITY))
                     .columns("cityName", "added")
                     .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                     .context(new HashMap<>())
                     .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        new HashMap<>(),
        outputSignature,
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        ),
        ImmutableList.of()
    );

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeRowBasedFrameWriterFactory(
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            outputSignature,
            Collections.emptyList(),
            false
        ),
        100
    );

    final WindowOperatorQueryFrameProcessor processor = new WindowOperatorQueryFrameProcessor(
        query,
        factChannel.getChannel(),
        countingWritableFrameChannel,
        frameWriterFactory,
        frameReader,
        new ObjectMapper(),
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        ),
        inputSignature,
        maxRowsMaterialized,
        ImmutableList.of("added")
    );

    exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(outputSignature)
    );

    final List<List<Object>> rows = rowsFromProcessor.toList();

    long actualNumFrames = Arrays.stream(channelCounters.snapshot().getFrames()).findFirst().getAsLong();
    Assert.assertEquals(numFramesWritten, actualNumFrames);
    Assert.assertEquals(7, rows.size());
  }

  private ReadableInput buildWindowTestInputChannel() throws IOException
  {
    RowSignature inputSignature = RowSignature.builder()
                                              .add("cityName", ColumnType.STRING)
                                              .add("added", ColumnType.LONG)
                                              .build();

    List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("added", 1L, "cityName", "city1"),
        ImmutableMap.of("added", 1L, "cityName", "city2"),
        ImmutableMap.of("added", 2L, "cityName", "city3"),
        ImmutableMap.of("added", 2L, "cityName", "city4"),
        ImmutableMap.of("added", 2L, "cityName", "city5"),
        ImmutableMap.of("added", 3L, "cityName", "city6"),
        ImmutableMap.of("added", 3L, "cityName", "city7")
    );

    return makeChannelFromRows(rows, inputSignature, Collections.emptyList());
  }

  private ReadableInput makeChannelFromRows(
      List<Map<String, Object>> rows,
      RowSignature signature,
      List<KeyColumn> keyColumns
  ) throws IOException
  {
    RowBasedSegment<Map<String, Object>> segment = new RowBasedSegment<>(
        SegmentId.dummy("test"),
        Sequences.simple(rows),
        columnName -> m -> m.get(columnName),
        signature
    );

    return makeChannelFromCursorFactory(segment.asCursorFactory(), keyColumns);
  }

  private ReadableInput makeChannelFromCursorFactory(
      final CursorFactory cursorFactory,
      final List<KeyColumn> keyColumns
  ) throws IOException
  {
    return makeChannelFromCursorFactory(cursorFactory, keyColumns, 1000);
  }
}
