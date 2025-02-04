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
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
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
import org.apache.druid.msq.util.MultiStageQueryContext;
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
  private static final List<Map<String, Object>> INPUT_ROWS = ImmutableList.of(
      ImmutableMap.of("added", 1L, "cityName", "city1"),
      ImmutableMap.of("added", 1L, "cityName", "city2"),
      ImmutableMap.of("added", 2L, "cityName", "city3"),
      ImmutableMap.of("added", 2L, "cityName", "city4"),
      ImmutableMap.of("added", 2L, "cityName", "city5"),
      ImmutableMap.of("added", 3L, "cityName", "city6"),
      ImmutableMap.of("added", 3L, "cityName", "city7")
  );

  @Test
  public void testFrameWriterReachingCapacity() throws IOException
  {
    // This test validates that all output rows are flushed to the output channel even if frame writer's
    // capacity is reached, by subsequent iterations of runIncrementally.
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

    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeRowBasedFrameWriterFactory(
            new ArenaMemoryAllocatorFactory(1 << 20),
            outputSignature,
            Collections.emptyList(),
            false
        ),
        INPUT_ROWS.size() / 4 // This forces frameWriter's capacity to be reached.
    );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();
    final WindowOperatorQueryFrameProcessor processor = new WindowOperatorQueryFrameProcessor(
        query.context(),
        factChannel.getChannel(),
        outputChannel.writable(),
        frameWriterFactory,
        frameReader,
        new ObjectMapper(),
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        )
    );

    exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(outputSignature)
    );

    List<List<Object>> outputRows = rowsFromProcessor.toList();
    Assert.assertEquals(INPUT_ROWS.size(), outputRows.size());

    for (int i = 0; i < INPUT_ROWS.size(); i++) {
      Map<String, Object> inputRow = INPUT_ROWS.get(i);
      List<Object> outputRow = outputRows.get(i);

      Assert.assertEquals("cityName should match", inputRow.get("cityName"), outputRow.get(0));
      Assert.assertEquals("added should match", inputRow.get("added"), outputRow.get(1));
      Assert.assertEquals("row_number() should be correct", (long) i + 1, outputRow.get(2));
    }
  }

  @Test
  public void testOutputChannelReachingCapacity() throws IOException
  {
    // This test validates that we don't end up writing multiple (2) frames to the output channel while reading from the input channel,
    // in the scenario when the input channel has finished and receiver's completed() gets called.
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
        new HashMap<>(
            // This ends up satisfying the criteria of needToProcessBatch() method,
            // so we end up processing the rows we've read, hence writing the 1st frame to the output channel.
            ImmutableMap.of(MultiStageQueryContext.MAX_ROWS_MATERIALIZED_IN_WINDOW, 12)
        ),
        outputSignature,
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        ),
        ImmutableList.of()
    );

    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeRowBasedFrameWriterFactory(
            new ArenaMemoryAllocatorFactory(1 << 20),
            outputSignature,
            Collections.emptyList(),
            false
        ),
        INPUT_ROWS.size() / 4 // This forces frameWriter's capacity to be reached, hence requiring another write.
    );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();
    final WindowOperatorQueryFrameProcessor processor = new WindowOperatorQueryFrameProcessor(
        query.context(),
        factChannel.getChannel(),
        outputChannel.writable(),
        frameWriterFactory,
        frameReader,
        new ObjectMapper(),
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        )
    );

    exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(outputSignature)
    );

    List<List<Object>> outputRows = rowsFromProcessor.toList();
    Assert.assertEquals(INPUT_ROWS.size(), outputRows.size());

    for (int i = 0; i < INPUT_ROWS.size(); i++) {
      Map<String, Object> inputRow = INPUT_ROWS.get(i);
      List<Object> outputRow = outputRows.get(i);

      Assert.assertEquals("cityName should match", inputRow.get("cityName"), outputRow.get(0));
      Assert.assertEquals("added should match", inputRow.get("added"), outputRow.get(1));
      Assert.assertEquals("row_number() should be correct", (long) i + 1, outputRow.get(2));
    }
  }

  @Test
  public void testProcessorRun() throws Exception
  {
    runProcessor(100, 1);
  }

  @Test
  public void testMaxRowsMaterializedConstraint()
  {
    final RuntimeException e = Assert.assertThrows(
        RuntimeException.class,
        () -> runProcessor(2, 3)
    );
    MatcherAssert.assertThat(
        ((MSQException) e.getCause().getCause()).getFault(),
        CoreMatchers.instanceOf(TooManyRowsInAWindowFault.class)
    );
    Assert.assertTrue(e.getMessage().contains("TooManyRowsInAWindow: Too many rows in a window (requested = 7, max = 2)"));
  }

  public void runProcessor(int maxRowsMaterialized, int expectedNumFramesWritten) throws Exception
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
        new HashMap<>(
            ImmutableMap.of(MultiStageQueryContext.MAX_ROWS_MATERIALIZED_IN_WINDOW, maxRowsMaterialized)
        ),
        outputSignature,
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        ),
        ImmutableList.of()
    );

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeRowBasedFrameWriterFactory(
            new ArenaMemoryAllocatorFactory(1 << 20),
            outputSignature,
            Collections.emptyList(),
            false
        ),
        100
    );

    final WindowOperatorQueryFrameProcessor processor = new WindowOperatorQueryFrameProcessor(
        query.context(),
        factChannel.getChannel(),
        countingWritableFrameChannel,
        frameWriterFactory,
        frameReader,
        new ObjectMapper(),
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        )
    );

    exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(outputSignature)
    );

    final List<List<Object>> rows = rowsFromProcessor.toList();

    long actualNumFrames = Arrays.stream(channelCounters.snapshot().getFrames()).findFirst().getAsLong();
    Assert.assertEquals(expectedNumFramesWritten, actualNumFrames);
    Assert.assertEquals(7, rows.size());
  }

  private ReadableInput buildWindowTestInputChannel() throws IOException
  {
    RowSignature inputSignature = RowSignature.builder()
                                              .add("cityName", ColumnType.STRING)
                                              .add("added", ColumnType.LONG)
                                              .build();
    return makeChannelFromRows(INPUT_ROWS, inputSignature, Collections.emptyList());
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
