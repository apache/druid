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

package org.apache.druid.msq.querykit.scan;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.collections.StupidResourceHolder;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.querykit.FrameProcessorTestBase;
import org.apache.druid.msq.test.LimitedFrameWriterFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.CompleteSegment;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ScanQueryFrameProcessorTest extends FrameProcessorTestBase
{

  @Test
  public void test_runWithSegments() throws Exception
  {
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();

    final CursorFactory cursorFactory =
        new QueryableIndexCursorFactory(queryableIndex);

    // put funny intervals on query to ensure it is adjusted to the segment interval before building cursor
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      ImmutableList.of(
                          Intervals.of("2001-01-01T00Z/2011-01-01T00Z"),
                          Intervals.of("2011-01-02T00Z/2021-01-01T00Z")
                      )
                  )
              )
              .columns(cursorFactory.getRowSignature().getColumnNames())
              .build();

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeRowBasedFrameWriterFactory(
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            cursorFactory.getRowSignature(),
            Collections.emptyList(),
            false
        ),
        1
    );

    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        null,
        new DefaultObjectMapper(),
        ReadableInput.segment(
            new SegmentWithDescriptor(
                () -> new StupidResourceHolder<>(new CompleteSegment(null, new QueryableIndexSegment(queryableIndex, SegmentId.dummy("test")))),
                new RichSegmentDescriptor(queryableIndex.getDataInterval(), queryableIndex.getDataInterval(), "dummy_version", 0)
            )
        ),
        Function.identity(),
        new ResourceHolder<>()
        {
          @Override
          public WritableFrameChannel get()
          {
            return outputChannel.writable();
          }

          @Override
          public void close()
          {
            try {
              outputChannel.writable().close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        },
        new ReferenceCountingResourceHolder<>(frameWriterFactory, () -> {})
    );

    ListenableFuture<Object> retVal = exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(cursorFactory.getRowSignature())
    );

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromCursorFactory(cursorFactory, cursorFactory.getRowSignature(), false),
        rowsFromProcessor
    );

    Assert.assertEquals(Unit.instance(), retVal.get());
  }

  @Test
  public void test_runWithInputChannel() throws Exception
  {
    final CursorFactory cursorFactory =
        new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .maxRowsPerFrame(5)
                            .frameType(FrameType.ROW_BASED)
                            .allocator(ArenaMemoryAllocator.createOnHeap(100_000));

    final RowSignature signature = frameSequenceBuilder.signature();
    final List<Frame> frames = frameSequenceBuilder.frames().toList();
    final BlockingQueueFrameChannel inputChannel = new BlockingQueueFrameChannel(frames.size());
    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    try (final WritableFrameChannel writableInputChannel = inputChannel.writable()) {
      for (final Frame frame : frames) {
        writableInputChannel.write(frame);
      }
    }

    // put funny intervals on query to ensure it is validated before building cursor
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      ImmutableList.of(
                          Intervals.of("2001-01-01T00Z/2011-01-01T00Z"),
                          Intervals.of("2011-01-02T00Z/2021-01-01T00Z")
                      )
                  )
              )
              .columns(cursorFactory.getRowSignature().getColumnNames())
              .build();

    final StagePartition stagePartition = new StagePartition(new StageId("query", 0), 0);

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeRowBasedFrameWriterFactory(
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            signature,
            Collections.emptyList(),
            false
        ),
        1
    );

    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        null,
        new DefaultObjectMapper(),
        ReadableInput.channel(inputChannel.readable(), FrameReader.create(signature), stagePartition),
        Function.identity(),
        new ResourceHolder<>()
        {
          @Override
          public WritableFrameChannel get()
          {
            return outputChannel.writable();
          }

          @Override
          public void close()
          {
            try {
              outputChannel.writable().close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        },
        new ReferenceCountingResourceHolder<>(frameWriterFactory, () -> {})
    );

    ListenableFuture<Object> retVal = exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(signature)
    );

    final RuntimeException e = Assert.assertThrows(
        RuntimeException.class,
        rowsFromProcessor::toList
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
            "Expected eternity intervals, but got[[2001-01-01T00:00:00.000Z/2011-01-01T00:00:00.000Z, "
            + "2011-01-02T00:00:00.000Z/2021-01-01T00:00:00.000Z]]"))
    );
  }
}
