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
import org.apache.druid.msq.querykit.FrameProcessorTestBase;
import org.apache.druid.msq.querykit.ReadableInput;
import org.apache.druid.msq.querykit.SegmentReferenceHolder;
import org.apache.druid.msq.test.LimitedFrameWriterFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.AsyncCursorHolder;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            cursorFactory.getRowSignature(),
            Collections.emptyList(),
            false
        ),
        1
    );

    final ReferenceCountedSegmentProvider segmentReferenceProvider =
        new ReferenceCountedSegmentProvider(new QueryableIndexSegment(queryableIndex, SegmentId.dummy("test")));
    Assertions.assertEquals(0, segmentReferenceProvider.getNumReferences());
    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        null,
        new DefaultObjectMapper(),
        ReadableInput.segment(
            new SegmentReferenceHolder(
                new SegmentReference(
                    SegmentId.dummy("test").toDescriptor(),
                    segmentReferenceProvider.acquireReference(),
                    null
                ),
                null
            )
        ),
        SegmentMapFunction.IDENTITY,
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
    Assertions.assertEquals(0, segmentReferenceProvider.getNumReferences()); // Segment reference was closed
  }

  @Test
  public void test_runWithInputChannel() throws Exception
  {
    final CursorFactory cursorFactory =
        new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .maxRowsPerFrame(5)
                            .frameType(FrameType.latestRowBased())
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

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
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
        ReadableInput.channel(inputChannel.readable(), FrameReader.create(signature), 0, 0),
        SegmentMapFunction.IDENTITY,
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

  /**
   * Verifies that {@link ScanQueryFrameProcessor#runWithSegment} yields via {@link
   * org.apache.druid.frame.processor.ReturnOrAwait#awaitAllFutures} when {@link CursorFactory#makeCursorHolderAsync}
   * returns an {@link AsyncCursorHolder} that has not yet completed, and resumes after it does. Exercises the partial
   * / non-blocking I/O integration path on the MSQ side without requiring a real partial segment.
   */
  @Test
  public void test_runWithSegments_asyncCursorHolderAwaits() throws Exception
  {
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();
    final CursorFactory baseCursorFactory = new QueryableIndexCursorFactory(queryableIndex);
    final AsyncCursorHolder deferredAsyncHolder = new AsyncCursorHolder(null);

    final CursorFactory deferredCursorFactory = new CursorFactory()
    {
      @Override
      public CursorHolder makeCursorHolder(CursorBuildSpec spec)
      {
        return baseCursorFactory.makeCursorHolder(spec);
      }

      @Override
      public AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
      {
        return deferredAsyncHolder;
      }

      @Override
      public RowSignature getRowSignature()
      {
        return baseCursorFactory.getRowSignature();
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return baseCursorFactory.getColumnCapabilities(column);
      }
    };

    final QueryableIndexSegment baseSegment = new QueryableIndexSegment(queryableIndex, SegmentId.dummy("test"));
    final Segment wrappedSegment = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return baseSegment.getId();
      }

      @Override
      public Interval getDataInterval()
      {
        return baseSegment.getDataInterval();
      }

      @Override
      public void validateOrElseThrow(PolicyEnforcer policyEnforcer)
      {
        baseSegment.validateOrElseThrow(policyEnforcer);
      }

      @Override
      public boolean isTombstone()
      {
        return baseSegment.isTombstone();
      }

      @Override
      public String getDebugString()
      {
        return baseSegment.getDebugString();
      }

      @SuppressWarnings("unchecked")
      @Override
      public <T> T as(@Nonnull Class<T> clazz)
      {
        if (CursorFactory.class.equals(clazz)) {
          return (T) deferredCursorFactory;
        }
        return baseSegment.as(clazz);
      }

      @Override
      public void close()
      {
        baseSegment.close();
      }
    };

    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
              .columns(baseCursorFactory.getRowSignature().getColumnNames())
              .build();

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            baseCursorFactory.getRowSignature(),
            Collections.emptyList(),
            false
        ),
        1
    );

    final ReferenceCountedSegmentProvider segmentReferenceProvider = new ReferenceCountedSegmentProvider(wrappedSegment);
    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        null,
        new DefaultObjectMapper(),
        ReadableInput.segment(
            new SegmentReferenceHolder(
                new SegmentReference(
                    SegmentId.dummy("test").toDescriptor(),
                    segmentReferenceProvider.acquireReference(),
                    null
                ),
                null
            )
        ),
        SegmentMapFunction.IDENTITY,
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

    final ListenableFuture<Object> retVal = exec.runFully(processor, null);

    // Processor should be awaiting the deferred holder and have produced no rows yet.
    Thread.sleep(200);
    Assertions.assertFalse(retVal.isDone(), "processor should be awaiting the deferred AsyncCursorHolder");
    Assertions.assertFalse(outputChannel.readable().canRead(), "no frames should have been written yet");

    // Complete the load and verify the processor proceeds to produce all rows.
    deferredAsyncHolder.set(baseCursorFactory.makeCursorHolder(ScanQueryEngine.makeCursorBuildSpec(query, null)));

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(baseCursorFactory.getRowSignature())
    );

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromCursorFactory(baseCursorFactory, baseCursorFactory.getRowSignature(), false),
        rowsFromProcessor
    );

    Assert.assertEquals(Unit.instance(), retVal.get(30, TimeUnit.SECONDS));
  }
}
