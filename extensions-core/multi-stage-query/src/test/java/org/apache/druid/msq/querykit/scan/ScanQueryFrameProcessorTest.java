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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.querykit.LazyResourceHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ScanQueryFrameProcessorTest extends InitializedNullHandlingTest
{
  private FrameProcessorExecutor exec;

  @Before
  public void setUp()
  {
    exec = new FrameProcessorExecutor(MoreExecutors.listeningDecorator(Execs.singleThreaded("test-exec")));
  }

  @After
  public void tearDown() throws Exception
  {
    exec.getExecutorService().shutdownNow();
    exec.getExecutorService().awaitTermination(10, TimeUnit.MINUTES);
  }

  @Test
  public void test_runWithInputChannel() throws Exception
  {
    final IncrementalIndexStorageAdapter adapter =
        new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromAdapter(adapter)
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

    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
              .columns(adapter.getRowSignature().getColumnNames())
              .legacy(false)
              .build();

    final StagePartition stagePartition = new StagePartition(new StageId("query", 0), 0);

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = limitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.ROW_BASED,
            HeapMemoryAllocator.unlimited(),
            signature,
            Collections.emptyList()
        ),
        1
    );

    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        ReadableInput.channel(inputChannel.readable(), FrameReader.create(signature), stagePartition),
        Int2ObjectMaps.emptyMap(),
        new JoinableFactoryWrapper(NoopJoinableFactory.INSTANCE),
        new ResourceHolder<WritableFrameChannel>()
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
        new LazyResourceHolder<>(() -> Pair.of(frameWriterFactory, () -> {})),
        null,
        0L
    );

    ListenableFuture<Long> retVal = exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(signature)
    );

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, signature, false),
        rowsFromProcessor
    );

    Assert.assertEquals(adapter.getNumRows(), (long) retVal.get());
  }

  /**
   * Wraps a {@link FrameWriterFactory}, creating a new factory that returns {@link FrameWriter} which write
   * a limited number of rows.
   */
  private static FrameWriterFactory limitedFrameWriterFactory(final FrameWriterFactory baseFactory, final int rowLimit)
  {
    return new FrameWriterFactory()
    {
      @Override
      public FrameWriter newFrameWriter(ColumnSelectorFactory columnSelectorFactory)
      {
        return new LimitedFrameWriter(baseFactory.newFrameWriter(columnSelectorFactory), rowLimit);
      }

      @Override
      public long allocatorCapacity()
      {
        return baseFactory.allocatorCapacity();
      }
    };
  }

  private static class LimitedFrameWriter implements FrameWriter
  {
    private final FrameWriter baseWriter;
    private final int rowLimit;

    public LimitedFrameWriter(FrameWriter baseWriter, int rowLimit)
    {
      this.baseWriter = baseWriter;
      this.rowLimit = rowLimit;
    }

    @Override
    public boolean addSelection()
    {
      if (baseWriter.getNumRows() >= rowLimit) {
        return false;
      } else {
        return baseWriter.addSelection();
      }
    }

    @Override
    public int getNumRows()
    {
      return baseWriter.getNumRows();
    }

    @Override
    public long getTotalSize()
    {
      return baseWriter.getTotalSize();
    }

    @Override
    public long writeTo(WritableMemory memory, long position)
    {
      return baseWriter.writeTo(memory, position);
    }

    @Override
    public void close()
    {
      baseWriter.close();
    }
  }
}
