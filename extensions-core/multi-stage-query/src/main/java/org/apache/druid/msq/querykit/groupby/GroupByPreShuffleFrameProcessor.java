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

import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.querykit.BaseLeafFrameProcessor;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;

/**
 * A {@link FrameProcessor} that reads one {@link Frame} at a time from a particular segment, writes them
 * to a {@link WritableFrameChannel}, and returns the number of rows output.
 */
public class GroupByPreShuffleFrameProcessor extends BaseLeafFrameProcessor
{
  private final GroupByQuery query;
  private final GroupByStrategySelector strategySelector;
  private final ColumnSelectorFactory frameWriterColumnSelectorFactory;
  private final Closer closer = Closer.create();

  private Yielder<ResultRow> resultYielder;
  private FrameWriter frameWriter;
  private long rowsOutput;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed

  public GroupByPreShuffleFrameProcessor(
      final GroupByQuery query,
      final ReadableInput baseInput,
      final Int2ObjectMap<ReadableInput> sideChannels,
      final GroupByStrategySelector strategySelector,
      final JoinableFactoryWrapper joinableFactory,
      final ResourceHolder<WritableFrameChannel> outputChannel,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      final long memoryReservedForBroadcastJoin
  )
  {
    super(
        query,
        baseInput,
        sideChannels,
        joinableFactory,
        outputChannel,
        frameWriterFactoryHolder,
        memoryReservedForBroadcastJoin
    );
    this.query = query;
    this.strategySelector = strategySelector;
    this.frameWriterColumnSelectorFactory = RowBasedGrouperHelper.createResultRowBasedColumnSelectorFactory(
        query,
        () -> resultYielder.get(),
        RowSignature.Finalization.NO
    );
  }

  @Override
  protected ReturnOrAwait<Long> runWithSegment(final SegmentWithDescriptor segment) throws IOException
  {
    if (resultYielder == null) {
      closer.register(segment);

      final Sequence<ResultRow> rowSequence =
          strategySelector.strategize(query)
                          .process(
                              query.withQuerySegmentSpec(new SpecificSegmentSpec(segment.getDescriptor())),
                              mapSegment(segment.getOrLoadSegment()).asStorageAdapter(),
                              null
                          );

      resultYielder = Yielders.each(rowSequence);
    }

    populateFrameWriterAndFlushIfNeeded();

    if (resultYielder == null || resultYielder.isDone()) {
      return ReturnOrAwait.returnObject(rowsOutput);
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  protected ReturnOrAwait<Long> runWithInputChannel(
      final ReadableFrameChannel inputChannel,
      final FrameReader inputFrameReader
  ) throws IOException
  {
    if (resultYielder == null || resultYielder.isDone()) {
      closeAndDiscardResultYielder();

      if (inputChannel.canRead()) {
        final Frame frame = inputChannel.read();
        final FrameSegment frameSegment = new FrameSegment(frame, inputFrameReader, SegmentId.dummy("x"));

        final Sequence<ResultRow> rowSequence =
            strategySelector.strategize(query)
                            .process(
                                query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY)),
                                mapSegment(frameSegment).asStorageAdapter(),
                                null
                            );

        resultYielder = Yielders.each(rowSequence);
      } else if (inputChannel.isFinished()) {
        flushFrameWriterIfNeeded();
        return ReturnOrAwait.returnObject(rowsOutput);
      } else {
        return ReturnOrAwait.awaitAll(inputChannels().size());
      }
    }

    // Cursor has some more data in it.
    populateFrameWriterAndFlushIfNeeded();

    if (resultYielder == null || resultYielder.isDone()) {
      closeAndDiscardResultYielder();
      return ReturnOrAwait.awaitAll(inputChannels().size());
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    closer.register(this::closeAndDiscardResultYielder);
    closer.register(frameWriter);
    closer.register(super::cleanup);
    closer.close();
  }

  private void populateFrameWriterAndFlushIfNeeded() throws IOException
  {
    createFrameWriterIfNeeded();

    while (!resultYielder.isDone()) {
      final boolean didAddToFrame = frameWriter.addSelection();

      if (didAddToFrame) {
        resultYielder = resultYielder.next(null);
      } else if (frameWriter.getNumRows() == 0) {
        throw new FrameRowTooLargeException(currentAllocatorCapacity);
      } else {
        flushFrameWriterIfNeeded();
        return;
      }
    }

    flushFrameWriterIfNeeded();
    closeAndDiscardResultYielder();
  }

  private void createFrameWriterIfNeeded()
  {
    if (frameWriter == null) {
      final FrameWriterFactory frameWriterFactory = getFrameWriterFactory();
      frameWriter = frameWriterFactory.newFrameWriter(frameWriterColumnSelectorFactory);
      currentAllocatorCapacity = frameWriterFactory.allocatorCapacity();
    }
  }

  private void flushFrameWriterIfNeeded() throws IOException
  {
    if (frameWriter != null && frameWriter.getNumRows() > 0) {
      final Frame frame = Frame.wrap(frameWriter.toByteArray());
      Iterables.getOnlyElement(outputChannels()).write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
      frameWriter.close();
      frameWriter = null;
      rowsOutput += frame.numRows();
    }
  }

  private void closeAndDiscardResultYielder() throws IOException
  {
    final Yielder<ResultRow> tmp = resultYielder;
    resultYielder = null;

    if (tmp != null) {
      tmp.close();
    }
  }
}
