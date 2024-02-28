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
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.exec.DataServerQueryResult;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.querykit.BaseLeafFrameProcessor;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * A {@link FrameProcessor} that reads one {@link Frame} at a time from a particular segment, writes them
 * to a {@link WritableFrameChannel}, and returns the number of rows output.
 */
public class GroupByPreShuffleFrameProcessor extends BaseLeafFrameProcessor
{
  private static final Logger log = new Logger(GroupByPreShuffleFrameProcessor.class);
  private final GroupByQuery query;
  private final GroupingEngine groupingEngine;
  private final ColumnSelectorFactory frameWriterColumnSelectorFactory;
  private final Closer closer = Closer.create();

  private Yielder<ResultRow> resultYielder;
  private FrameWriter frameWriter;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed
  private SegmentsInputSlice handedOffSegments = null;
  private Yielder<Yielder<ResultRow>> currentResultsYielder;

  public GroupByPreShuffleFrameProcessor(
      final GroupByQuery query,
      final GroupingEngine groupingEngine,
      final ReadableInput baseInput,
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final ResourceHolder<WritableFrameChannel> outputChannelHolder,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder
  )
  {
    super(
        baseInput,
        segmentMapFn,
        outputChannelHolder,
        frameWriterFactoryHolder
    );
    this.query = query;
    this.groupingEngine = groupingEngine;
    this.frameWriterColumnSelectorFactory = RowBasedGrouperHelper.createResultRowBasedColumnSelectorFactory(
        query,
        () -> resultYielder.get(),
        RowSignature.Finalization.NO
    );
  }

  @Override
  protected ReturnOrAwait<SegmentsInputSlice> runWithDataServerQuery(DataServerQueryHandler dataServerQueryHandler) throws IOException
  {
    if (resultYielder == null || resultYielder.isDone()) {
      if (currentResultsYielder == null) {
        final DataServerQueryResult<ResultRow> dataServerQueryResult =
            dataServerQueryHandler.fetchRowsFromDataServer(
                groupingEngine.prepareGroupByQuery(query),
                Function.identity(),
                closer
            );
        handedOffSegments = dataServerQueryResult.getHandedOffSegments();
        if (!handedOffSegments.getDescriptors().isEmpty()) {
          log.info(
              "Query to dataserver for segments found [%d] handed off segments",
              handedOffSegments.getDescriptors().size()
          );
        }
        List<Yielder<ResultRow>> yielders = dataServerQueryResult.getResultsYielders();
        currentResultsYielder = Yielders.each(Sequences.simple(yielders));
      }
      if (currentResultsYielder.isDone()) {
        return ReturnOrAwait.returnObject(handedOffSegments);
      } else {
        resultYielder = currentResultsYielder.get();
        currentResultsYielder = currentResultsYielder.next(null);
      }
    }

    populateFrameWriterAndFlushIfNeeded();

    if ((resultYielder == null || resultYielder.isDone()) && currentResultsYielder.isDone()) {
      return ReturnOrAwait.returnObject(handedOffSegments);
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  protected ReturnOrAwait<Unit> runWithSegment(final SegmentWithDescriptor segment) throws IOException
  {
    if (resultYielder == null) {
      final ResourceHolder<Segment> segmentHolder = closer.register(segment.getOrLoad());

      final Sequence<ResultRow> rowSequence =
          groupingEngine.process(
              query.withQuerySegmentSpec(new SpecificSegmentSpec(segment.getDescriptor())),
              mapSegment(segmentHolder.get()).asStorageAdapter(),
              null
          );

      resultYielder = Yielders.each(rowSequence);
    }

    populateFrameWriterAndFlushIfNeeded();

    if (resultYielder == null || resultYielder.isDone()) {
      return ReturnOrAwait.returnObject(Unit.instance());
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  protected ReturnOrAwait<Unit> runWithInputChannel(
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
            groupingEngine.process(
                query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY)),
                mapSegment(frameSegment).asStorageAdapter(),
                null
            );

        resultYielder = Yielders.each(rowSequence);
      } else if (inputChannel.isFinished()) {
        flushFrameWriterIfNeeded();
        return ReturnOrAwait.returnObject(Unit.instance());
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
