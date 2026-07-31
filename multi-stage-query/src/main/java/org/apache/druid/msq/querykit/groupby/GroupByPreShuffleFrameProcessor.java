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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
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
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.querykit.BaseLeafFrameProcessor;
import org.apache.druid.msq.querykit.ReadableInput;
import org.apache.druid.msq.querykit.SegmentReferenceHolder;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.AsyncCursorHolder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
  @Nullable
  private final QueryToolChest<ResultRow, GroupByQuery> toolChest;
  private final NonBlockingPool<ByteBuffer> bufferPool;
  private final ColumnSelectorFactory frameWriterColumnSelectorFactory;
  private final Closer closer = Closer.create();

  private Yielder<ResultRow> resultYielder;
  private FrameWriter frameWriter;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed
  private SegmentsInputSlice handedOffSegments = null;
  private Yielder<Yielder<ResultRow>> currentResultsYielder;
  private ListenableFuture<DataServerQueryResult<ResultRow>> dataServerQueryResultFuture;
  @Nullable
  private CursorFactory currentCursorFactory;
  @Nullable
  private TimeBoundaryInspector currentTimeBoundaryInspector;
  /**
   * In-flight {@link GroupingEngine#makeCursorHolderAsync} handle for the current segment, when {@link #resultYielder}
   * has not yet been derived. Registered on {@link #closer} so the produced {@link CursorHolder} is always disposed
   * regardless of where the underlying load is in its lifecycle. Cleared after we transfer ownership of the holder to
   * {@link GroupingEngine#processCursorHolder} (which moves it onto the resulting Sequence's baggage closer).
   */
  @Nullable
  private AsyncCursorHolder asyncCursorHolder;

  public GroupByPreShuffleFrameProcessor(
      final GroupByQuery query,
      final GroupingEngine groupingEngine,
      @Nullable final QueryToolChest<ResultRow, GroupByQuery> toolChest,
      final NonBlockingPool<ByteBuffer> bufferPool,
      final ReadableInput baseInput,
      final SegmentMapFunction segmentMapFn,
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
    this.toolChest = toolChest;
    this.bufferPool = bufferPool;
    this.frameWriterColumnSelectorFactory = RowBasedGrouperHelper.createResultRowBasedColumnSelectorFactory(
        query,
        () -> resultYielder.get(),
        RowSignature.Finalization.NO
    );
  }

  @Override
  protected ReturnOrAwait<SegmentsInputSlice> runWithDataServerQuery(DataServerQueryHandler dataServerQueryHandler) throws IOException
  {
    if (toolChest == null) {
      // toolChest is always set in production, but may not always be set in tests.
      throw DruidException.defensive("toolChest is required for data server queries");
    }

    if (resultYielder == null || resultYielder.isDone()) {
      if (currentResultsYielder == null) {
        if (dataServerQueryResultFuture == null) {
          final GroupByQuery preparedQuery = groupingEngine.prepareGroupByQuery(query);
          final Function<ResultRow, ResultRow> preComputeManipulatorFn =
              toolChest.makePreComputeManipulatorFn(preparedQuery, MetricManipulatorFns.deserializing());
          dataServerQueryResultFuture =
              dataServerQueryHandler.fetchRowsFromDataServer(
                  preparedQuery,
                  toolChest.getBaseResultType(),
                  sequence -> sequence.map(preComputeManipulatorFn),
                  closer
              );

          // Give up the processing thread while we wait for the query to finish. This is only really asynchronous
          // with Dart. On tasks, the IndexerDataServerQueryHandler does not return from fetchRowsFromDataServer until
          // the response has started to come back.
          return ReturnOrAwait.awaitAllFutures(Collections.singletonList(dataServerQueryResultFuture));
        }

        final DataServerQueryResult<ResultRow> dataServerQueryResult =
            FutureUtils.getUncheckedImmediately(dataServerQueryResultFuture);
        dataServerQueryResultFuture = null;
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
  protected ReturnOrAwait<Unit> runWithSegment(final SegmentReferenceHolder segmentHolder) throws IOException
  {
    if (resultYielder == null) {
      if (asyncCursorHolder == null && currentCursorFactory == null) {
        // First invocation for this segment: map it, check the TimeBoundary fast path, otherwise kick off the async
        // cursor-holder load and cache the cursor factory + time-boundary inspector for the follow-up invocation.
        final Segment segment = mapSegment(segmentHolder, closer);
        currentTimeBoundaryInspector = segment.as(TimeBoundaryInspector.class);

        if (GroupByTimeBoundaryUtils.canUseTimeBoundaryInspector(
            query,
            currentTimeBoundaryInspector,
            segmentHolder.getDescriptor()
        )) {
          // Resolve this query using the TimeBoundaryInspector, no need for a cursor.
          resultYielder = Yielders.each(
              Sequences.simple(
                  List.of(GroupByTimeBoundaryUtils.computeTimeBoundaryResult(query, currentTimeBoundaryInspector))
              )
          );
        } else {
          currentCursorFactory = Objects.requireNonNull(segment.as(CursorFactory.class));
          // Resolve this query using a cursor.
          asyncCursorHolder = closer.register(
              groupingEngine.makeCursorHolderAsync(
                  computeQueryForSegment(segmentHolder.getDescriptor()),
                  currentCursorFactory,
                  null
              )
          );
        }
      }

      if (asyncCursorHolder != null) {
        if (!asyncCursorHolder.isReady()) {
          final SettableFuture<?> awaitFuture = SettableFuture.create();
          asyncCursorHolder.addReadyCallback(() -> awaitFuture.set(null));
          return ReturnOrAwait.awaitAllFutures(List.of(awaitFuture));
        }
        // The holder is ready, ownership of the holder transitions onto the returned Sequence's baggage closer
        final CursorHolder holder = asyncCursorHolder.release();
        asyncCursorHolder = null;
        // currentCursorFactory is non-null whenever asyncCursorHolder is non-null (both are set together in the
        // first-invocation branch above). The requireNonNull pins the invariant for static analysis.
        final Sequence<ResultRow> rowSequence = groupingEngine.processCursorHolder(
            computeQueryForSegment(segmentHolder.getDescriptor()),
            Objects.requireNonNull(currentCursorFactory),
            holder,
            currentTimeBoundaryInspector,
            bufferPool,
            null
        );
        resultYielder = Yielders.each(rowSequence);
      }
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
        final Frame frame = inputChannel.readFrame();
        final FrameSegment frameSegment = new FrameSegment(frame, inputFrameReader);
        final Segment mappedSegment = mapUnmanagedSegment(frameSegment);

        final Sequence<ResultRow> rowSequence =
            groupingEngine.process(
                query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY)),
                Objects.requireNonNull(mappedSegment.as(CursorFactory.class)),
                mappedSegment.as(TimeBoundaryInspector.class),
                bufferPool,
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
      Iterables.getOnlyElement(outputChannels()).write(frame);
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

  private GroupByQuery computeQueryForSegment(final SegmentDescriptor descriptor)
  {
    return (GroupByQuery) query
        .withQuerySegmentSpec(new SpecificSegmentSpec(descriptor))
        .optimizeForSegment(new PerSegmentQueryOptimizationContext(descriptor));
  }
}
