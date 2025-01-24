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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.util.SettableLongVirtualColumn;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.InvalidFieldException;
import org.apache.druid.frame.write.InvalidNullByteException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.exec.DataServerQueryResult;
import org.apache.druid.msq.input.ParseExceptionUtils;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.external.ExternalSegment;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.querykit.BaseLeafFrameProcessor;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.query.Order;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CompleteSegment;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link FrameProcessor} that reads one {@link Frame} at a time from a particular segment, writes them
 * to a {@link WritableFrameChannel}, and returns the number of rows output.
 */
public class ScanQueryFrameProcessor extends BaseLeafFrameProcessor
{
  private static final Logger log = new Logger(ScanQueryFrameProcessor.class);
  private final ScanQuery query;
  private final AtomicLong runningCountForLimit;
  private final ObjectMapper jsonMapper;
  private final SettableLongVirtualColumn partitionBoostVirtualColumn;
  private final VirtualColumns frameWriterVirtualColumns;
  private final Closer closer = Closer.create();

  private Cursor cursor;
  private Closeable cursorCloser;
  private Segment segment;
  private final SimpleSettableOffset cursorOffset = new SimpleAscendingOffset(Integer.MAX_VALUE);
  private FrameWriter frameWriter;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed
  private SegmentsInputSlice handedOffSegments = null;

  public ScanQueryFrameProcessor(
      final ScanQuery query,
      @Nullable final AtomicLong runningCountForLimit,
      final ObjectMapper jsonMapper,
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
    this.runningCountForLimit = runningCountForLimit;
    this.jsonMapper = jsonMapper;
    this.partitionBoostVirtualColumn = new SettableLongVirtualColumn(QueryKitUtils.PARTITION_BOOST_COLUMN);

    final List<VirtualColumn> frameWriterVirtualColumns = new ArrayList<>();
    frameWriterVirtualColumns.add(partitionBoostVirtualColumn);

    final VirtualColumn segmentGranularityVirtualColumn =
        QueryKitUtils.makeSegmentGranularityVirtualColumn(jsonMapper, query.context());

    if (segmentGranularityVirtualColumn != null) {
      frameWriterVirtualColumns.add(segmentGranularityVirtualColumn);
    }

    this.frameWriterVirtualColumns = VirtualColumns.create(frameWriterVirtualColumns);
  }

  @Override
  public ReturnOrAwait<Object> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (runningCountForLimit != null
        && runningCountForLimit.get() > query.getScanRowsOffset() + query.getScanRowsLimit()) {
      return ReturnOrAwait.returnObject(Unit.instance());
    }

    return super.runIncrementally(readableInputs);
  }

  @Override
  public void cleanup() throws IOException
  {
    closer.register(cursorCloser);
    closer.register(frameWriter);
    closer.register(super::cleanup);
    closer.close();
  }

  public static Sequence<Object[]> mappingFunction(Sequence<ScanResultValue> inputSequence)
  {
    return inputSequence.flatMap(resultRow -> {
      List<List<Object>> events = (List<List<Object>>) resultRow.getEvents();
      return Sequences.simple(events);
    }).map(List::toArray);
  }

  /**
   * Prepares the scan query to be sent to a data server.
   * If the query contains a non-time ordering, removes the ordering and limit, as the native query stack does not
   * support it.
   */
  private static ScanQuery prepareScanQueryForDataServer(@NotNull ScanQuery scanQuery)
  {
    if (Order.NONE.equals(scanQuery.getTimeOrder()) && !scanQuery.getOrderBys().isEmpty()) {
      return Druids.ScanQueryBuilder.copy(scanQuery)
                                    .orderBy(ImmutableList.of())
                                    .limit(0)
                                    .build();
    } else {
      return scanQuery;
    }
  }

  @Override
  protected ReturnOrAwait<SegmentsInputSlice> runWithDataServerQuery(final DataServerQueryHandler dataServerQueryHandler) throws IOException
  {
    if (cursor == null) {
      ScanQuery preparedQuery = prepareScanQueryForDataServer(query);
      final DataServerQueryResult<Object[]> dataServerQueryResult =
          dataServerQueryHandler.fetchRowsFromDataServer(
              preparedQuery,
              ScanQueryFrameProcessor::mappingFunction,
              closer
          );
      handedOffSegments = dataServerQueryResult.getHandedOffSegments();
      if (!handedOffSegments.getDescriptors().isEmpty()) {
        log.info(
            "Query to dataserver for segments found [%d] handed off segments",
            handedOffSegments.getDescriptors().size()
        );
      }
      RowSignature rowSignature = ScanQueryKit.getAndValidateSignature(preparedQuery, jsonMapper);
      List<Cursor> cursors = dataServerQueryResult.getResultsYielders().stream().map(yielder -> {
        Pair<Cursor, Closeable> cursorFromIterable = IterableRowsCursorHelper.getCursorFromYielder(
            yielder,
            rowSignature
        );
        closer.register(cursorFromIterable.rhs);
        return cursorFromIterable.lhs;
      }).collect(Collectors.toList());

      final Yielder<Cursor> cursorYielder = Yielders.each(Sequences.simple(cursors));

      if (cursorYielder.isDone()) {
        // No cursors!
        cursorYielder.close();
        return ReturnOrAwait.returnObject(handedOffSegments);
      } else {
        final long rowsFlushed = setNextCursor(cursorYielder.get(), null, null);
        closer.register(cursorYielder);
        if (rowsFlushed > 0) {
          return ReturnOrAwait.runAgain();
        }
      }
    }

    populateFrameWriterAndFlushIfNeededWithExceptionHandling();

    if (cursor.isDone()) {
      flushFrameWriter();
    }

    if (cursor.isDone() && (frameWriter == null || frameWriter.getNumRows() == 0)) {
      return ReturnOrAwait.returnObject(handedOffSegments);
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  protected ReturnOrAwait<Unit> runWithSegment(final SegmentWithDescriptor segment) throws IOException
  {
    if (cursor == null) {
      final ResourceHolder<CompleteSegment> segmentHolder = closer.register(segment.getOrLoad());

      final Segment mappedSegment = mapSegment(segmentHolder.get().getSegment());
      final CursorFactory cursorFactory = mappedSegment.asCursorFactory();
      if (cursorFactory == null) {
        throw new ISE(
            "Null cursor factory found. Probably trying to issue a query against a segment being memory unmapped."
        );
      }

      final CursorHolder nextCursorHolder =
          cursorFactory.makeCursorHolder(
              ScanQueryEngine.makeCursorBuildSpec(
                  query.withQuerySegmentSpec(new SpecificSegmentSpec(segment.getDescriptor())),
                  null
              )
          );
      final Cursor nextCursor = nextCursorHolder.asCursor();

      if (nextCursor == null) {
        // No cursors!
        nextCursorHolder.close();
        return ReturnOrAwait.returnObject(Unit.instance());
      } else {
        final long rowsFlushed = setNextCursor(nextCursor, nextCursorHolder, segmentHolder.get().getSegment());
        assert rowsFlushed == 0; // There's only ever one cursor when running with a segment
      }
    }

    populateFrameWriterAndFlushIfNeededWithExceptionHandling();

    if (cursor.isDone()) {
      flushFrameWriter();
    }

    if (cursor.isDone() && (frameWriter == null || frameWriter.getNumRows() == 0)) {
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
    if (cursor == null || cursor.isDone()) {
      if (inputChannel.canRead()) {
        final Frame frame = inputChannel.read();
        final FrameSegment frameSegment = new FrameSegment(frame, inputFrameReader, SegmentId.dummy("scan"));

        final Segment mappedSegment = mapSegment(frameSegment);
        final CursorFactory cursorFactory = mappedSegment.asCursorFactory();
        if (cursorFactory == null) {
          throw new ISE(
              "Null cursor factory found. Probably trying to issue a query against a segment being memory unmapped."
          );
        }

        if (!Intervals.ONLY_ETERNITY.equals(query.getIntervals())) {
          // runWithInputChannel is for running on subquery results, where we don't expect to see "intervals" set.
          // The SQL planner avoid it for subqueries; see DruidQuery#canUseIntervalFiltering.
          throw DruidException.defensive("Expected eternity intervals, but got[%s]", query.getIntervals());
        }

        final CursorHolder nextCursorHolder =
            cursorFactory.makeCursorHolder(ScanQueryEngine.makeCursorBuildSpec(query, null));
        final Cursor nextCursor = nextCursorHolder.asCursor();

        if (nextCursor == null) {
          // no cursor
          nextCursorHolder.close();
          return ReturnOrAwait.returnObject(Unit.instance());
        }
        final long rowsFlushed = setNextCursor(nextCursor, nextCursorHolder, frameSegment);

        if (rowsFlushed > 0) {
          return ReturnOrAwait.runAgain();
        }
      } else if (inputChannel.isFinished()) {
        flushFrameWriter();
        return ReturnOrAwait.returnObject(Unit.instance());
      } else {
        return ReturnOrAwait.awaitAll(inputChannels().size());
      }
    }

    // Cursor has some more data in it.
    populateFrameWriterAndFlushIfNeededWithExceptionHandling();

    if (cursor.isDone()) {
      return ReturnOrAwait.awaitAll(inputChannels().size());
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  /**
   * Populates the null exception message with the input source name and the row number
   */
  private void populateFrameWriterAndFlushIfNeededWithExceptionHandling()
  {
    try {
      populateFrameWriterAndFlushIfNeeded();
    }
    catch (InvalidNullByteException inbe) {
      InvalidNullByteException.Builder builder = InvalidNullByteException.builder(inbe);
      throw
          builder.source(ParseExceptionUtils.generateReadableInputSourceNameFromMappedSegment(this.segment)) // frame segment
                 .rowNumber(this.cursorOffset.getOffset() + 1)
                 .build();
    }
    catch (InvalidFieldException ffwe) {
      InvalidFieldException.Builder builder = InvalidFieldException.builder(ffwe);
      throw
          builder.source(ParseExceptionUtils.generateReadableInputSourceNameFromMappedSegment(this.segment)) // frame segment
                 .rowNumber(this.cursorOffset.getOffset() + 1)
                 .build();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void populateFrameWriterAndFlushIfNeeded() throws IOException
  {
    createFrameWriterIfNeeded();

    while (!cursor.isDone()) {
      if (!frameWriter.addSelection()) {
        if (frameWriter.getNumRows() > 0) {
          final long numRowsWritten = flushFrameWriter();

          if (runningCountForLimit != null) {
            runningCountForLimit.addAndGet(numRowsWritten);
          }

          return;
        } else {
          throw new FrameRowTooLargeException(currentAllocatorCapacity);
        }
      }

      cursor.advance();
      cursorOffset.increment();
      partitionBoostVirtualColumn.setValue(partitionBoostVirtualColumn.getValue() + 1);
    }
  }

  private void createFrameWriterIfNeeded()
  {
    if (frameWriter == null) {
      final FrameWriterFactory frameWriterFactory = getFrameWriterFactory();
      final ColumnSelectorFactory frameWriterColumnSelectorFactory =
          wrapColumnSelectorFactoryIfNeeded(frameWriterVirtualColumns.wrap(cursor.getColumnSelectorFactory()));
      frameWriter = frameWriterFactory.newFrameWriter(frameWriterColumnSelectorFactory);
      currentAllocatorCapacity = frameWriterFactory.allocatorCapacity();
    }
  }

  private long flushFrameWriter() throws IOException
  {
    if (frameWriter != null && frameWriter.getNumRows() > 0) {
      final Frame frame = Frame.wrap(frameWriter.toByteArray());
      Iterables.getOnlyElement(outputChannels()).write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
      frameWriter.close();
      frameWriter = null;
      return frame.numRows();
    } else {
      if (frameWriter != null) {
        frameWriter.close();
        frameWriter = null;
      }

      return 0;
    }
  }

  private long setNextCursor(
      final Cursor cursor,
      @Nullable final Closeable cursorCloser,
      final Segment segment
  ) throws IOException
  {
    final long rowsFlushed = flushFrameWriter();
    if (this.cursorCloser != null) {
      // Close here, don't add to the processor-level Closer, to avoid leaking CursorHolders. We may generate many
      // CursorHolders per instance of this processor, and we need to close them as we go, not all at the end.
      this.cursorCloser.close();
    }
    this.cursor = cursor;
    this.cursorCloser = cursorCloser;
    this.segment = segment;
    this.cursorOffset.reset();
    return rowsFlushed;
  }

  /**
   * Wraps the column selector factory if the underlying input to the processor is an external source
   */
  private ColumnSelectorFactory wrapColumnSelectorFactoryIfNeeded(final ColumnSelectorFactory baseColumnSelectorFactory)
  {
    if (segment instanceof ExternalSegment) {
      return new ExternalColumnSelectorFactory(
          baseColumnSelectorFactory,
          ((ExternalSegment) segment).externalInputSource(),
          ((ExternalSegment) segment).signature(),
          cursorOffset
      );
    }
    return baseColumnSelectorFactory;
  }
}
