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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;
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
import org.apache.druid.frame.util.SettableLongVirtualColumn;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.querykit.BaseLeafFrameProcessor;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link FrameProcessor} that reads one {@link Frame} at a time from a particular segment, writes them
 * to a {@link WritableFrameChannel}, and returns the number of rows output.
 */
public class ScanQueryFrameProcessor extends BaseLeafFrameProcessor
{
  private final ScanQuery query;
  private final AtomicLong runningCountForLimit;
  private final SettableLongVirtualColumn partitionBoostVirtualColumn;
  private final VirtualColumns frameWriterVirtualColumns;
  private final Closer closer = Closer.create();

  private long rowsOutput = 0;
  private Cursor cursor;
  private FrameWriter frameWriter;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed

  public ScanQueryFrameProcessor(
      final ScanQuery query,
      final ReadableInput baseInput,
      final Int2ObjectMap<ReadableInput> sideChannels,
      final JoinableFactoryWrapper joinableFactory,
      final ResourceHolder<WritableFrameChannel> outputChannel,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      @Nullable final AtomicLong runningCountForLimit,
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
    this.runningCountForLimit = runningCountForLimit;
    this.partitionBoostVirtualColumn = new SettableLongVirtualColumn(QueryKitUtils.PARTITION_BOOST_COLUMN);

    final List<VirtualColumn> frameWriterVirtualColumns = new ArrayList<>();
    frameWriterVirtualColumns.add(partitionBoostVirtualColumn);

    final VirtualColumn segmentGranularityVirtualColumn = QueryKitUtils.makeSegmentGranularityVirtualColumn(query);

    if (segmentGranularityVirtualColumn != null) {
      frameWriterVirtualColumns.add(segmentGranularityVirtualColumn);
    }

    this.frameWriterVirtualColumns = VirtualColumns.create(frameWriterVirtualColumns);
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    final boolean legacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");

    if (legacy) {
      throw new ISE("Cannot use this engine in legacy mode");
    }

    if (runningCountForLimit != null
        && runningCountForLimit.get() > query.getScanRowsOffset() + query.getScanRowsLimit()) {
      return ReturnOrAwait.returnObject(rowsOutput);
    }

    return super.runIncrementally(readableInputs);
  }

  @Override
  public void cleanup() throws IOException
  {
    closer.register(frameWriter);
    closer.register(super::cleanup);
    closer.close();
  }

  @Override
  protected ReturnOrAwait<Long> runWithSegment(final SegmentWithDescriptor segment) throws IOException
  {
    if (cursor == null) {
      closer.register(segment);

      final Yielder<Cursor> cursorYielder = Yielders.each(
          makeCursors(
              query.withQuerySegmentSpec(new SpecificSegmentSpec(segment.getDescriptor())),
              mapSegment(segment.getOrLoadSegment()).asStorageAdapter()
          )
      );

      if (cursorYielder.isDone()) {
        // No cursors!
        cursorYielder.close();
        return ReturnOrAwait.returnObject(rowsOutput);
      } else {
        final long rowsFlushed = setNextCursor(cursorYielder.get());
        assert rowsFlushed == 0; // There's only ever one cursor when running with a segment
        closer.register(cursorYielder);
      }
    }

    populateFrameWriterAndFlushIfNeeded();

    if (cursor.isDone()) {
      flushFrameWriter();
    }

    if (cursor.isDone() && (frameWriter == null || frameWriter.getNumRows() == 0)) {
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
    if (cursor == null || cursor.isDone()) {
      if (inputChannel.canRead()) {
        final Frame frame = inputChannel.read();
        final FrameSegment frameSegment = new FrameSegment(frame, inputFrameReader, SegmentId.dummy("scan"));

        final long rowsFlushed = setNextCursor(
            Iterables.getOnlyElement(
                makeCursors(
                    query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY)),
                    mapSegment(frameSegment).asStorageAdapter()
                ).toList()
            )
        );

        if (rowsFlushed > 0) {
          return ReturnOrAwait.runAgain();
        }
      } else if (inputChannel.isFinished()) {
        flushFrameWriter();
        return ReturnOrAwait.returnObject(rowsOutput);
      } else {
        return ReturnOrAwait.awaitAll(inputChannels().size());
      }
    }

    // Cursor has some more data in it.
    populateFrameWriterAndFlushIfNeeded();

    if (cursor.isDone()) {
      return ReturnOrAwait.awaitAll(inputChannels().size());
    } else {
      return ReturnOrAwait.runAgain();
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
      partitionBoostVirtualColumn.setValue(partitionBoostVirtualColumn.getValue() + 1);
    }
  }

  private void createFrameWriterIfNeeded()
  {
    if (frameWriter == null) {
      final FrameWriterFactory frameWriterFactory = getFrameWriterFactory();
      final ColumnSelectorFactory frameWriterColumnSelectorFactory =
          frameWriterVirtualColumns.wrap(cursor.getColumnSelectorFactory());
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
      rowsOutput += frame.numRows();
      return frame.numRows();
    } else {
      if (frameWriter != null) {
        frameWriter.close();
        frameWriter = null;
      }

      return 0;
    }
  }

  private long setNextCursor(final Cursor cursor) throws IOException
  {
    final long rowsFlushed = flushFrameWriter();
    this.cursor = cursor;
    return rowsFlushed;
  }

  private static Sequence<Cursor> makeCursors(final ScanQuery query, final StorageAdapter adapter)
  {
    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));

    return adapter.makeCursors(
        filter,
        intervals.get(0),
        query.getVirtualColumns(),
        Granularities.ALL,
        ScanQuery.Order.DESCENDING.equals(query.getTimeOrder()),
        null
    );
  }
}
