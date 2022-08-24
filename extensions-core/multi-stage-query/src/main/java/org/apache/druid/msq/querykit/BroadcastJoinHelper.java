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

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.msq.indexing.error.BroadcastTablesTooLargeFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BroadcastJoinHelper
{
  private final Int2IntMap inputNumberToProcessorChannelMap;
  private final List<ReadableFrameChannel> channels;
  private final List<FrameReader> channelReaders;
  private final JoinableFactoryWrapper joinableFactory;
  private final List<List<Object[]>> channelData;
  private final IntSet sideChannelNumbers;
  private final long memoryReservedForBroadcastJoin;

  private long memoryUsed = 0L;

  /**
   * Create a new broadcast join helper. Currently this builds the tables in channelData. Using
   * {@link org.apache.druid.query.groupby.epinephelinae.collection.MemoryOpenHashTable} should be more appropriate for
   * this purpose
   *
   * @param inputNumberToProcessorChannelMap map of input slice number -> channel position in the "channels" list
   * @param channels                         list of input channels
   * @param channelReaders                   list of input channel readers; corresponds one-to-one with "channels"
   * @param joinableFactory                  joinable factory for this server
   * @param memoryReservedForBroadcastJoin   total bytes of frames we are permitted to use; derived from
   *                                         {@link org.apache.druid.msq.exec.WorkerMemoryParameters#broadcastJoinMemory}
   */
  public BroadcastJoinHelper(
      final Int2IntMap inputNumberToProcessorChannelMap,
      final List<ReadableFrameChannel> channels,
      final List<FrameReader> channelReaders,
      final JoinableFactoryWrapper joinableFactory,
      final long memoryReservedForBroadcastJoin
  )
  {
    this.inputNumberToProcessorChannelMap = inputNumberToProcessorChannelMap;
    this.channels = channels;
    this.channelReaders = channelReaders;
    this.joinableFactory = joinableFactory;
    this.channelData = new ArrayList<>();
    this.sideChannelNumbers = new IntOpenHashSet();
    this.sideChannelNumbers.addAll(inputNumberToProcessorChannelMap.values());
    this.memoryReservedForBroadcastJoin = memoryReservedForBroadcastJoin;

    for (int i = 0; i < channels.size(); i++) {
      if (sideChannelNumbers.contains(i)) {
        channelData.add(new ArrayList<>());
        sideChannelNumbers.add(i);
      } else {
        channelData.add(null);
      }
    }
  }

  /**
   * Reads up to one frame from each readable side channel, and uses them to incrementally build up joinable
   * broadcast tables.
   *
   * @param readableInputs all readable input channel numbers, including non-side-channels
   *
   * @return whether side channels have been fully read
   */
  public boolean buildBroadcastTablesIncrementally(final IntSet readableInputs)
  {
    final IntIterator inputChannelIterator = readableInputs.iterator();

    while (inputChannelIterator.hasNext()) {
      final int channelNumber = inputChannelIterator.nextInt();
      if (sideChannelNumbers.contains(channelNumber) && channels.get(channelNumber).canRead()) {
        final Frame frame = channels.get(channelNumber).read();

        memoryUsed += frame.numBytes();

        if (memoryUsed > memoryReservedForBroadcastJoin) {
          throw new MSQException(new BroadcastTablesTooLargeFault(memoryReservedForBroadcastJoin));
        }

        addFrame(channelNumber, frame);
      }
    }

    for (int channelNumber : sideChannelNumbers) {
      if (!channels.get(channelNumber).isFinished()) {
        return false;
      }
    }

    return true;
  }

  public IntSet getSideChannelNumbers()
  {
    return sideChannelNumbers;
  }

  public Function<SegmentReference, SegmentReference> makeSegmentMapFn(final Query<?> query)
  {
    final DataSource dataSourceWithInlinedChannelData = inlineChannelData(query.getDataSource());
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(dataSourceWithInlinedChannelData);

    return joinableFactory.createSegmentMapFn(
        analysis.getJoinBaseTableFilter().map(Filters::toFilter).orElse(null),
        analysis.getPreJoinableClauses(),
        new AtomicLong(),
        analysis.getBaseQuery().orElse(query)
    );
  }

  @VisibleForTesting
  DataSource inlineChannelData(final DataSource originalDataSource)
  {
    if (originalDataSource instanceof InputNumberDataSource) {
      final int inputNumber = ((InputNumberDataSource) originalDataSource).getInputNumber();
      if (inputNumberToProcessorChannelMap.containsKey(inputNumber)) {
        final int channelNumber = inputNumberToProcessorChannelMap.get(inputNumber);

        if (sideChannelNumbers.contains(channelNumber)) {
          return InlineDataSource.fromIterable(
              channelData.get(channelNumber),
              channelReaders.get(channelNumber).signature()
          );
        } else {
          return originalDataSource;
        }
      } else {
        return originalDataSource;
      }
    } else {
      final List<DataSource> newChildren = new ArrayList<>(originalDataSource.getChildren().size());

      for (final DataSource child : originalDataSource.getChildren()) {
        newChildren.add(inlineChannelData(child));
      }

      return originalDataSource.withChildren(newChildren);
    }
  }

  private void addFrame(final int channelNumber, final Frame frame)
  {
    final List<Object[]> data = channelData.get(channelNumber);
    final FrameReader frameReader = channelReaders.get(channelNumber);
    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);

    final List<ColumnValueSelector> selectors =
        frameReader.signature().getColumnNames().stream().map(
            columnName ->
                cursor.getColumnSelectorFactory().makeColumnValueSelector(columnName)
        ).collect(Collectors.toList());

    while (!cursor.isDone()) {
      final Object[] row = new Object[selectors.size()];
      for (int i = 0; i < row.length; i++) {
        row[i] = selectors.get(i).getObject();
      }
      data.add(row);
      cursor.advance();
    }
  }
}
