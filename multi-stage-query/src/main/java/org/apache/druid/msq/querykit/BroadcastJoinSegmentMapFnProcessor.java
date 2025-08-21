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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.error.BroadcastTablesTooLargeFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.Query;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Processor that reads broadcast join data and creates a segment mapping function. The resulting segment
 * mapping function embeds the joinable data within itself, and can be applied anywhere that would otherwise have used
 * {@link org.apache.druid.query.JoinDataSource#createSegmentMapFunction(Query)}.
 *
 * @see SimpleSegmentMapFnProcessor processor that creates a segment mapping function when there is no broadcast input
 */
public class BroadcastJoinSegmentMapFnProcessor implements FrameProcessor<SegmentMapFunction>
{
  private final Query<?> query;
  private final PolicyEnforcer policyEnforcer;
  private final Int2IntMap inputNumberToProcessorChannelMap;
  private final List<ReadableFrameChannel> channels;
  private final List<FrameReader> channelReaders;
  private final List<List<Object[]>> channelData;
  private final IntSet sideChannelNumbers;
  private final long memoryReservedForBroadcastJoin;

  private long memoryUsed = 0L;

  /**
   * Create a new broadcast join data reader. Currently, this builds the tables as Object arrays
   * in {@link #channelData}. Using {@link org.apache.druid.query.groupby.epinephelinae.collection.MemoryOpenHashTable}
   * would likely be an improvement.
   *
   * @param query                            original query
   * @param inputNumberToProcessorChannelMap map of input slice number -> channel position in the "channels" list
   * @param channels                         list of input channels
   * @param channelReaders                   list of input channel readers; corresponds one-to-one with "channels"
   * @param memoryReservedForBroadcastJoin   total bytes of frames we are permitted to use; derived from
   *                                         {@link WorkerMemoryParameters#getBroadcastBufferMemory()}
   */
  public BroadcastJoinSegmentMapFnProcessor(
      final Query<?> query,
      final PolicyEnforcer policyEnforcer,
      final Int2IntMap inputNumberToProcessorChannelMap,
      final List<ReadableFrameChannel> channels,
      final List<FrameReader> channelReaders,
      final long memoryReservedForBroadcastJoin
  )
  {
    this.query = query;
    this.policyEnforcer = policyEnforcer;
    this.inputNumberToProcessorChannelMap = inputNumberToProcessorChannelMap;
    this.channels = channels;
    this.channelReaders = channelReaders;
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
   * Helper that enables implementations of {@link BaseLeafStageProcessor} to set up an instance of this class.
   */
  public static BroadcastJoinSegmentMapFnProcessor create(
      final Query<?> query,
      final PolicyEnforcer policyEnforcer,
      final Int2ObjectMap<ReadableInput> sideChannels,
      final long memoryReservedForBroadcastJoin
  )
  {
    final Int2IntMap inputNumberToProcessorChannelMap = new Int2IntOpenHashMap();
    final List<ReadableFrameChannel> inputChannels = new ArrayList<>();
    final List<FrameReader> channelReaders = new ArrayList<>();

    for (Int2ObjectMap.Entry<ReadableInput> sideChannelEntry : sideChannels.int2ObjectEntrySet()) {
      final int inputNumber = sideChannelEntry.getIntKey();
      inputNumberToProcessorChannelMap.put(inputNumber, inputChannels.size());
      inputChannels.add(sideChannelEntry.getValue().getChannel());
      channelReaders.add(sideChannelEntry.getValue().getChannelFrameReader());
    }

    return new BroadcastJoinSegmentMapFnProcessor(
        query,
        policyEnforcer,
        inputNumberToProcessorChannelMap,
        inputChannels,
        channelReaders,
        memoryReservedForBroadcastJoin
    );
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return channels;
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public ReturnOrAwait<SegmentMapFunction> runIncrementally(IntSet readableInputs)
  {
    if (buildBroadcastTablesIncrementally(readableInputs)) {
      return ReturnOrAwait.returnObject(createSegmentMapFunction());
    } else {
      return ReturnOrAwait.awaitAny(sideChannelNumbers);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
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

  private SegmentMapFunction createSegmentMapFunction()
  {
    DataSource transformed = inlineChannelData(query.getDataSource());
    return ExecutionVertex.of(query.withDataSource(transformed)).createSegmentMapFunction(policyEnforcer);
  }

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

  /**
   * Reads up to one frame from each readable side channel, and uses them to incrementally build up joinable
   * broadcast tables.
   *
   * @param readableInputs all readable input channel numbers, including non-side-channels
   * @return whether side channels have been fully read
   */
  boolean buildBroadcastTablesIncrementally(final IntSet readableInputs)
  {
    final IntIterator inputChannelIterator = readableInputs.iterator();

    while (inputChannelIterator.hasNext()) {
      final int channelNumber = inputChannelIterator.nextInt();
      if (sideChannelNumbers.contains(channelNumber) && channels.get(channelNumber).canRead()) {
        final Frame frame = channels.get(channelNumber).read();

        memoryUsed += frame.numBytes();

        if (memoryUsed > memoryReservedForBroadcastJoin) {
          throw new MSQException(
              new BroadcastTablesTooLargeFault(
                  memoryReservedForBroadcastJoin,
                  Optional.ofNullable(query)
                          .map(q -> q.context().getString(PlannerContext.CTX_SQL_JOIN_ALGORITHM))
                          .map(JoinAlgorithm::fromString)
                          .orElse(null)
              )
          );
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

  IntSet getSideChannelNumbers()
  {
    return sideChannelNumbers;
  }
}
