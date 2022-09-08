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
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public abstract class BaseLeafFrameProcessor implements FrameProcessor<Long>
{
  private final Query<?> query;
  private final ReadableInput baseInput;
  private final List<ReadableFrameChannel> inputChannels;
  private final ResourceHolder<WritableFrameChannel> outputChannel;
  private final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder;
  private final BroadcastJoinHelper broadcastJoinHelper;

  private Function<SegmentReference, SegmentReference> segmentMapFn;

  protected BaseLeafFrameProcessor(
      final Query<?> query,
      final ReadableInput baseInput,
      final Int2ObjectMap<ReadableInput> sideChannels,
      final JoinableFactoryWrapper joinableFactory,
      final ResourceHolder<WritableFrameChannel> outputChannel,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      final long memoryReservedForBroadcastJoin
  )
  {
    this.query = query;
    this.baseInput = baseInput;
    this.outputChannel = outputChannel;
    this.frameWriterFactoryHolder = frameWriterFactoryHolder;

    final Pair<List<ReadableFrameChannel>, BroadcastJoinHelper> inputChannelsAndBroadcastJoinHelper =
        makeInputChannelsAndBroadcastJoinHelper(
            query.getDataSource(),
            baseInput,
            sideChannels,
            joinableFactory,
            memoryReservedForBroadcastJoin
        );

    this.inputChannels = inputChannelsAndBroadcastJoinHelper.lhs;
    this.broadcastJoinHelper = inputChannelsAndBroadcastJoinHelper.rhs;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return inputChannels;
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel.get());
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (!initializeSegmentMapFn(readableInputs)) {
      return ReturnOrAwait.awaitAll(broadcastJoinHelper.getSideChannelNumbers());
    } else if (readableInputs.size() != inputChannels.size()) {
      return ReturnOrAwait.awaitAll(inputChannels.size());
    } else if (baseInput.hasSegment()) {
      return runWithSegment(baseInput.getSegment());
    } else {
      assert baseInput.hasChannel();
      return runWithInputChannel(baseInput.getChannel(), baseInput.getChannelFrameReader());
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    // Don't close the output channel, because multiple workers write to the same channel.
    // The channel should be closed by the caller.
    FrameProcessors.closeAll(inputChannels(), Collections.emptyList(), outputChannel, frameWriterFactoryHolder);
  }

  protected FrameWriterFactory getFrameWriterFactory()
  {
    return frameWriterFactoryHolder.get();
  }

  protected abstract ReturnOrAwait<Long> runWithSegment(SegmentWithDescriptor segment) throws IOException;

  protected abstract ReturnOrAwait<Long> runWithInputChannel(
      ReadableFrameChannel inputChannel,
      FrameReader inputFrameReader
  ) throws IOException;

  /**
   * Helper intended to be used by subclasses. Applies {@link #segmentMapFn}, which applies broadcast joins
   * if applicable to this query.
   */
  protected SegmentReference mapSegment(final Segment segment)
  {
    return segmentMapFn.apply(ReferenceCountingSegment.wrapRootGenerationSegment(segment));
  }

  private boolean initializeSegmentMapFn(final IntSet readableInputs)
  {
    if (segmentMapFn != null) {
      return true;
    } else if (broadcastJoinHelper == null) {
      segmentMapFn = Function.identity();
      return true;
    } else {
      final boolean retVal = broadcastJoinHelper.buildBroadcastTablesIncrementally(readableInputs);

      if (retVal) {
        segmentMapFn = broadcastJoinHelper.makeSegmentMapFn(query);
      }

      return retVal;
    }
  }

  /**
   * Helper that enables implementations of {@link BaseLeafFrameProcessorFactory} to set up their primary and side channels.
   */
  private static Pair<List<ReadableFrameChannel>, BroadcastJoinHelper> makeInputChannelsAndBroadcastJoinHelper(
      final DataSource dataSource,
      final ReadableInput baseInput,
      final Int2ObjectMap<ReadableInput> sideChannels,
      final JoinableFactoryWrapper joinableFactory,
      final long memoryReservedForBroadcastJoin
  )
  {
    if (!(dataSource instanceof JoinDataSource) && !sideChannels.isEmpty()) {
      throw new ISE("Did not expect side channels for dataSource [%s]", dataSource);
    }

    final List<ReadableFrameChannel> inputChannels = new ArrayList<>();
    final BroadcastJoinHelper broadcastJoinHelper;

    if (baseInput.hasChannel()) {
      inputChannels.add(baseInput.getChannel());
    }

    if (dataSource instanceof JoinDataSource) {
      final Int2IntMap inputNumberToProcessorChannelMap = new Int2IntOpenHashMap();
      final List<FrameReader> channelReaders = new ArrayList<>();

      if (baseInput.hasChannel()) {
        // BroadcastJoinHelper doesn't need to read the base channel, so stub in a null reader.
        channelReaders.add(null);
      }

      for (Int2ObjectMap.Entry<ReadableInput> sideChannelEntry : sideChannels.int2ObjectEntrySet()) {
        final int inputNumber = sideChannelEntry.getIntKey();
        inputNumberToProcessorChannelMap.put(inputNumber, inputChannels.size());
        inputChannels.add(sideChannelEntry.getValue().getChannel());
        channelReaders.add(sideChannelEntry.getValue().getChannelFrameReader());
      }

      broadcastJoinHelper = new BroadcastJoinHelper(
          inputNumberToProcessorChannelMap,
          inputChannels,
          channelReaders,
          joinableFactory,
          memoryReservedForBroadcastJoin
      );
    } else {
      broadcastJoinHelper = null;
    }

    return Pair.of(inputChannels, broadcastJoinHelper);
  }
}
