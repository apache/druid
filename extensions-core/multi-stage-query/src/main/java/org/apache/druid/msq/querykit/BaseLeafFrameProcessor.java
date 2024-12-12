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

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public abstract class BaseLeafFrameProcessor implements FrameProcessor<Object>
{
  private final ReadableInput baseInput;
  private final ResourceHolder<WritableFrameChannel> outputChannelHolder;
  private final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder;
  private final Function<SegmentReference, SegmentReference> segmentMapFn;

  protected BaseLeafFrameProcessor(
      final ReadableInput baseInput,
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final ResourceHolder<WritableFrameChannel> outputChannelHolder,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder
  )
  {
    this.baseInput = baseInput;
    this.outputChannelHolder = outputChannelHolder;
    this.frameWriterFactoryHolder = frameWriterFactoryHolder;
    this.segmentMapFn = segmentMapFn;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    if (baseInput.hasSegment() || baseInput.hasDataServerQuery()) {
      return Collections.emptyList();
    } else {
      return Collections.singletonList(baseInput.getChannel());
    }
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannelHolder.get());
  }

  @Override
  public ReturnOrAwait<Object> runIncrementally(final IntSet readableInputs) throws IOException
  {
    //noinspection rawtypes
    final ReturnOrAwait retVal;

    if (baseInput.hasSegment()) {
      retVal = runWithSegment(baseInput.getSegment());
    } else if (baseInput.hasDataServerQuery()) {
      retVal = runWithDataServerQuery(baseInput.getDataServerQuery());
    } else {
      retVal = runWithInputChannel(baseInput.getChannel(), baseInput.getChannelFrameReader());
    }

    //noinspection rawtype,unchecked
    return retVal;
  }

  @Override
  public void cleanup() throws IOException
  {
    // Don't close the output channel, because multiple workers write to the same channel.
    // The channel should be closed by the caller.
    FrameProcessors.closeAll(inputChannels(), Collections.emptyList(), outputChannelHolder, frameWriterFactoryHolder);
  }

  protected FrameWriterFactory getFrameWriterFactory()
  {
    return frameWriterFactoryHolder.get();
  }

  /**
   * Runs the leaf processor using a segment described by the {@link SegmentWithDescriptor} as the input. This may result
   * in calls to fetch the segment from an external source.
   */
  protected abstract ReturnOrAwait<Unit> runWithSegment(SegmentWithDescriptor segment) throws IOException;

  /**
   * Runs the leaf processor using the results from a data server as the input. The query and data server details are
   * described by {@link DataServerQueryHandler}.
   */
  protected abstract ReturnOrAwait<SegmentsInputSlice> runWithDataServerQuery(DataServerQueryHandler dataServerQueryHandler) throws IOException;

  protected abstract ReturnOrAwait<Unit> runWithInputChannel(
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
}
