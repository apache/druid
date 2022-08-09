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

package org.apache.druid.frame.processor;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Processor that merges frames from inputChannels into a single outputChannel. No sorting is done: input frames are
 * simply written to the output channel as they come in.
 *
 * For sorted output, use {@link FrameChannelMerger} instead.
 */
public class FrameChannelMuxer implements FrameProcessor<Long>
{
  private final List<ReadableFrameChannel> inputChannels;
  private final WritableFrameChannel outputChannel;

  private final IntSet remainingChannels = new IntOpenHashSet();
  private long rowsRead = 0L;

  public FrameChannelMuxer(
      final List<ReadableFrameChannel> inputChannels,
      final WritableFrameChannel outputChannel
  )
  {
    this.inputChannels = inputChannels;
    this.outputChannel = outputChannel;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return inputChannels;
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (remainingChannels.isEmpty()) {
      // First run.
      for (int i = 0; i < inputChannels.size(); i++) {
        final ReadableFrameChannel channel = inputChannels.get(i);
        if (!channel.isFinished()) {
          remainingChannels.add(i);
        }
      }
    }

    if (!readableInputs.isEmpty()) {
      // Avoid biasing towards lower-numbered channels.
      final int channelIdx = ThreadLocalRandom.current().nextInt(readableInputs.size());

      int i = 0;
      for (IntIterator iterator = readableInputs.iterator(); iterator.hasNext(); i++) {
        final int channelNumber = iterator.nextInt();
        final ReadableFrameChannel channel = inputChannels.get(channelNumber);

        if (channel.isFinished()) {
          remainingChannels.remove(channelNumber);
        } else if (i == channelIdx) {
          final Frame frame = channel.read();
          outputChannel.write(frame);
          rowsRead += frame.numRows();
        }
      }
    }

    if (remainingChannels.isEmpty()) {
      return ReturnOrAwait.returnObject(rowsRead);
    } else {
      return ReturnOrAwait.awaitAny(remainingChannels);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }
}
