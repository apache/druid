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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.java.util.common.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Processor that reads up to "maxFrames" frames from some input channels and combines them into a batch. There may be
 * frames left over in the channels when the worker is done.
 *
 * Returns the batch and the set of channels that have more left to read.
 *
 * This processor does not close its input channels. The caller should do that after all input channels are finished.
 */
public class FrameChannelBatcher implements FrameProcessor<Pair<List<Frame>, IntSet>>
{
  private final List<ReadableFrameChannel> channels;
  private final int maxFrames;

  private final IntSet channelsToRead;
  private List<Frame> out = new ArrayList<>();

  public FrameChannelBatcher(
      final List<ReadableFrameChannel> channels,
      final int maxFrames
  )
  {
    this.channels = channels;
    this.maxFrames = maxFrames;
    this.channelsToRead = new IntOpenHashSet();

    for (int i = 0; i < channels.size(); i++) {
      if (!channels.get(i).isFinished()) {
        channelsToRead.add(i);
      }
    }
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
  public ReturnOrAwait<Pair<List<Frame>, IntSet>> runIncrementally(final IntSet readableInputs)
  {
    if (channelsToRead.isEmpty()) {
      return ReturnOrAwait.returnObject(Pair.of(flush(), IntSets.emptySet()));
    }

    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAny(channelsToRead);
    }

    // Random first channel to avoid biasing towards low-numbered channels.
    final int firstChannel = ThreadLocalRandom.current().nextInt(channels.size());

    // Modular iteration.
    for (int i = 0; i < channels.size() && out.size() < maxFrames; i++) {
      final int channelNumber = (firstChannel + i) % channels.size();

      if (readableInputs.contains(channelNumber) && channelsToRead.contains(channelNumber)) {
        final ReadableFrameChannel channel = channels.get(channelNumber);
        if (channel.canRead()) {
          out.add(channel.read());
        } else if (channel.isFinished()) {
          channelsToRead.remove(channelNumber);
        }
      }
    }

    if (out.size() >= maxFrames) {
      return ReturnOrAwait.returnObject(Pair.of(flush(), channelsToRead));
    } else {
      return ReturnOrAwait.awaitAny(channelsToRead);
    }
  }

  @Override
  public void cleanup()
  {
    // Don't close the input channels, because this worker will not necessarily read through the entire channels.
    // The channels should be closed by the caller.
  }

  private List<Frame> flush()
  {
    final List<Frame> tmp = out;
    out = null;
    return tmp;
  }
}
