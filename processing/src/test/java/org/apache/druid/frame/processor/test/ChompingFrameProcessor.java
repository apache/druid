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

package org.apache.druid.frame.processor.test;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processor used by {@link org.apache.druid.frame.processor.FrameProcessorExecutorTest}.
 *
 * Throws away all input frames; never writes any output.
 *
 * Returns the number of frames read.
 */
public class ChompingFrameProcessor implements FrameProcessor<Long>
{
  private final List<ReadableFrameChannel> channels;
  private final IntSet awaitSet;
  private final CountDownLatch didReadFrame = new CountDownLatch(1);
  private final AtomicBoolean didCleanup = new AtomicBoolean(false);
  private long numFrames = 0L;

  public ChompingFrameProcessor(List<ReadableFrameChannel> channels)
  {
    this.channels = channels;
    this.awaitSet = new IntOpenHashSet(IntSets.fromTo(0, channels.size()));
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
  public ReturnOrAwait<Long> runIncrementally(IntSet readableInputs)
  {
    for (final int channelNumber : readableInputs) {
      final ReadableFrameChannel channel = channels.get(channelNumber);

      if (channel.isFinished()) {
        awaitSet.remove(channelNumber);
      } else {
        // Call valueOrThrow() for side effects: we want to throw any exceptions that come in on the channel.
        channel.read();
        didReadFrame.countDown();
        numFrames++;
      }
    }

    if (awaitSet.isEmpty()) {
      return ReturnOrAwait.returnObject(numFrames);
    } else {
      return ReturnOrAwait.awaitAny(awaitSet);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
    didCleanup.set(true);
  }

  public void awaitRead() throws InterruptedException
  {
    didReadFrame.await();
  }

  public boolean didCleanup()
  {
    return didCleanup.get();
  }
}
