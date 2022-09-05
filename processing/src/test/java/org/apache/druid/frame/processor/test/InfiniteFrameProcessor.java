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

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Processor used by {@link org.apache.druid.frame.processor.FrameProcessorExecutorTest}.
 *
 * Writes the same frame over and over again to its output channel, without ever exiting.
 *
 * Returns the number of frames written.
 */
public class InfiniteFrameProcessor implements FrameProcessor<Long>
{
  private final Frame frame;
  private final WritableFrameChannel outChannel;
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final AtomicBoolean didCleanup = new AtomicBoolean(false);
  private final AtomicLong numFrames = new AtomicLong();

  public InfiniteFrameProcessor(
      final Frame frame,
      final WritableFrameChannel outChannel
  )
  {
    this.frame = frame;
    this.outChannel = outChannel;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outChannel);
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    outChannel.write(frame);
    numFrames.incrementAndGet();

    if (stop.get()) {
      return ReturnOrAwait.returnObject(numFrames.get());
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
    didCleanup.set(true);
  }

  public long getNumFrames()
  {
    return numFrames.get();
  }

  public void stop()
  {
    stop.set(true);
  }

  public boolean didCleanup()
  {
    return didCleanup.get();
  }
}
