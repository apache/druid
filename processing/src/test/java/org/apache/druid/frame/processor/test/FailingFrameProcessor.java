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
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Processor used by {@link org.apache.druid.frame.processor.FrameProcessorExecutorTest}.
 *
 * Copies some number of frames from input to output, then fails. If the input channel does not have this
 * many frames, fails when the input channel is finished.
 */
public class FailingFrameProcessor implements FrameProcessor<Long>
{
  private final ReadableFrameChannel inChannel;
  private final WritableFrameChannel outChannel;
  private final int numFramesBeforeFailure;

  private int numFramesSoFar = 0;

  public FailingFrameProcessor(
      final ReadableFrameChannel inChannel,
      final WritableFrameChannel outChannel,
      final int numFramesBeforeFailure
  )
  {
    this.inChannel = inChannel;
    this.outChannel = outChannel;
    this.numFramesBeforeFailure = numFramesBeforeFailure;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outChannel);
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (readableInputs.contains(0)) {
      if (inChannel.isFinished()) {
        throw new RuntimeException("failure!");
      }

      if (numFramesSoFar >= numFramesBeforeFailure) {
        throw new RuntimeException("failure!");
      }

      outChannel.write(inChannel.read());
      numFramesSoFar++;

      if (numFramesSoFar >= numFramesBeforeFailure) {
        throw new RuntimeException("failure!");
      }
    }

    return ReturnOrAwait.awaitAll(inputChannels().size());
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }
}
