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

package org.apache.druid.msq.counters;

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.utils.JvmUtils;

import java.io.IOException;
import java.util.List;

/**
 * Wrapper around {@link FrameProcessor} that accumulates time taken into a {@link CpuCounter}.
 */
public class CpuTimeAccumulatingFrameProcessor<T> implements FrameProcessor<T>
{
  private final FrameProcessor<T> delegate;
  private final CpuCounter counter;

  public CpuTimeAccumulatingFrameProcessor(final FrameProcessor<T> delegate, final CpuCounter counter)
  {
    this.delegate = delegate;
    this.counter = counter;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return delegate.inputChannels();
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return delegate.outputChannels();
  }

  @Override
  public ReturnOrAwait<T> runIncrementally(IntSet readableInputs) throws InterruptedException, IOException
  {
    // Can't use counter.run, because it turns "throws InterruptedException, IOException" into "throws Exception".
    final long startCpu = JvmUtils.getCurrentThreadCpuTime();
    final long startWall = System.nanoTime();

    try {
      return delegate.runIncrementally(readableInputs);
    }
    finally {
      counter.accumulate(
          JvmUtils.getCurrentThreadCpuTime() - startCpu,
          System.nanoTime() - startWall
      );
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    final long startCpu = JvmUtils.getCurrentThreadCpuTime();
    final long startWall = System.nanoTime();

    try {
      delegate.cleanup();
    }
    finally {
      counter.accumulate(
          JvmUtils.getCurrentThreadCpuTime() - startCpu,
          System.nanoTime() - startWall
      );
    }
  }
}
