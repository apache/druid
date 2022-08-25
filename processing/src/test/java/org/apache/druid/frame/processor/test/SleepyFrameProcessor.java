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
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processor used by {@link org.apache.druid.frame.processor.FrameProcessorExecutorTest}.
 *
 * Sleeps forever in {@link #runIncrementally}.
 *
 * Returns the number of frames written.
 */
public class SleepyFrameProcessor implements FrameProcessor<Long>
{
  private final CountDownLatch didRun = new CountDownLatch(1);
  private final AtomicBoolean didGetInterrupt = new AtomicBoolean(false);
  private final CountDownLatch cleanupLatch = new CountDownLatch(1);
  private final AtomicBoolean didCleanup = new AtomicBoolean(false);

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  @SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
  public ReturnOrAwait<Long> runIncrementally(IntSet readableInputs) throws InterruptedException
  {
    didRun.countDown();

    while (true) {
      try {
        Thread.sleep(Duration.standardDays(1).getMillis());
      }
      catch (InterruptedException e) {
        didGetInterrupt.set(true);
        throw e;
      }
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
    didCleanup.set(true);
    cleanupLatch.countDown();
  }

  public void awaitRun() throws InterruptedException
  {
    didRun.await();
  }

  public boolean didGetInterrupt()
  {
    return didGetInterrupt.get();
  }

  public boolean didCleanup()
  {
    return didCleanup.get();
  }

  public void awaitCleanup() throws InterruptedException
  {
    cleanupLatch.await();
  }
}
