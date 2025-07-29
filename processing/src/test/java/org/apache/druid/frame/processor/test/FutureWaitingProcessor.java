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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Processor that waits for two futures using {@link ReturnOrAwait#awaitAllFutures(Collection)}.
 */
public class FutureWaitingProcessor implements FrameProcessor<List<String>>
{
  private final ListenableFuture<String> future1;
  private final ListenableFuture<String> future2;

  public FutureWaitingProcessor(ListenableFuture<String> future1, ListenableFuture<String> future2)
  {
    this.future1 = future1;
    this.future2 = future2;
  }

  private int runCount = 0;
  private boolean cleanedUp;
  private final List<String> results = new ArrayList<>();

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
  public ReturnOrAwait<List<String>> runIncrementally(IntSet readableInputs)
  {
    runCount++;

    if (runCount == 1) {
      // First run: wait for both futures
      return ReturnOrAwait.awaitAllFutures(ImmutableList.of(future1, future2));
    } else if (runCount == 2) {
      // Second run: futures should be complete, collect results
      Assert.assertTrue("future1 should be done", future1.isDone());
      Assert.assertTrue("future2 should be done", future2.isDone());

      try {
        results.add(future1.get());
        results.add(future2.get());
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return ReturnOrAwait.returnObject(results);
    } else {
      throw new ISE("Should not run more than twice");
    }
  }

  @Override
  public void cleanup()
  {
    cleanedUp = true;
  }

  public boolean isCleanedUp()
  {
    return cleanedUp;
  }
}
