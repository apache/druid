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
import org.apache.druid.frame.processor.ReturnOrAwait;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Frame processor that writes from and reads from at most a single channel, and runs {@link #runIncrementally(IntSet)}
 * at most once. When {@link #runIncrementally(IntSet)} is called, the class calls {@link #doSimpleWork()} and returns
 * the value.
 */
public abstract class SingleChannelFrameProcessor<T> implements FrameProcessor<T>
{
  @Nullable
  private final ReadableFrameChannel readableFrameChannel;
  @Nullable
  private final WritableFrameChannel writableFrameChannel;

  public SingleChannelFrameProcessor(
      @Nullable ReadableFrameChannel readableFrameChannel,
      @Nullable WritableFrameChannel writableFrameChannel
  )
  {
    this.readableFrameChannel = readableFrameChannel;
    this.writableFrameChannel = writableFrameChannel;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    if (readableFrameChannel == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(readableFrameChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    if (writableFrameChannel == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(writableFrameChannel);
  }

  @Override
  public ReturnOrAwait<T> runIncrementally(IntSet readableInputs) throws IOException
  {
    return ReturnOrAwait.returnObject(doSimpleWork());
  }

  public abstract T doSimpleWork() throws IOException;

  @Override
  public void cleanup()
  {
  }
}
