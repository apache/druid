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

package org.apache.druid.frame.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.ResourceLimitExceededException;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Supplier;


public class ComposingWritableFrameChannelTest
{
  @Test
  public void testComposingWritableChannelSwitchesProperly() throws IOException
  {

    // This frame channel writes a single frame
    WritableFrameChannel writableFrameChannel1 = new LimitedWritableFrameChannel(2);
    WritableFrameChannel writableFrameChannel2 = new LimitedWritableFrameChannel(100);

    Supplier<ReadableFrameChannel> readableFrameChannelSupplier1 = () -> null;
    Supplier<ReadableFrameChannel> readableFrameChannelSupplier2 = () -> null;

    OutputChannel outputChannel1 = OutputChannel.pair(
        writableFrameChannel1,
        ArenaMemoryAllocator.createOnHeap(1),
        readableFrameChannelSupplier1,
        1
    );
    OutputChannel outputChannel2 = OutputChannel.pair(
        writableFrameChannel2,
        ArenaMemoryAllocator.createOnHeap(1),
        readableFrameChannelSupplier2,
        2
    );

    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();

    ComposingWritableFrameChannel composingWritableFrameChannel = new ComposingWritableFrameChannel(
        ImmutableList.of(
            () -> outputChannel1,
            () -> outputChannel2
        ),
        null,
        ImmutableList.of(
            () -> writableFrameChannel1,
            () -> writableFrameChannel2
        ),
        partitionToChannelMap
    );

    composingWritableFrameChannel.write(new FrameWithPartition(Mockito.mock(Frame.class), 1));
    composingWritableFrameChannel.write(new FrameWithPartition(Mockito.mock(Frame.class), 2));
    composingWritableFrameChannel.write(new FrameWithPartition(Mockito.mock(Frame.class), 3));

    // Assert the location of the channels where the frames have been written to
    Assert.assertEquals(ImmutableSet.of(0), partitionToChannelMap.get(1));
    Assert.assertEquals(ImmutableSet.of(0), partitionToChannelMap.get(2));
    Assert.assertEquals(ImmutableSet.of(1), partitionToChannelMap.get(3));


    // Test if the older channel has been converted to read only
    Assert.assertThrows(ISE.class, outputChannel1::getWritableChannel);
    composingWritableFrameChannel.close();

    Exception ise1 = Assert.assertThrows(IllegalStateException.class, () -> outputChannel1.getFrameMemoryAllocator());
    MatcherAssert.assertThat(
        ise1,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo(
            "Frame allocator is not available. The output channel might be marked as read-only, hence memory allocator is not required."))
    );


    Exception ise2 = Assert.assertThrows(IllegalStateException.class, () -> outputChannel2.getFrameMemoryAllocator());
    MatcherAssert.assertThat(
        ise2,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo(
            "Frame allocator is not available. The output channel might be marked as read-only, hence memory allocator is not required."))
    );

  }

  static class LimitedWritableFrameChannel implements WritableFrameChannel
  {
    private final int maxFrames;
    private int curFrame = 0;

    public LimitedWritableFrameChannel(int maxFrames)
    {
      this.maxFrames = maxFrames;
    }

    @Override
    public void write(FrameWithPartition frameWithPartition)
    {
      if (curFrame >= maxFrames) {
        throw new ResourceLimitExceededException("Cannot write more frames to the channel");
      }
      ++curFrame;
    }

    @Override
    public void write(Frame frame)
    {
    }

    @Override
    public void fail(@Nullable Throwable cause)
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    public boolean isClosed()
    {
      return false;
    }

    @Override
    public ListenableFuture<?> writabilityFuture()
    {
      return null;
    }
  }
}
