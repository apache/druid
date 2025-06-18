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

import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

public class OutputChannelTest
{
  @Test
  public void test_nil()
  {
    final OutputChannel channel = OutputChannel.nil(1);

    Assert.assertEquals(1, channel.getPartitionNumber());
    Assert.assertTrue(channel.getReadableChannel().isFinished());

    // No writable channel: cannot call getWritableChannel.
    final IllegalStateException e1 = Assert.assertThrows(IllegalStateException.class, channel::getWritableChannel);
    MatcherAssert.assertThat(
        e1,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo(
            "Writable channel is not available. The output channel might be marked as read-only, hence no writes are allowed."))
    );

    // No writable channel: cannot call getFrameMemoryAllocator.
    final IllegalStateException e2 = Assert.assertThrows(IllegalStateException.class, channel::getFrameMemoryAllocator);
    MatcherAssert.assertThat(
        e2,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo(
            "Frame allocator is not available. The output channel might be marked as read-only, hence memory allocator is not required."))
    );

    // Mapping the writable channel of a nil channel has no effect, because there is no writable channel.
    Assert.assertSame(
        channel,
        channel.mapWritableChannel(c -> BlockingQueueFrameChannel.minimal().writable())
    );
  }

  @Test
  public void test_pair()
  {
    final BlockingQueueFrameChannel theChannel = BlockingQueueFrameChannel.minimal();
    final HeapMemoryAllocator allocator = HeapMemoryAllocator.unlimited();
    final OutputChannel channel = OutputChannel.pair(
        theChannel.writable(),
        allocator,
        theChannel::readable,
        1
    );

    Assert.assertEquals(1, channel.getPartitionNumber());
    Assert.assertSame(theChannel.readable(), channel.getReadableChannel());
    Assert.assertSame(theChannel.writable(), channel.getWritableChannel());
    Assert.assertSame(allocator, channel.getFrameMemoryAllocator());

    // Use mapWritableChannel to replace the writable channel with an open one; readable channel is no longer ready.
    final WritableFrameFileChannel otherWritableChannel = new WritableFrameFileChannel(null);
    final OutputChannel mappedChannel = channel.mapWritableChannel(c -> otherWritableChannel);

    Assert.assertEquals(1, channel.getPartitionNumber());
    Assert.assertSame(theChannel.readable(), channel.getReadableChannel());
    Assert.assertSame(otherWritableChannel, mappedChannel.getWritableChannel());
    Assert.assertSame(allocator, mappedChannel.getFrameMemoryAllocator());
  }

  @Test
  public void test_immediatelyReadablePair()
  {
    final BlockingQueueFrameChannel theChannel = BlockingQueueFrameChannel.minimal();
    final HeapMemoryAllocator allocator = HeapMemoryAllocator.unlimited();
    final OutputChannel channel = OutputChannel.immediatelyReadablePair(
        theChannel.writable(),
        allocator,
        theChannel.readable(),
        1
    );

    Assert.assertEquals(1, channel.getPartitionNumber());
    Assert.assertSame(theChannel.readable(), channel.getReadableChannel());
    Assert.assertSame(theChannel.writable(), channel.getWritableChannel());
    Assert.assertSame(allocator, channel.getFrameMemoryAllocator());

    // Use mapWritableChannel to replace the writable channel.
    final WritableFrameFileChannel otherWritableChannel = new WritableFrameFileChannel(null);
    final OutputChannel mappedChannel = channel.mapWritableChannel(c -> otherWritableChannel);

    Assert.assertEquals(1, mappedChannel.getPartitionNumber());
    Assert.assertSame(theChannel.readable(), mappedChannel.getReadableChannel());
    Assert.assertSame(otherWritableChannel, mappedChannel.getWritableChannel());
    Assert.assertSame(allocator, mappedChannel.getFrameMemoryAllocator());
  }
}
