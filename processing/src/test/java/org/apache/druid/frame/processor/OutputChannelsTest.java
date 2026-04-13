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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class OutputChannelsTest
{
  @Test
  public void test_none()
  {
    final OutputChannels channels = OutputChannels.none();

    Assertions.assertEquals(IntSets.emptySet(), channels.getPartitionNumbers());
    Assertions.assertEquals(Collections.emptyList(), channels.getAllChannels());
    Assertions.assertEquals(Collections.emptyList(), channels.getChannelsForPartition(0));
  }

  @Test
  public void test_wrap()
  {
    final OutputChannels channels = OutputChannels.wrap(ImmutableList.of(OutputChannel.nil(1)));

    Assertions.assertEquals(IntSet.of(1), channels.getPartitionNumbers());
    Assertions.assertEquals(1, channels.getAllChannels().size());
    Assertions.assertEquals(Collections.emptyList(), channels.getChannelsForPartition(0));
    Assertions.assertEquals(1, channels.getChannelsForPartition(1).size());
  }

  @Test
  public void test_readOnly()
  {
    final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
    final OutputChannels channels = OutputChannels.wrap(
        ImmutableList.of(
            OutputChannel.immediatelyReadablePair(
                channel.writable(),
                HeapMemoryAllocator.unlimited(),
                channel.readable(),
                1
            )
        )
    );

    final OutputChannels readOnlyChannels = channels.readOnly();
    Assertions.assertEquals(IntSet.of(1), readOnlyChannels.getPartitionNumbers());
    Assertions.assertEquals(1, readOnlyChannels.getAllChannels().size());
    Assertions.assertEquals(1, channels.getChannelsForPartition(1).size());

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> Iterables.getOnlyElement(readOnlyChannels.getAllChannels()).getWritableChannel()
    );

    Assertions.assertTrue(e.getMessage().startsWith("Writable channel is not available."));

    final DruidException e2 = Assertions.assertThrows(
        DruidException.class,
        () -> Iterables.getOnlyElement(readOnlyChannels.getAllChannels()).getFrameMemoryAllocator()
    );

    Assertions.assertTrue(e2.getMessage().startsWith("Frame memory allocator is not available."));
  }

  @Test
  public void test_sanityCheck()
  {
    final OutputChannels channelsDuplicatedPartition = OutputChannels.wrap(ImmutableList.of(
        OutputChannel.nil(1),
        OutputChannel.nil(1)
    ));
    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        channelsDuplicatedPartition::verifySingleChannel
    );
    Assertions.assertEquals("Expected one channel for partition [1], but got [2]", e.getMessage());

    final OutputChannels channelsNegativePartition = OutputChannels.wrap(ImmutableList.of(OutputChannel.nil(-1)));
    final IllegalStateException e2 = Assertions.assertThrows(
        IllegalStateException.class,
        channelsNegativePartition::verifySingleChannel
    );
    Assertions.assertEquals("Expected partitionNumber >= 0, but got [-1]", e2.getMessage());

    final OutputChannels channels = OutputChannels.wrap(ImmutableList.of(OutputChannel.nil(1), OutputChannel.nil(2)));
    Assertions.assertEquals(channels, channels.verifySingleChannel());
  }
}
