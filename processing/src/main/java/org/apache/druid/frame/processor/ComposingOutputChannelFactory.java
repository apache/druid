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

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ComposingReadableFrameChannel;
import org.apache.druid.frame.channel.ComposingWritableFrameChannel;
import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class ComposingOutputChannelFactory implements OutputChannelFactory
{
  private final List<OutputChannelFactory> channelFactories;
  private final int frameSize;

  public ComposingOutputChannelFactory(List<OutputChannelFactory> channelFactories, int frameSize)
  {
    this.channelFactories = Preconditions.checkNotNull(channelFactories, "channelFactories is null");
    this.frameSize = frameSize;
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    ImmutableList.Builder<Supplier<WritableFrameChannel>> writableFrameChannelSuppliersBuilder = ImmutableList.builder();
    ImmutableList.Builder<Supplier<ReadableFrameChannel>> readableFrameChannelSuppliersBuilder = ImmutableList.builder();
    for (OutputChannelFactory channelFactory : channelFactories) {
      // open channel lazily
      Supplier<OutputChannel> channel =
          Suppliers.memoize(() -> {
            try {
              return channelFactory.openChannel(partitionNumber);
            }
            catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          })::get;
      writableFrameChannelSuppliersBuilder.add(() -> channel.get().getWritableChannel());
      readableFrameChannelSuppliersBuilder.add(() -> channel.get().getReadableChannelSupplier().get());
    }
    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(
        writableFrameChannelSuppliersBuilder.build(),
        partitionToChannelMap
    );
    Supplier<ReadableFrameChannel> readableFrameChannelSupplier = Suppliers.memoize(
        () -> new ComposingReadableFrameChannel(
            partitionNumber,
            readableFrameChannelSuppliersBuilder.build(),
            partitionToChannelMap
        )
    )::get;
    return OutputChannel.pair(
        writableFrameChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        readableFrameChannelSupplier,
        partitionNumber
    );
  }

  @Override
  public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead)
  {
    ImmutableList.Builder<Supplier<WritableFrameChannel>> writableFrameChannelsBuilder = ImmutableList.builder();
    ImmutableList.Builder<Supplier<PartitionedReadableFrameChannel>> readableFrameChannelSuppliersBuilder =
        ImmutableList.builder();
    for (OutputChannelFactory channelFactory : channelFactories) {
      Supplier<PartitionedOutputChannel> channel =
          Suppliers.memoize(() -> {
            try {
              return channelFactory.openPartitionedChannel(name, deleteAfterRead);
            }
            catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          })::get;
      writableFrameChannelsBuilder.add(() -> channel.get().getWritableChannel());
      readableFrameChannelSuppliersBuilder.add(() -> channel.get().getReadableChannelSupplier().get());
    }
    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(
        writableFrameChannelsBuilder.build(),
        partitionToChannelMap
    );

    List<Supplier<PartitionedReadableFrameChannel>> readableFrameChannelSuppliers =
        readableFrameChannelSuppliersBuilder.build();
    PartitionedReadableFrameChannel partitionedReadableFrameChannel = new PartitionedReadableFrameChannel()
    {
      private final Set<Integer> openedChannels = Sets.newHashSetWithExpectedSize(1);

      @Override
      public ReadableFrameChannel getReadableFrameChannel(int partitionNumber)
      {
        ImmutableList.Builder<Supplier<ReadableFrameChannel>> suppliers = ImmutableList.builder();
        for (int i = 0; i < readableFrameChannelSuppliers.size(); i++) {
          int finalI = i;
          suppliers.add(
              Suppliers.memoize(
                  () -> {
                    openedChannels.add(finalI);
                    return readableFrameChannelSuppliers.get(finalI).get().getReadableFrameChannel(partitionNumber);
                  }
              )::get
          );
        }

        return new ComposingReadableFrameChannel(partitionNumber, suppliers.build(), partitionToChannelMap);
      }

      @Override
      public void close() throws IOException
      {
        for (Integer channelId : openedChannels) {
          readableFrameChannelSuppliers.get(channelId).get().close();
        }
      }
    };

    return PartitionedOutputChannel.pair(
        writableFrameChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> partitionedReadableFrameChannel
    );
  }

  @Override
  public OutputChannel openNilChannel(int partitionNumber)
  {
    return OutputChannel.nil(partitionNumber);
  }
}
