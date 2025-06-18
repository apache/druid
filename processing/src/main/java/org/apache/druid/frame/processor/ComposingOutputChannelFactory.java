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

/**
 * A channel factory which provides ordered composed channels. The factory encapsulates multiple output channel factories
 * and automatically switches between then when the current factory is 'full' for writes. The reads can also encapsulate
 * multiple readable channels with automatic switching.
 */
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
  public OutputChannel openChannel(int partitionNumber)
  {
    ImmutableList.Builder<Supplier<WritableFrameChannel>> writableFrameChannelSuppliersBuilder = ImmutableList.builder();
    ImmutableList.Builder<Supplier<ReadableFrameChannel>> readableFrameChannelSuppliersBuilder = ImmutableList.builder();
    ImmutableList.Builder<Supplier<OutputChannel>> outputChannelSupplierBuilder = ImmutableList.builder();
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
      outputChannelSupplierBuilder.add(channel);
      writableFrameChannelSuppliersBuilder.add(() -> channel.get().getWritableChannel());
      // We read the output channel once they have been written to, and therefore it is space efficient and safe to
      // save their read only copies
      readableFrameChannelSuppliersBuilder.add(() -> channel.get().readOnly().getReadableChannelSupplier().get());
    }

    // the map maintains a mapping of channels which have the data for a given partition.
    // it is useful to identify the readable channels to open in the composition while reading the partition data.
    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(
        outputChannelSupplierBuilder.build(),
        null,
        writableFrameChannelSuppliersBuilder.build(),
        partitionToChannelMap
    );
    Supplier<ReadableFrameChannel> readableFrameChannelSupplier = Suppliers.memoize(
        () -> new ComposingReadableFrameChannel(
            partitionNumber,
            readableFrameChannelSuppliersBuilder.build(),
            partitionToChannelMap.get(partitionNumber)
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
    ImmutableList.Builder<Supplier<PartitionedOutputChannel>> partitionedOutputChannelSupplierBuilder = ImmutableList.builder();
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
      partitionedOutputChannelSupplierBuilder.add(channel);
      writableFrameChannelsBuilder.add(() -> channel.get().getWritableChannel());
      // We read the output channel once they have been written to, and therefore it is space efficient and safe to
      // save their read only copies
      readableFrameChannelSuppliersBuilder.add(() -> channel.get().readOnly().getReadableChannelSupplier().get());
    }
    // the map maintains a mapping of channels which have the data for a given partition.
    // it is useful to identify the readable channels to open in the composition while reading the partition data.

    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(
        null,
        partitionedOutputChannelSupplierBuilder.build(),
        writableFrameChannelsBuilder.build(),
        partitionToChannelMap
    );

    List<Supplier<PartitionedReadableFrameChannel>> readableFrameChannelSuppliers =
        readableFrameChannelSuppliersBuilder.build();
    PartitionedReadableFrameChannel partitionedReadableFrameChannel = new PartitionedReadableFrameChannel()
    {
      // maintained so that we only close channels which were opened for reading
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

        return new ComposingReadableFrameChannel(
            partitionNumber,
            suppliers.build(),
            partitionToChannelMap.get(partitionNumber)
        );
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
    // Doing this since some output channel factories create marker objects for nil channels
    for (OutputChannelFactory outputChannelFactory : channelFactories) {
      outputChannelFactory.openNilChannel(partitionNumber);
    }
    return OutputChannel.nil(partitionNumber);
  }

  @Override
  public boolean isBuffered()
  {
    for (final OutputChannelFactory channelFactory : channelFactories) {
      if (channelFactory.isBuffered()) {
        return true;
      }
    }

    return false;
  }
}
