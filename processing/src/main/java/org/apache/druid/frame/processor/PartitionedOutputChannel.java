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
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A channel which can contain multiple partitions of data. It is used by {@link SuperSorter} currently to write multiple
 * partitions on the same channel. The readable channel provided to the caller is an instance of
 * {@link PartitionedReadableFrameChannel} which allows the caller to open a readable channel for the desired partition.
 */
public class PartitionedOutputChannel
{
  @Nullable
  private final WritableFrameChannel writableChannel;
  @Nullable
  private final MemoryAllocator frameMemoryAllocator;
  private final Supplier<PartitionedReadableFrameChannel> readableChannelSupplier;

  private PartitionedOutputChannel(
      @Nullable final WritableFrameChannel writableChannel,
      @Nullable final MemoryAllocator frameMemoryAllocator,
      final Supplier<PartitionedReadableFrameChannel> readableChannelSupplier
  )
  {
    this.writableChannel = writableChannel;
    this.frameMemoryAllocator = frameMemoryAllocator;
    this.readableChannelSupplier = readableChannelSupplier;
  }

  /**
   * Creates a partitioned output channel pair.
   *
   * @param writableChannel         writable channel for producer
   * @param frameMemoryAllocator    memory allocator for producer to use while writing frames to the channel
   * @param readableChannelSupplier partitioned readable channel for consumer. May be called multiple times, so you
   *                                should wrap this in {@link Suppliers#memoize} if needed.
   */
  public static PartitionedOutputChannel pair(
      final WritableFrameChannel writableChannel,
      final MemoryAllocator frameMemoryAllocator,
      final Supplier<PartitionedReadableFrameChannel> readableChannelSupplier
  )
  {
    return new PartitionedOutputChannel(
        Preconditions.checkNotNull(writableChannel, "writableChannel"),
        Preconditions.checkNotNull(frameMemoryAllocator, "frameMemoryAllocator"),
        readableChannelSupplier
    );
  }

  /**
   * Returns the writable channel of this pair. The producer writes to this channel.
   */
  public WritableFrameChannel getWritableChannel()
  {
    if (writableChannel == null) {
      throw new ISE("Writable channel is not available");
    } else {
      return writableChannel;
    }
  }

  /**
   * Returns the memory allocator for the writable channel. The producer uses this to generate frames for the channel.
   */
  public MemoryAllocator getFrameMemoryAllocator()
  {
    if (frameMemoryAllocator == null) {
      throw new ISE("Writable channel is not available");
    } else {
      return frameMemoryAllocator;
    }
  }

  /**
   * Returns the partitioned readable channel supplier of this pair. The consumer reads from this channel.
   */
  public Supplier<PartitionedReadableFrameChannel> getReadableChannelSupplier()
  {
    return readableChannelSupplier;
  }

  public PartitionedOutputChannel mapWritableChannel(final Function<WritableFrameChannel, WritableFrameChannel> mapFn)
  {
    if (writableChannel == null) {
      return this;
    } else {
      return new PartitionedOutputChannel(
          mapFn.apply(writableChannel),
          frameMemoryAllocator,
          readableChannelSupplier
      );
    }
  }
}
