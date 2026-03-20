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
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents an output channel for some frame processor. Composed of a pair of {@link WritableFrameChannel}, which the
 * processor writes to, along with a supplier of a {@link ReadableFrameChannel}, which readers can read from.
 *
 * At the time an instance of this class is created, the writable channel is already open, but the readable channel
 * has not yet been created. It is created upon the first call to {@link #getReadableChannel()}.
 */
public class OutputChannel
{
  @Nullable
  private volatile WritableFrameChannel writableChannel;

  @Nullable
  private volatile MemoryAllocator frameMemoryAllocator;

  private final Supplier<ReadableFrameChannel> readableChannelSupplier;
  private final int partitionNumber;

  private OutputChannel(
      @Nullable final WritableFrameChannel writableChannel,
      @Nullable final MemoryAllocator frameMemoryAllocator,
      final Supplier<ReadableFrameChannel> readableChannelSupplier,
      final int partitionNumber
  )
  {
    this.writableChannel = writableChannel;
    this.frameMemoryAllocator = frameMemoryAllocator;
    this.readableChannelSupplier = readableChannelSupplier;
    this.partitionNumber = partitionNumber;

    if (partitionNumber < 0 && partitionNumber != WritableFrameChannel.NO_PARTITION) {
      throw new IAE("Invalid partition number [%d]", partitionNumber);
    }
  }

  /**
   * Creates an output channel pair, where the readable channel is not usable until writing is complete.
   *
   * @param writableChannel         writable channel for producer
   * @param frameMemoryAllocator    memory allocator for producer to use while writing frames to the channel
   * @param readableChannelSupplier readable channel for consumer. May be called multiple times, so you should wrap this
   *                                in {@link Suppliers#memoize} if needed.
   * @param partitionNumber         partition number, if any; may be {@link WritableFrameChannel#NO_PARTITION} if unknown
   */
  public static OutputChannel pair(
      final WritableFrameChannel writableChannel,
      final MemoryAllocator frameMemoryAllocator,
      final Supplier<ReadableFrameChannel> readableChannelSupplier,
      final int partitionNumber
  )
  {
    return new OutputChannel(
        Preconditions.checkNotNull(writableChannel, "writableChannel"),
        Preconditions.checkNotNull(frameMemoryAllocator, "frameMemoryAllocator"),
        readableChannelSupplier,
        partitionNumber
    );
  }

  /**
   * Creates an output channel pair, where the readable channel is usable before writing is complete.
   *
   * @param writableChannel      writable channel for producer
   * @param frameMemoryAllocator memory allocator for producer to use while writing frames to the channel
   * @param readableChannel      readable channel for consumer
   * @param partitionNumber      partition number, if any; may be {@link WritableFrameChannel#NO_PARTITION} if unknown
   */
  public static OutputChannel immediatelyReadablePair(
      final WritableFrameChannel writableChannel,
      final MemoryAllocator frameMemoryAllocator,
      final ReadableFrameChannel readableChannel,
      final int partitionNumber
  )
  {
    return new OutputChannel(
        Preconditions.checkNotNull(writableChannel, "writableChannel"),
        Preconditions.checkNotNull(frameMemoryAllocator, "frameMemoryAllocator"),
        () -> readableChannel,
        partitionNumber
    );
  }

  /**
   * Creates a read-only output channel.
   *
   * @param readableChannel readable channel for consumer.
   * @param partitionNumber partition number, if any; may be {@link WritableFrameChannel#NO_PARTITION} if unknown
   */
  public static OutputChannel readOnly(
      final ReadableFrameChannel readableChannel,
      final int partitionNumber
  )
  {
    return readOnly(() -> readableChannel, partitionNumber);
  }

  /**
   * Creates a read-only output channel.
   *
   * @param readableChannelSupplier readable channel for consumer. May be called multiple times, so you should wrap this
   *                                in {@link Suppliers#memoize} if needed.
   * @param partitionNumber         partition number, if any; may be {@link WritableFrameChannel#NO_PARTITION} if unknown
   */
  public static OutputChannel readOnly(
      final Supplier<ReadableFrameChannel> readableChannelSupplier,
      final int partitionNumber
  )
  {
    return new OutputChannel(null, null, readableChannelSupplier, partitionNumber);
  }

  /**
   * Create a nil output channel, representing a processor that writes nothing. It is not actually writable, but
   * provides a way for downstream processors to read nothing.
   */
  public static OutputChannel nil(final int partitionNumber)
  {
    return new OutputChannel(null, null, () -> ReadableNilFrameChannel.INSTANCE, partitionNumber);
  }

  /**
   * Returns the writable channel of this pair. The producer writes to this channel. Throws ISE if the output channel is
   * read only.
   */
  public WritableFrameChannel getWritableChannel()
  {
    // Store into a local since the field is volatile.
    final WritableFrameChannel theWritableChannel = writableChannel;

    if (theWritableChannel == null) {
      throw DruidException.defensive(
          "Writable channel is not available. "
          + "The output channel might be marked as read-only, hence no writes are allowed."
      );
    } else {
      return theWritableChannel;
    }
  }

  /**
   * Returns the memory allocator for the writable channel. The producer uses this to generate frames for the channel.
   * Throws ISE if the output channel is read only.
   */
  public MemoryAllocator getFrameMemoryAllocator()
  {
    // Store into a local since the field is volatile.
    final MemoryAllocator theFrameMemoryAllocator = frameMemoryAllocator;

    if (theFrameMemoryAllocator == null) {
      throw DruidException.defensive(
          "Frame memory allocator is not available. "
          + "The output channel might be marked as read-only, hence no writes are allowed."
      );
    } else {
      return theFrameMemoryAllocator;
    }
  }

  /**
   * Returns the readable channel of this pair. This readable channel may, or may not, be usable before the
   * writable channel is closed. It depends on how the channel pair was created.
   */
  public ReadableFrameChannel getReadableChannel()
  {
    return readableChannelSupplier.get();
  }

  public Supplier<ReadableFrameChannel> getReadableChannelSupplier()
  {
    return readableChannelSupplier;
  }

  public int getPartitionNumber()
  {
    return partitionNumber;
  }

  public OutputChannel mapWritableChannel(final Function<WritableFrameChannel, WritableFrameChannel> mapFn)
  {
    // Store into locals since the fields are volatile.
    final WritableFrameChannel theWritableChannel = writableChannel;
    final MemoryAllocator theFrameMemoryAllocator = frameMemoryAllocator;

    if (theWritableChannel == null || theFrameMemoryAllocator == null) {
      return this;
    } else {
      return new OutputChannel(
          mapFn.apply(theWritableChannel),
          theFrameMemoryAllocator,
          readableChannelSupplier,
          partitionNumber
      );
    }
  }

  /**
   * Returns a read-only version of this instance. Read-only versions have neither {@link #getWritableChannel()} nor
   * {@link #getFrameMemoryAllocator()}, and therefore require substantially less memory.
   *
   * Returns the same instance if it is already read-only.
   */
  public OutputChannel readOnly()
  {
    return isReadOnly() ? this : OutputChannel.readOnly(readableChannelSupplier, partitionNumber);
  }

  /**
   * Returns whether this instance is read-only (has no writable channel).
   */
  public boolean isReadOnly()
  {
    return writableChannel == null;
  }

  /**
   * Removes the reference to the {@link #writableChannel} and {@link #frameMemoryAllocator} from the object, making
   * it more efficient
   */
  public void convertToReadOnly()
  {
    this.writableChannel = null;
    this.frameMemoryAllocator = null;
  }
}
