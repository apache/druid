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

package org.apache.druid.msq.indexing;

import com.google.common.base.Preconditions;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.msq.counters.StorageCounters;

import java.io.IOException;

/**
 * Wraps an {@link OutputChannelFactory} to track storage file and byte counts in {@link StorageCounters}.
 * Similar to {@link ChannelCountingOutputChannelFactory} but for storage-level tracking rather than
 * per-partition channel tracking.
 */
public class StorageCountingOutputChannelFactory implements OutputChannelFactory
{
  private final OutputChannelFactory baseFactory;
  private final StorageCounters storageCounters;
  private final boolean isDurable;

  public StorageCountingOutputChannelFactory(
      final OutputChannelFactory baseFactory,
      final StorageCounters storageCounters,
      final boolean isDurable
  )
  {
    this.baseFactory = Preconditions.checkNotNull(baseFactory, "baseFactory");
    this.storageCounters = Preconditions.checkNotNull(storageCounters, "storageCounters");
    this.isDurable = isDurable;
  }

  @Override
  public OutputChannel openChannel(final int partitionNumber) throws IOException
  {
    final OutputChannel baseChannel = baseFactory.openChannel(partitionNumber);

    return baseChannel.mapWritableChannel(
        baseWritableChannel ->
            new StorageCountingWritableFrameChannel(
                baseWritableChannel,
                storageCounters,
                isDurable
            )
    );
  }

  @Override
  public PartitionedOutputChannel openPartitionedChannel(
      final String name,
      final boolean deleteAfterRead
  ) throws IOException
  {
    final PartitionedOutputChannel baseChannel = baseFactory.openPartitionedChannel(name, deleteAfterRead);

    return baseChannel.mapWritableChannel(
        baseWritableChannel ->
            new StorageCountingWritableFrameChannel(
                baseWritableChannel,
                storageCounters,
                isDurable
            )
    );
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    // No need for storage counters on nil channels: they never receive input.
    return baseFactory.openNilChannel(partitionNumber);
  }

  @Override
  public boolean isBuffered()
  {
    return baseFactory.isBuffered();
  }
}
