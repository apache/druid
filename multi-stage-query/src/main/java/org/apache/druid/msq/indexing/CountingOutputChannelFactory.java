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
import org.apache.druid.msq.counters.ChannelCounters;

import java.io.IOException;

public class CountingOutputChannelFactory implements OutputChannelFactory
{
  private final OutputChannelFactory baseFactory;
  private final ChannelCounters channelCounters;

  public CountingOutputChannelFactory(
      final OutputChannelFactory baseFactory,
      final ChannelCounters channelCounters
  )
  {
    this.baseFactory = Preconditions.checkNotNull(baseFactory, "baseFactory");
    this.channelCounters = Preconditions.checkNotNull(channelCounters, "channelCounter");
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    final OutputChannel baseChannel = baseFactory.openChannel(partitionNumber);

    return baseChannel.mapWritableChannel(
        baseWritableChannel ->
            new CountingWritableFrameChannel(
                baseChannel.getWritableChannel(),
                channelCounters,
                partitionNumber
            )
    );
  }

  @Override
  public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead) throws IOException
  {
    final PartitionedOutputChannel baseChannel = baseFactory.openPartitionedChannel(name, deleteAfterRead);

    return baseChannel.mapWritableChannel(
        baseWritableChannel ->
            new CountingWritableFrameChannel(
                baseChannel.getWritableChannel(),
                channelCounters,
                null
            )
    );
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    // No need for counters on nil channels: they never receive input.
    return baseFactory.openNilChannel(partitionNumber);
  }

  @Override
  public boolean isBuffered()
  {
    return baseFactory.isBuffered();
  }
}
