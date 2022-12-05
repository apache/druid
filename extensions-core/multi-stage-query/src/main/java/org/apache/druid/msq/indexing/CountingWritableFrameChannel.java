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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.msq.counters.ChannelCounters;

import javax.annotation.Nullable;
import java.io.IOException;

public class CountingWritableFrameChannel implements WritableFrameChannel
{
  private final WritableFrameChannel baseChannel;
  private final ChannelCounters channelCounters;
  private final int partitionNumber;

  public CountingWritableFrameChannel(
      final WritableFrameChannel baseChannel,
      final ChannelCounters channelCounters,
      final int partitionNumber
  )
  {
    this.baseChannel = baseChannel;
    this.channelCounters = channelCounters;
    this.partitionNumber = partitionNumber;
  }

  @Override
  public void write(FrameWithPartition frame) throws IOException
  {
    baseChannel.write(frame);
    channelCounters.addFrame(partitionNumber, frame.frame());
  }

  @Override
  public void fail(@Nullable Throwable cause) throws IOException
  {
    baseChannel.fail(cause);
  }

  @Override
  public void close() throws IOException
  {
    baseChannel.close();
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return baseChannel.writabilityFuture();
  }
}
