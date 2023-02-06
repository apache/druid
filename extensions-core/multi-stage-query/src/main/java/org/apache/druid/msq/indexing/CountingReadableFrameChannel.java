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
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.msq.counters.ChannelCounters;

public class CountingReadableFrameChannel implements ReadableFrameChannel
{
  private final ReadableFrameChannel baseChannel;
  private final ChannelCounters channelCounters;
  private final int partitionNumber;

  public CountingReadableFrameChannel(
      ReadableFrameChannel baseChannel,
      ChannelCounters channelCounters,
      int partitionNumber
  )
  {
    this.baseChannel = baseChannel;
    this.channelCounters = channelCounters;
    this.partitionNumber = partitionNumber;
  }

  @Override
  public boolean isFinished()
  {
    return baseChannel.isFinished();
  }

  @Override
  public boolean canRead()
  {
    return baseChannel.canRead();
  }

  @Override
  public Frame read()
  {
    final Frame frame = baseChannel.read();
    channelCounters.addFrame(partitionNumber, frame);
    return frame;
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    return baseChannel.readabilityFuture();
  }

  @Override
  public void close()
  {
    baseChannel.close();
  }
}
