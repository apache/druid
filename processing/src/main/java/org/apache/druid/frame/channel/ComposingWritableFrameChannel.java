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

package org.apache.druid.frame.channel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.ResourceLimitExceededException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ComposingWritableFrameChannel implements WritableFrameChannel
{
  private final List<Supplier<WritableFrameChannel>> channels;
  private final Map<Integer, HashSet<Integer>> partitionToChannelMap;
  private int currentIndex;

  public ComposingWritableFrameChannel(
      List<Supplier<WritableFrameChannel>> channels,
      Map<Integer, HashSet<Integer>> partitionToChannelMap
  )
  {
    this.channels = Preconditions.checkNotNull(channels, "channels is null");
    this.partitionToChannelMap =
        Preconditions.checkNotNull(partitionToChannelMap, "partitionToChannelMap is null");
    this.currentIndex = 0;
  }

  @Override
  public void write(FrameWithPartition frameWithPartition) throws IOException
  {
    if (currentIndex >= channels.size()) {
      throw new ISE("No more channels available to write. Total available channels : " + channels.size());
    }

    try {
      channels.get(currentIndex).get().write(frameWithPartition);
      partitionToChannelMap.computeIfAbsent(frameWithPartition.partition(), k -> Sets.newHashSetWithExpectedSize(1))
                           .add(currentIndex);
    }
    catch (ResourceLimitExceededException rlee) {
      channels.get(currentIndex).get().close();
      currentIndex++;
      if (currentIndex >= channels.size()) {
        throw rlee;
      }
      write(frameWithPartition);
    }
  }

  @Override
  public void fail(@Nullable Throwable cause) throws IOException
  {
    for (Supplier<WritableFrameChannel> channel : channels) {
      channel.get().fail(cause);
    }
  }

  @Override
  public void close() throws IOException
  {
    if (currentIndex < channels.size()) {
      channels.get(currentIndex).get().close();
    }
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return channels.get(currentIndex).get().writabilityFuture();
  }
}
