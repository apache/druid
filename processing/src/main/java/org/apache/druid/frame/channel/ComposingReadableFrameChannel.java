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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;

/**
 * A composed readable channel to read frames. The channel can encapsulate multiple readable channels in it and
 * automatically switches to next channels once the currently read channel is finished.
 */
@NotThreadSafe
public class ComposingReadableFrameChannel implements ReadableFrameChannel
{
  private final List<Supplier<ReadableFrameChannel>> channels;
  private ReadableFrameChannel currentChannel;
  private int currentIndex;

  public ComposingReadableFrameChannel(
      int partition,
      List<Supplier<ReadableFrameChannel>> channels,
      HashSet<Integer> validChannels
  )
  {
    Preconditions.checkNotNull(channels, "channels is null");
    if (validChannels == null) {
      // no writes for the partition, send an empty readable channel
      this.channels = ImmutableList.of(() -> ReadableNilFrameChannel.INSTANCE);
    } else {
      Preconditions.checkState(validChannels.size() > 0, "No channels found for partition " + partition);
      ImmutableList.Builder<Supplier<ReadableFrameChannel>> validChannelsBuilder = ImmutableList.builder();
      ArrayList<Integer> sortedChannelIds = new ArrayList<>(validChannels);
      Collections.sort(sortedChannelIds); // the data was written from lowest to highest channel
      for (Integer channelId : sortedChannelIds) {
        validChannelsBuilder.add(channels.get(channelId));
      }
      this.channels = validChannelsBuilder.build();
    }
    this.currentIndex = 0;
    this.currentChannel = null;
  }

  @Override
  public boolean isFinished()
  {
    initCurrentChannel();
    if (!currentChannel.isFinished()) {
      return false;
    }
    currentChannel.close();
    currentChannel = null;
    if (isLastIndex()) {
      return true;
    }
    ++currentIndex;
    return isFinished();
  }

  @Override
  public boolean canRead()
  {
    initCurrentChannel();
    if (currentChannel.canRead()) {
      return true;
    }
    if (currentChannel.isFinished()) {
      currentChannel.close();
      currentChannel = null;
      if (isLastIndex()) {
        return false;
      }
      ++currentIndex;
      return canRead();
    }
    return false;
  }

  @Override
  public Frame read()
  {
    return currentChannel.read();
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    initCurrentChannel();
    if (!currentChannel.isFinished()) {
      return currentChannel.readabilityFuture();
    }
    currentChannel.close();
    currentChannel = null;
    if (isLastIndex()) {
      return Futures.immediateFuture(true);
    }
    ++currentIndex;
    return readabilityFuture();
  }

  @Override
  public void close()
  {
    if (currentChannel != null) {
      currentChannel.close();
    }
  }

  private boolean isLastIndex()
  {
    return currentIndex == channels.size() - 1;
  }

  private void initCurrentChannel()
  {
    if (currentChannel == null) {
      currentChannel = channels.get(currentIndex).get();
    }
  }
}
