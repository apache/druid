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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Channel that concatenates a sequence of other channels that are provided by an iterator. The iterator is
 * walked just-in-time, meaning that {@link Iterator#next()} is not called until the channel is actually ready
 * to be used.
 *
 * The first channel is pulled from the iterator immediately upon construction.
 */
public class ReadableConcatFrameChannel implements ReadableFrameChannel
{
  private final Iterator<ReadableFrameChannel> channelIterator;

  // Null means there were never any channels to begin with.
  @Nullable
  private ReadableFrameChannel currentChannel;

  private ReadableConcatFrameChannel(Iterator<ReadableFrameChannel> channelIterator)
  {
    this.channelIterator = channelIterator;
    this.currentChannel = channelIterator.hasNext() ? channelIterator.next() : null;
  }

  /**
   * Creates a new concatenated channel. The first channel is pulled from the provided iterator immediately.
   * Other channels are pulled on-demand, when they are ready to be used.
   */
  public static ReadableConcatFrameChannel open(final Iterator<ReadableFrameChannel> channelIterator)
  {
    return new ReadableConcatFrameChannel(channelIterator);
  }

  @Override
  public boolean isFinished()
  {
    advanceCurrentChannelIfFinished();
    return currentChannel == null || currentChannel.isFinished();
  }

  @Override
  public boolean canRead()
  {
    advanceCurrentChannelIfFinished();
    return currentChannel != null && currentChannel.canRead();
  }

  @Override
  public Frame read()
  {
    if (!canRead()) {
      throw new NoSuchElementException();
    }

    assert currentChannel != null; // True because canRead() was true.
    return currentChannel.read();
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    advanceCurrentChannelIfFinished();

    if (currentChannel == null) {
      return Futures.immediateFuture(null);
    } else {
      return currentChannel.readabilityFuture();
    }
  }

  @Override
  public void close()
  {
    if (currentChannel != null) {
      currentChannel.close();
    }
  }

  private void advanceCurrentChannelIfFinished()
  {
    while (currentChannel != null && currentChannel.isFinished() && channelIterator.hasNext()) {
      currentChannel = channelIterator.next();
    }
  }
}
