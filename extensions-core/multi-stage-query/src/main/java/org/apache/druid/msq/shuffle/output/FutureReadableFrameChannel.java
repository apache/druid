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

package org.apache.druid.msq.shuffle.output;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.NoSuchElementException;

public class FutureReadableFrameChannel implements ReadableFrameChannel
{
  private static final Logger log = new Logger(FutureReadableFrameChannel.class);

  private final ListenableFuture<ReadableFrameChannel> channelFuture;
  private ReadableFrameChannel channel;

  public FutureReadableFrameChannel(final ListenableFuture<ReadableFrameChannel> channelFuture)
  {
    this.channelFuture = channelFuture;
  }

  @Override
  public boolean isFinished()
  {
    if (populateChannel()) {
      return channel.isFinished();
    } else {
      return false;
    }
  }

  @Override
  public boolean canRead()
  {
    if (populateChannel()) {
      return channel.canRead();
    } else {
      return false;
    }
  }

  @Override
  public Frame read()
  {
    if (populateChannel()) {
      return channel.read();
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    if (populateChannel()) {
      return channel.readabilityFuture();
    } else {
      return FutureUtils.transformAsync(channelFuture, ignored -> readabilityFuture());
    }
  }

  @Override
  public void close()
  {
    if (populateChannel()) {
      channel.close();
    } else {
      channelFuture.cancel(true);
      channelFuture.addListener(
          () -> {
            final ReadableFrameChannel channel;

            try {
              channel = FutureUtils.getUncheckedImmediately(channelFuture);
            }
            catch (Throwable ignored) {
              // Some error happened while creating the channel. Suppress it.
              return;
            }

            try {
              channel.close();
            }
            catch (Throwable t) {
              log.noStackTrace().warn(t, "Failed to close channel");
            }
          },
          Execs.directExecutor()
      );
    }
  }

  private boolean populateChannel()
  {
    if (channel != null) {
      return true;
    } else if (channelFuture.isDone()) {
      channel = FutureUtils.getUncheckedImmediately(channelFuture);
      return true;
    } else {
      return false;
    }
  }
}
