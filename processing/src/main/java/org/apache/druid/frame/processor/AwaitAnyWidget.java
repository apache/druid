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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper used by {@link FrameProcessorExecutor#runFully} when workers return {@link ReturnOrAwait#awaitAny}.
 *
 * The main idea is to reuse listeners from previous calls to {@link #awaitAny(IntSet)} in cases where a particular
 * channel has not receieved any input since the last call. (Otherwise, listeners would pile up.)
 */
public class AwaitAnyWidget
{
  private final List<ReadableFrameChannel> channels;

  @GuardedBy("listeners")
  private final List<ChannelListener> listeners;

  public AwaitAnyWidget(final List<ReadableFrameChannel> channels)
  {
    this.channels = channels;
    this.listeners = new ArrayList<>(channels.size());

    for (int i = 0; i < channels.size(); i++) {
      listeners.add(null);
    }
  }

  /**
   * Returns a future that resolves when any channel in the provided set is ready for reading.
   *
   * Numbers in this set correspond to positions in the {@link #channels} list.
   */
  public ListenableFuture<?> awaitAny(final IntSet awaitSet)
  {
    synchronized (listeners) {
      // Will be set to null when any channel is ready. We use null because the specific value doesn't matter:
      // the purpose of this future is just to allow waiting and serve as a signal.
      final SettableFuture<?> retVal = SettableFuture.create();

      final IntIterator awaitSetIterator = awaitSet.iterator();
      while (awaitSetIterator.hasNext()) {
        final int channelNumber = awaitSetIterator.nextInt();
        final ReadableFrameChannel channel = channels.get(channelNumber);

        if (channel.canRead() || channel.isFinished()) {
          retVal.set(null);
          return retVal;
        } else {
          final ChannelListener priorListener = listeners.get(channelNumber);
          if (priorListener == null || !priorListener.replaceFuture(retVal)) {
            final ChannelListener newListener = new ChannelListener(retVal);
            channel.readabilityFuture().addListener(newListener, Execs.directExecutor());
            listeners.set(channelNumber, newListener);
          }
        }
      }

      return retVal;
    }
  }

  private static class ChannelListener implements Runnable
  {
    @GuardedBy("this")
    private SettableFuture<?> future;

    public ChannelListener(SettableFuture<?> future)
    {
      this.future = future;
    }

    @Override
    public void run()
    {
      synchronized (this) {
        try {
          future.set(null);
        }
        finally {
          future = null;
        }
      }
    }

    /**
     * Replaces our future if the listener has not fired yet.
     *
     * Returns true if the future was replaced, false if the listener has already fired.
     */
    public boolean replaceFuture(final SettableFuture<?> newFuture)
    {
      synchronized (this) {
        if (future == null) {
          return false;
        } else {
          future = newFuture;
          return true;
        }
      }
    }
  }
}
