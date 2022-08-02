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

import org.apache.druid.frame.Frame;
import org.apache.druid.java.util.common.guava.BaseSequence;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

/**
 * Adapter that converts a {@link ReadableFrameChannel} into a {@link org.apache.druid.java.util.common.guava.Sequence}
 * of {@link Frame}.
 *
 * This class does blocking reads on the channel, rather than nonblocking reads. Therefore, it is preferable to use
 * {@link ReadableFrameChannel} directly whenever nonblocking reads are desired.
 */
public class FrameChannelSequence extends BaseSequence<Frame, FrameChannelSequence.FrameChannelIterator>
{
  public FrameChannelSequence(final ReadableFrameChannel channel)
  {
    super(
        new IteratorMaker<Frame, FrameChannelIterator>()
        {
          @Override
          public FrameChannelIterator make()
          {
            return new FrameChannelIterator(channel);
          }

          @Override
          public void cleanup(FrameChannelIterator iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );
  }

  public static class FrameChannelIterator implements Iterator<Frame>, Closeable
  {
    private final ReadableFrameChannel channel;

    private FrameChannelIterator(final ReadableFrameChannel channel)
    {
      this.channel = channel;
    }

    @Override
    public boolean hasNext()
    {
      // Blocking read.
      await();
      return channel.canRead();
    }

    @Override
    public Frame next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      return channel.read();
    }

    @Override
    public void close()
    {
      channel.doneReading();
    }

    private void await()
    {
      try {
        channel.readabilityFuture().get();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
