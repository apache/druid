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

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumns;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class FrameProcessors
{
  private FrameProcessors()
  {
    // No instantiation.
  }

  public static <T> FrameProcessor<T> withBaggage(final FrameProcessor<T> processor, final Closeable baggage)
  {
    class FrameProcessorWithBaggage implements FrameProcessor<T>
    {
      final AtomicBoolean cleanedUp = new AtomicBoolean();

      @Override
      public List<ReadableFrameChannel> inputChannels()
      {
        return processor.inputChannels();
      }

      @Override
      public List<WritableFrameChannel> outputChannels()
      {
        return processor.outputChannels();
      }

      @Override
      public ReturnOrAwait<T> runIncrementally(IntSet readableInputs) throws InterruptedException, IOException
      {
        return processor.runIncrementally(readableInputs);
      }

      @Override
      public void cleanup() throws IOException
      {
        if (cleanedUp.compareAndSet(false, true)) {
          //noinspection EmptyTryBlock
          try (Closeable ignore1 = baggage;
               Closeable ignore2 = processor::cleanup) {
            // piggy-back try-with-resources semantics
          }
        }
      }

      @Override
      public String toString()
      {
        return processor + " (with baggage)";
      }
    }

    return new FrameProcessorWithBaggage();
  }

  public static Cursor makeCursor(final Frame frame, final FrameReader frameReader)
  {
    // Safe to never close the Sequence that the Cursor comes from, because it does not do anything when it is closed.
    // Refer to FrameStorageAdapter#makeCursors.

    return Yielders.each(
        new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY)
            .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null)
    ).get();
  }

  /**
   * Helper method for implementing {@link FrameProcessor#cleanup()}.
   *
   * The objects are closed in the order provided.
   */
  public static void closeAll(
      final List<ReadableFrameChannel> readableFrameChannels,
      final List<WritableFrameChannel> writableFrameChannels,
      final Closeable... otherCloseables
  ) throws IOException
  {
    final Closer closer = Closer.create();

    // Add everything to the Closer in reverse order, because the Closer closes in reverse order.

    for (Closeable closeable : Lists.reverse(Arrays.asList(otherCloseables))) {
      if (closeable != null) {
        closer.register(closeable);
      }
    }

    for (WritableFrameChannel channel : Lists.reverse(writableFrameChannels)) {
      closer.register(channel::close);
    }

    for (ReadableFrameChannel channel : Lists.reverse(readableFrameChannels)) {
      closer.register(channel::close);
    }

    closer.close();
  }
}
