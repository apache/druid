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

package org.apache.druid.frame.processor.manager;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.guava.Sequences;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class SequenceProcessorManagerTest
{
  @Test
  public void test_empty() throws Exception
  {
    final AtomicLong closed = new AtomicLong();

    try (final SequenceProcessorManager<Object, FrameProcessor<Object>> manager =
             new SequenceProcessorManager<>(
                 Sequences.<FrameProcessor<Object>>empty()
                          .withBaggage(closed::getAndIncrement))) {
      final ListenableFuture<Optional<ProcessorAndCallback<Object>>> future = manager.next();
      Assert.assertTrue(future.isDone());
      Assert.assertFalse(future.get().isPresent());
    }

    Assert.assertEquals(1, closed.get());
  }

  @Test
  public void test_one() throws Exception
  {
    final NilFrameProcessor processor = new NilFrameProcessor();
    final AtomicLong closed = new AtomicLong();

    try (final SequenceProcessorManager<Unit, FrameProcessor<Unit>> manager =
             new SequenceProcessorManager<>(
                 Sequences.<FrameProcessor<Unit>>simple(Collections.singleton(processor))
                          .withBaggage(closed::getAndIncrement))) {
      // First element.
      ListenableFuture<Optional<ProcessorAndCallback<Unit>>> future = manager.next();
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.get().isPresent());
      Assert.assertSame(processor, future.get().get().processor());

      // End of sequence.
      future = manager.next();
      Assert.assertTrue(future.isDone());
      Assert.assertFalse(future.get().isPresent());
    }

    Assert.assertEquals(1, closed.get());
  }

  @Test
  public void test_two() throws Exception
  {
    final NilFrameProcessor processor0 = new NilFrameProcessor();
    final NilFrameProcessor processor1 = new NilFrameProcessor();
    final AtomicLong closed = new AtomicLong();

    try (final SequenceProcessorManager<Unit, FrameProcessor<Unit>> manager =
             new SequenceProcessorManager<>(
                 Sequences.<FrameProcessor<Unit>>simple(ImmutableList.of(processor0, processor1))
                          .withBaggage(closed::getAndIncrement))) {
      // First element.
      ListenableFuture<Optional<ProcessorAndCallback<Unit>>> future = manager.next();
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.get().isPresent());
      Assert.assertSame(processor0, future.get().get().processor());

      // Second element.
      future = manager.next();
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.get().isPresent());
      Assert.assertSame(processor1, future.get().get().processor());

      // End of sequence.
      future = manager.next();
      Assert.assertTrue(future.isDone());
      Assert.assertFalse(future.get().isPresent());

      // One more, should throw because there's nothing left.
      Assert.assertThrows(
          NoSuchElementException.class,
          manager::next
      );
    }

    Assert.assertEquals(1, closed.get());
  }

  @Test
  public void test_empty_closeThenNext()
  {
    final AtomicLong closed = new AtomicLong();

    final SequenceProcessorManager<Object, FrameProcessor<Object>> manager =
        new SequenceProcessorManager<>(
            Sequences.<FrameProcessor<Object>>empty()
                     .withBaggage(closed::getAndIncrement));
    manager.close();

    // IllegalStateException instead of NoSuchElementException because the problem is that we are closed.
    Assert.assertThrows(
        IllegalStateException.class,
        manager::next
    );

    // Sequence is not closed because it never started iterating.
    Assert.assertEquals(0, closed.get());
  }

  public static class NilFrameProcessor<T> implements FrameProcessor<T>
  {
    @Override
    public List<ReadableFrameChannel> inputChannels()
    {
      return Collections.emptyList();
    }

    @Override
    public List<WritableFrameChannel> outputChannels()
    {
      return Collections.emptyList();
    }

    @Override
    public ReturnOrAwait<T> runIncrementally(IntSet readableInputs)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup()
    {
      // Do nothing.
    }
  }
}
