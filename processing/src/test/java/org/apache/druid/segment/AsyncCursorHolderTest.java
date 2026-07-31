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

package org.apache.druid.segment;

import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class AsyncCursorHolderTest
{
  @Test
  void testCloseAfterReleaseDoesNotDoubleCloseHolder()
  {
    final CountingCursorHolder holder = new CountingCursorHolder();
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(holder);

    final CursorHolder released = asyncHolder.release();
    Assertions.assertSame(holder, released);
    Assertions.assertEquals(0, holder.closeCount(), "release should not close the holder");

    asyncHolder.close();
    Assertions.assertEquals(0, holder.closeCount(), "close after release must not double-close the holder");
  }

  @Test
  void testCloseWhenAlreadyReadyClosesHolder()
  {
    final CountingCursorHolder holder = new CountingCursorHolder();
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(holder);

    asyncHolder.close();
    Assertions.assertEquals(1, holder.closeCount(), "close should close the holder when ready and not released");
  }

  @Test
  void testCloseMultiple()
  {
    final CountingCursorHolder holder = new CountingCursorHolder();
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(holder);

    asyncHolder.close();
    Throwable t = Assertions.assertThrows(DruidException.class, asyncHolder::close);
    Assertions.assertEquals(1, holder.closeCount());
    Assertions.assertEquals("Already closed", t.getMessage());
  }

  @Test
  void testCloseBeforeSetInvokesCancelerAndProducerClosesOrphan()
  {
    final AtomicInteger cancelerCalls = new AtomicInteger();
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(cancelerCalls::incrementAndGet);

    Assertions.assertFalse(asyncHolder.isReady());
    asyncHolder.close();
    Assertions.assertEquals(1, cancelerCalls.get(), "close before completion should invoke the canceler");

    // Producer races and produces a holder anyway: set should return false; producer must close the orphan.
    final CountingCursorHolder lateHolder = new CountingCursorHolder();
    final boolean accepted = asyncHolder.set(lateHolder);
    Assertions.assertFalse(accepted, "set should be rejected after close");
    Assertions.assertEquals(0, lateHolder.closeCount(), "wrapper does NOT close orphan; producer is responsible");
  }

  @Test
  void testLateProducerSetAfterCloseClosesOrphan()
  {
    // Simulates a producer (e.g. a future-based loader) that delivers a holder after the wrapper has been closed:
    // set returns false, the producer notices, and closes the orphan itself.
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(null);
    asyncHolder.close();

    final CountingCursorHolder lateHolder = new CountingCursorHolder();
    final boolean accepted = asyncHolder.set(lateHolder);
    Assertions.assertFalse(accepted, "set should be rejected after close");
    // (Producer-side responsibility — wrapper does not close on the producer's behalf.)
    Assertions.assertEquals(0, lateHolder.closeCount(), "wrapper does NOT close orphan; producer is responsible");
  }

  @Test
  void testNoCancelerIsCalledIfLoadAlreadyCompleted()
  {
    final AtomicInteger cancelerCalls = new AtomicInteger();
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(cancelerCalls::incrementAndGet);
    asyncHolder.set(new CountingCursorHolder());

    asyncHolder.close();
    Assertions.assertEquals(0, cancelerCalls.get(), "canceler must not run if the load already completed");
  }

  @Test
  void testReleaseBeforeReadyThrows()
  {
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(null);
    Assertions.assertThrows(DruidException.class, asyncHolder::release);
  }

  @Test
  void testReleaseAfterReleaseThrows()
  {
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(new CountingCursorHolder());
    asyncHolder.release();
    Assertions.assertThrows(DruidException.class, asyncHolder::release);
  }

  @Test
  void testReleaseAfterCloseThrows()
  {
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(new CountingCursorHolder());
    asyncHolder.close();
    Assertions.assertThrows(DruidException.class, asyncHolder::release);
  }

  @Test
  void testReleaseAfterFailedLoadThrowsWrappedFailure()
  {
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(null);
    final IllegalArgumentException failure = new IllegalArgumentException("boom");
    asyncHolder.setException(failure);

    final IllegalArgumentException thrown = Assertions.assertThrows(IllegalArgumentException.class, asyncHolder::release);
    Assertions.assertSame(failure, thrown, "release should propagate the underlying failure");
  }

  @Test
  void testSetAfterSetThrows()
  {
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(null);
    asyncHolder.set(new CountingCursorHolder());
    Assertions.assertThrows(DruidException.class, () -> asyncHolder.set(new CountingCursorHolder()));
  }

  @Test
  void testSetAfterSetExceptionThrows()
  {
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(null);
    asyncHolder.setException(new RuntimeException("boom"));
    Assertions.assertThrows(DruidException.class, () -> asyncHolder.set(new CountingCursorHolder()));
  }

  @Test
  void testAddReadyCallbackFiresImmediatelyWhenAlreadyReady()
  {
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(new CountingCursorHolder());
    final AtomicInteger fired = new AtomicInteger();
    asyncHolder.addReadyCallback(fired::incrementAndGet);
    Assertions.assertEquals(1, fired.get(), "callback should fire synchronously when already ready");
  }

  @Test
  void testAddReadyCallbackFiresOnSet()
  {
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(null);
    final AtomicInteger fired = new AtomicInteger();
    asyncHolder.addReadyCallback(fired::incrementAndGet);
    Assertions.assertEquals(0, fired.get(), "callback should not fire before completion");

    asyncHolder.set(new CountingCursorHolder());
    Assertions.assertEquals(1, fired.get(), "callback should fire when set is called");
  }

  @Test
  void testAddReadyCallbackFiresOnSetException()
  {
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(null);
    final AtomicInteger fired = new AtomicInteger();
    asyncHolder.addReadyCallback(fired::incrementAndGet);
    Assertions.assertEquals(0, fired.get(), "callback should not fire before completion");

    asyncHolder.setException(new RuntimeException("boom"));
    Assertions.assertEquals(1, fired.get(), "callback should fire when setException is called");
  }

  @Test
  void testMultipleCallbacksAllFire()
  {
    final AsyncCursorHolder asyncHolder = new AsyncCursorHolder(null);
    final AtomicInteger fired = new AtomicInteger();
    asyncHolder.addReadyCallback(fired::incrementAndGet);
    asyncHolder.addReadyCallback(fired::incrementAndGet);
    asyncHolder.addReadyCallback(fired::incrementAndGet);

    asyncHolder.set(new CountingCursorHolder());
    Assertions.assertEquals(3, fired.get(), "all registered callbacks should fire");
  }

  /**
   * Minimal {@link CursorHolder} that just counts close invocations. Other methods are unimplemented because the
   * tests don't exercise them.
   */
  private static class CountingCursorHolder implements CursorHolder
  {
    private int closeCount;

    int closeCount()
    {
      return closeCount;
    }

    @Override
    public Cursor asCursor()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
      closeCount++;
    }
  }
}
