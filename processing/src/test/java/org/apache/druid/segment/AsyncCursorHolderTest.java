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

import com.google.common.util.concurrent.SettableFuture;
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
  void testCloseIsIdempotent()
  {
    final CountingCursorHolder holder = new CountingCursorHolder();
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(holder);

    asyncHolder.close();
    asyncHolder.close();
    asyncHolder.close();
    Assertions.assertEquals(1, holder.closeCount(), "repeated close should be idempotent");
  }

  @Test
  void testCloseBeforeFutureCompletesClosesHolderOnArrival()
  {
    final CountingCursorHolder holder = new CountingCursorHolder();
    final SettableFuture<CursorHolder> future = SettableFuture.create();
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.fromFuture(future);

    Assertions.assertFalse(asyncHolder.isReady());
    asyncHolder.close();
    Assertions.assertEquals(0, holder.closeCount(), "no holder yet, nothing to close");

    // Future resolves after close — the registered FutureCallback should close the now-arrived holder.
    future.set(holder);
    Assertions.assertEquals(1, holder.closeCount(), "holder should be closed when it arrives after close()");
  }

  @Test
  void testReleaseBeforeReadyThrows()
  {
    final SettableFuture<CursorHolder> future = SettableFuture.create();
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.fromFuture(future);

    Assertions.assertThrows(IllegalStateException.class, asyncHolder::release);
  }

  @Test
  void testReleaseAfterReleaseThrows()
  {
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(new CountingCursorHolder());
    asyncHolder.release();
    Assertions.assertThrows(IllegalStateException.class, asyncHolder::release);
  }

  @Test
  void testReleaseAfterCloseThrows()
  {
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(new CountingCursorHolder());
    asyncHolder.close();
    Assertions.assertThrows(IllegalStateException.class, asyncHolder::release);
  }

  @Test
  void testAddReadyCallbackFiresWhenAlreadyReady()
  {
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.completed(new CountingCursorHolder());
    final AtomicInteger fired = new AtomicInteger();
    asyncHolder.addReadyCallback(fired::incrementAndGet);
    Assertions.assertEquals(1, fired.get(), "callback should fire synchronously when already ready");
  }

  @Test
  void testAddReadyCallbackFiresOnFutureCompletion()
  {
    final SettableFuture<CursorHolder> future = SettableFuture.create();
    final AsyncCursorHolder asyncHolder = AsyncCursorHolder.fromFuture(future);
    final AtomicInteger fired = new AtomicInteger();
    asyncHolder.addReadyCallback(fired::incrementAndGet);
    Assertions.assertEquals(0, fired.get(), "callback should not fire before the future completes");

    future.set(new CountingCursorHolder());
    Assertions.assertEquals(1, fired.get(), "callback should fire when the future completes");
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
