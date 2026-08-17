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

package org.apache.druid.common.asyncresource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RecoverAsyncResourceTest
{
  @Test
  public void testSourceSuccessPassesThroughAndDoesNotRecover()
  {
    final TrackingAsyncResource<String> source = new TrackingAsyncResource<>();
    final AtomicInteger recoveryCalls = new AtomicInteger();

    final AsyncResource<String> recovering = AsyncResources.recover(
        source,
        e -> {
          recoveryCalls.incrementAndGet();
          return "fallback";
        }
    );

    source.delegate.set("value", null);

    Assertions.assertTrue(recovering.isReady());
    Assertions.assertEquals("value", recovering.get());
    Assertions.assertEquals(0, recoveryCalls.get(), "recovery must not be called on success");
    Assertions.assertEquals(0, source.closeCount.get(), "source must not be closed before close()");

    recovering.close();
    Assertions.assertEquals(1, source.closeCount.get(), "close() must close the source");
  }

  @Test
  public void testSourceFailureRecoversWithFallbackValue()
  {
    final TrackingAsyncResource<String> source = new TrackingAsyncResource<>();
    final AtomicReference<Throwable> recoveryArg = new AtomicReference<>();

    final AsyncResource<String> recovering = AsyncResources.recover(
        source,
        e -> {
          recoveryArg.set(e);
          return "fallback";
        }
    );

    final RuntimeException failure = new RuntimeException("boom");
    source.delegate.setException(failure);

    Assertions.assertTrue(recovering.isReady());
    Assertions.assertEquals("fallback", recovering.get());
    Assertions.assertSame(failure, recoveryArg.get(), "recovery receives the source's error");
    Assertions.assertEquals(
        1,
        source.closeCount.get(),
        "source must be closed eagerly during recovery, before the wrapper is closed"
    );

    // Closing the wrapper does not double-close the source.
    recovering.close();
    Assertions.assertEquals(1, source.closeCount.get());
  }

  @Test
  public void testSourceFailureWithNullRecoveryPropagatesError()
  {
    final TrackingAsyncResource<String> source = new TrackingAsyncResource<>();

    final AsyncResource<String> recovering = AsyncResources.recover(
        source,
        e -> null
    );

    final RuntimeException failure = new RuntimeException("boom");
    source.delegate.setException(failure);

    Assertions.assertTrue(recovering.isReady());
    final RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, recovering::get);
    Assertions.assertSame(failure, thrown);
    Assertions.assertEquals(0, source.closeCount.get(), "source must not be closed until close() when not recovering");

    recovering.close();
    Assertions.assertEquals(1, source.closeCount.get());
  }

  @Test
  public void testRecoveryThrowsPropagatesWithOriginalSuppressed()
  {
    final TrackingAsyncResource<String> source = new TrackingAsyncResource<>();
    final RuntimeException recoveryFailure = new RuntimeException("recovery failed");

    final AsyncResource<String> recovering = AsyncResources.recover(
        source,
        e -> {
          throw recoveryFailure;
        }
    );

    final RuntimeException original = new RuntimeException("boom");
    source.delegate.setException(original);

    Assertions.assertTrue(recovering.isReady());
    final RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, recovering::get);
    Assertions.assertSame(original, thrown);
    Assertions.assertEquals(1, thrown.getSuppressed().length);
    Assertions.assertSame(recoveryFailure, thrown.getSuppressed()[0]);
  }

  @Test
  public void testCloseBeforeReadyClosesSource()
  {
    final TrackingAsyncResource<String> source = new TrackingAsyncResource<>();

    final AsyncResource<String> recovering = AsyncResources.recover(
        source,
        e -> "fallback"
    );

    // Close the wrapper while the source is still pending.
    recovering.close();
    Assertions.assertEquals(1, source.closeCount.get());
    Assertions.assertFalse(recovering.isReady());

    // Completing the source after close is a no-op and must not throw.
    Assertions.assertFalse(source.delegate.set("late", null));
  }

  /**
   * An {@link AsyncResource} that delegates to a {@link SettableAsyncResource} and counts {@link #close()} calls,
   * so tests can verify how the source resource's lifecycle is managed.
   */
  private static class TrackingAsyncResource<T> implements AsyncResource<T>
  {
    private final SettableAsyncResource<T> delegate = new SettableAsyncResource<>();
    private final AtomicInteger closeCount = new AtomicInteger();

    @Override
    public boolean isReady()
    {
      return delegate.isReady();
    }

    @Override
    public void addReadyCallback(Runnable callback)
    {
      delegate.addReadyCallback(callback);
    }

    @Override
    public T get()
    {
      return delegate.get();
    }

    @Override
    public void close()
    {
      closeCount.incrementAndGet();
      delegate.close();
    }
  }
}
