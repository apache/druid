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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.guava.FutureUtils;

import java.io.Closeable;

/**
 * Closeable wrapper around an asynchronously-loaded {@link CursorHolder}, returned by
 * {@link CursorFactory#makeCursorHolderAsync}. Designed to make lifecycle management safe even when the holder is
 * still loading: callers receive a single Closeable handle and can register it once with their cleanup machinery,
 * regardless of where the underlying load is in its lifecycle.
 * <p>
 * The hazard this exists to avoid: returning a {@code ListenableFuture<CursorHolder>} (or similar future-of-Closeable)
 * makes correct cleanup error-prone, where cancelling the future or letting a caller fail before consuming the future
 * can orphan the produced holder, leaking the underlying resources. By exposing a Closeable that internally tracks the
 * load and disposes whatever has materialized, callers don't have to write that bookkeeping themselves.
 * <p>
 * Typical usage from a non-blocking caller (e.g. an MSQ frame processor):
 * <pre>{@code
 * if (asyncHolder == null) {
 *     asyncHolder = cursorFactory.makeCursorHolderAsync(spec);
 *     closer.register(asyncHolder);  // safe at any lifecycle point — close() handles in-flight loads
 * }
 * if (!asyncHolder.isReady()) {
 *     SettableFuture<?> awaitFuture = SettableFuture.create();
 *     asyncHolder.addReadyCallback(() -> awaitFuture.set(null));
 *     return ReturnOrAwait.awaitAllFutures(List.of(awaitFuture));
 * }
 * final CursorHolder holder = asyncHolder.release();  // ownership transfers to the caller
 * // ... use holder; close it when done (or hand it to a component that owns its lifecycle) ...
 * }</pre>
 * The wrapper has no blocking accessor by design: callers must wait for {@link #isReady()} via
 * {@link #addReadyCallback} (so the wait can be expressed as a yield) and then call {@link #release()}.
 */
public class AsyncCursorHolder implements Closeable
{
  /**
   * Already-ready {@link AsyncCursorHolder} backed by an immediately-available {@link CursorHolder}. The default
   * synchronous {@link CursorFactory#makeCursorHolderAsync} implementation uses this.
   */
  public static AsyncCursorHolder completed(CursorHolder holder)
  {
    return new AsyncCursorHolder(Futures.immediateFuture(holder));
  }

  /**
   * Wraps a {@link ListenableFuture}{@code <CursorHolder>}. Implementations doing real I/O should produce the future
   * (e.g. by submitting to an executor) and pass it here.
   */
  public static AsyncCursorHolder fromFuture(ListenableFuture<CursorHolder> future)
  {
    return new AsyncCursorHolder(future);
  }

  private final ListenableFuture<CursorHolder> future;
  private boolean closed = false;
  private boolean disposed = false;

  private AsyncCursorHolder(ListenableFuture<CursorHolder> future)
  {
    this.future = future;
    // If close() lands before the future completes we still need to dispose the eventually-produced holder. Register
    // a callback up-front so the holder is closed whenever it arrives if we've been closed in the meantime.
    Futures.addCallback(
        future,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(CursorHolder holder)
          {
            synchronized (AsyncCursorHolder.this) {
              if (closed && !disposed) {
                disposed = true;
                try {
                  holder.close();
                }
                catch (Throwable ignored) {
                  // Best-effort — we're already in cleanup; nothing to do if close itself fails.
                }
              }
            }
          }

          @Override
          public void onFailure(Throwable t)
          {
            // Nothing to dispose: no holder was produced.
          }
        },
        MoreExecutors.directExecutor()
    );
  }

  /**
   * Whether {@link #release()} can return without blocking. Once true, stays true. Callers that need to wait for
   * readiness without blocking the current thread should register a {@link #addReadyCallback} and yield.
   */
  public boolean isReady()
  {
    return future.isDone();
  }

  /**
   * Take ownership of the underlying {@link CursorHolder}. After this returns, {@link #close()} on this
   * {@code AsyncCursorHolder} is a no-op; the caller is responsible for closing the returned holder. Useful when
   * passing the holder to another component (e.g. a cursor-lifecycle manager) that takes ownership of it.
   * <p>
   * Throws {@link IllegalStateException} if the holder is not yet ready, has already been released, or this wrapper
   * has been closed. Throws if the underlying load failed. Does not block; callers must check {@link #isReady()}
   * first (typically by yielding via a {@link #addReadyCallback}-driven wait pattern).
   */
  public synchronized CursorHolder release()
  {
    if (closed) {
      throw new IllegalStateException("AsyncCursorHolder is already closed");
    }
    if (disposed) {
      throw new IllegalStateException("AsyncCursorHolder has already been released");
    }
    if (!future.isDone()) {
      throw new IllegalStateException("AsyncCursorHolder is not ready yet");
    }
    final CursorHolder holder = FutureUtils.getUncheckedImmediately(future);
    disposed = true;
    return holder;
  }

  /**
   * Register a callback to fire when {@link #isReady()} becomes true (whether the load succeeded or failed). If the
   * holder is already ready, the callback fires synchronously. Multiple callbacks may be registered; each fires once.
   */
  public void addReadyCallback(Runnable callback)
  {
    future.addListener(callback, MoreExecutors.directExecutor());
  }

  /**
   * Close the holder. Safe at any lifecycle point and idempotent:
   * <ul>
   *   <li>Already-loaded: closes the underlying {@link CursorHolder} immediately.</li>
   *   <li>Loading in progress: marks for disposal; the produced holder is closed when the load completes.</li>
   *   <li>Load failed: no-op (nothing to dispose).</li>
   *   <li>Already closed: no-op.</li>
   * </ul>
   * Note that closing does NOT cancel an in-flight load — cancelling a future-of-Closeable is the exact lifecycle
   * hazard this class exists to prevent. The load completes normally and the resulting holder is closed promptly.
   */
  @Override
  public synchronized void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    if (disposed) {
      // Ownership of the holder was already transferred (via release) or already disposed; nothing to do here.
      return;
    }
    if (future.isDone() && !future.isCancelled()) {
      try {
        final CursorHolder holder = FutureUtils.getUncheckedImmediately(future);
        disposed = true;
        holder.close();
      }
      catch (Throwable t) {
        // Future completed with a failure, no holder to close.
      }
    }
    // Otherwise the FutureCallback registered in the constructor will close the holder when the load completes.
  }
}
