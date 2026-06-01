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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Closeable wrapper around an asynchronously-loaded {@link CursorHolder}, returned by
 * {@link CursorFactory#makeCursorHolderAsync}. Designed to make lifecycle management safe even when the holder is
 * still loading: callers receive a single Closeable handle and can register it once with their cleanup machinery,
 * regardless of where the underlying load is in its lifecycle.
 * <p>
 * The hazard this exists to avoid: returning a {@code ListenableFuture<CursorHolder>} (or similar future-of-Closeable)
 * makes correct cleanup error-prone, where canceling the future or letting a caller fail before consuming the future
 * can orphan the produced holder, leaking the underlying resources. By exposing a Closeable that internally tracks the
 * load and disposes whatever has materialized, callers don't have to write that bookkeeping themselves.
 * <p>
 * <h3>Producer protocol</h3>
 * Producers feed results in via {@link #set(CursorHolder)} or {@link #setException(Throwable)}, both of which return
 * a boolean. If they return {@code false}, this wrapper has already been closed and the producer is responsible for
 * closing whatever holder it just produced.
 * Producers may pass a {@link Runnable} canceler at construction time which runs on {@link #close()} when the wrapper
 * is closed before the {@link #set} has been called, giving the producer an opportunity to abort its work. The canceler
 * is best-effort: a producer may have already produced the holder by the time it observes cancellation, in which case
 * its {@link #set} call will return false and it must close the holder it tried to set.
 * <p>
 * <h3>Consumer protocol</h3>
 * Consumers wait for {@link #isReady()} via {@link #addReadyCallback}, and {@link #release()} to transfer ownership of
 * the {@link CursorHolder} (or throw the producer exception). Calling {@link #release()} before {@link #isReady()}
 * returns {@code true}, multiple times, or after this holder has been closed will throw a {@link DruidException}.
 * <p>
 * For example (using {@link ReturnOrAwait} to show intended yield-then-resume usage pattern):
 * <pre>{@code
 * if (asyncHolder == null) {
 *     asyncHolder = cursorFactory.makeCursorHolderAsync(spec);
 *     closer.register(asyncHolder);  // safe at any lifecycle point, close() handles in-flight loads
 * }
 * if (!asyncHolder.isReady()) {
 *     SettableFuture<?> awaitFuture = SettableFuture.create();
 *     asyncHolder.addReadyCallback(() -> awaitFuture.set(null));
 *     return ReturnOrAwait.awaitAllFutures(List.of(awaitFuture));
 * }
 * final CursorHolder holder = asyncHolder.release();  // ownership transfers to the caller
 * // ... use holder; close it when done (or hand it to a component that owns its lifecycle) ...
 * }</pre>
 */
public class AsyncCursorHolder implements Closeable
{
  private static final Logger LOG = new Logger(AsyncCursorHolder.class);

  /**
   * Completed {@link AsyncCursorHolder} backed by an already available {@link CursorHolder}
   */
  public static AsyncCursorHolder completed(CursorHolder holder)
  {
    final AsyncCursorHolder result = new AsyncCursorHolder(null);
    result.set(holder);
    return result;
  }

  @Nullable
  private final Runnable canceler;

  @GuardedBy("this")
  @Nullable
  private CursorHolder result = null;
  @GuardedBy("this")
  @Nullable
  private Throwable error = null;
  @GuardedBy("this")
  private boolean closed = false;
  @GuardedBy("this")
  private boolean disposed = false;
  @GuardedBy("this")
  private final List<Runnable> readyCallbacks = new ArrayList<>();

  /**
   * @param canceler optional callback invoked from {@link #close()} when the wrapper is closed before the load has
   *                 completed ({@link #set} or {@link #setException}). Producers that support cancellation should
   *                 provide one; producers that don't can pass {@code null}, in which case {@link #close()} just stops
   *                 observing the result.
   */
  public AsyncCursorHolder(@Nullable Runnable canceler)
  {
    this.canceler = canceler;
  }

  /**
   * Allows producer to mark the load successful with the given holder. Returns {@code true} if accepted, {@code false}
   * if this wrapper has already been closed, in which case the producer is responsible for closing {@link CursorHolder}
   * itself. Throws {@link DruidException} if the load was already completed (from prior calls to this method or
   * {@link #setException}).
   * <p>
   * Callbacks registered via {@link #addReadyCallback} fire outside the lock to avoid lock-ordering deadlocks and
   * unbounded lock holds.
   */
  public boolean set(CursorHolder holder)
  {
    if (holder == null) {
      throw DruidException.defensive("CursorHolder cannot be null");
    }
    return setInternal(Either.value(holder));
  }

  /**
   * Allows producer to mark the load as failed. Returns {@code true} if accepted, {@code false} if this wrapper has
   * already been closed (no holder was produced, so there's nothing for the producer to clean up). Throws
   * {@link DruidException} if the load was already completed (from prior calls to this method or {@link #set}).
   * <p>
   * Callbacks registered via {@link #addReadyCallback} fire outside the lock to avoid lock-ordering deadlocks and
   * unbounded lock holds.
   */
  public boolean setException(Throwable t)
  {
    return setInternal(Either.error(t));
  }

  private boolean setInternal(Either<Throwable, CursorHolder> value)
  {
    final List<Runnable> callbacksToFire;
    synchronized (this) {
      if (closed) {
        return false;
      }
      if (result != null || error != null) {
        throw DruidException.defensive("AsyncCursorHolder is already completed");
      }
      if (value.isError()) {
        error = value.error();
      } else {
        result = value.valueOrThrow();
      }
      callbacksToFire = drainCallbacks();
    }
    fireCallbacks(callbacksToFire);
    return true;
  }

  /**
   * Whether the load has completed (successfully or with failure). Once true, stays true. Callers that need to wait
   * for readiness without blocking the current thread should register a {@link #addReadyCallback} and yield.
   */
  public synchronized boolean isReady()
  {
    return result != null || error != null;
  }

  /**
   * Take ownership of the underlying {@link CursorHolder}. After this returns, {@link #close()} on this
   * {@code AsyncCursorHolder} is a no-op; the caller is responsible for closing the returned holder. Useful when
   * passing the holder to another component (e.g. a cursor-lifecycle manager) that takes ownership of it.
   * <p>
   * Throws {@link DruidException} if the holder is not yet ready, has already been released, or this wrapper
   * has been closed. Wraps and rethrows the failure if the underlying load failed. Does not block; callers must
   * check {@link #isReady()} first (typically by yielding via a {@link #addReadyCallback}-driven wait pattern).
   */
  public synchronized CursorHolder release()
  {
    if (closed) {
      throw DruidException.defensive("AsyncCursorHolder is already closed");
    }
    if (disposed) {
      throw DruidException.defensive("AsyncCursorHolder has already been released");
    }
    if (error != null) {
      // pass through as is
      if (error instanceof RuntimeException runtime) {
        throw runtime;
      } else if (error instanceof Error e) {
        throw e;
      }
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build(error, error.getMessage());
    }
    if (result == null) {
      throw DruidException.defensive("AsyncCursorHolder is not ready yet");
    }
    final CursorHolder resultToReturn = result;
    // clear result so it can be eligible for gc
    result = null;
    disposed = true;
    return resultToReturn;
  }

  /**
   * Register a callback to fire when {@link #isReady()} becomes true (whether the load succeeded or failed). If the
   * holder is already ready, the callback fires synchronously on the calling thread. Otherwise it fires on whatever
   * thread invokes {@link #set} or {@link #setException}, outside the wrapper's lock so the callback may safely
   * re-enter the wrapper. Multiple callbacks may be registered; each fires once.
   */
  public void addReadyCallback(Runnable callback)
  {
    final boolean fireImmediately;
    synchronized (this) {
      if (result != null || error != null) {
        fireImmediately = true;
      } else {
        readyCallbacks.add(callback);
        fireImmediately = false;
      }
    }
    if (fireImmediately) {
      callback.run();
    }
  }

  /**
   * Close the wrapper. Safe at any lifecycle point and idempotent:
   * <ul>
   *   <li>Already-loaded: closes the underlying {@link CursorHolder} immediately.</li>
   *   <li>Loading in progress: invokes the canceler (if one was supplied at construction). The producer may still
   *       call {@link #set} / {@link #setException} after this; if the producer wins the race and calls {@code set}
   *       with a holder, {@code set} returns false and the producer is responsible for closing it.</li>
   *   <li>Load failed: no-op (no holder was produced).</li>
   *   <li>Already released: no-op.</li>
   *   <li>Already closed: throws {@link DruidException}.</li>
   * </ul>
   */
  @Override
  public void close()
  {
    final CursorHolder holderToClose;
    final Runnable cancelerToRun;
    synchronized (this) {
      if (closed) {
        throw DruidException.defensive("Already closed");
      }
      closed = true;
      if (disposed) {
        // Ownership was already transferred via release(); the caller manages the holder lifecycle.
        return;
      }
      if (result != null) {
        // Result is here and no one has released it; we close it.
        disposed = true;
        holderToClose = result;
        cancelerToRun = null;
      } else if (error != null) {
        // Load already failed; nothing to dispose.
        holderToClose = null;
        cancelerToRun = null;
      } else {
        // Load not yet completed; signal cancellation to the producer (if any).
        holderToClose = null;
        cancelerToRun = canceler;
      }
    }
    if (holderToClose != null) {
      try {
        holderToClose.close();
      }
      catch (Throwable ignored) {
        // Best-effort cleanup
      }
    }
    if (cancelerToRun != null) {
      try {
        cancelerToRun.run();
      }
      catch (Throwable t) {
        // Best-effort cancel
        LOG.warn(t, "AsyncCursorHolder canceler exception");
      }
    }
  }

  @GuardedBy("this")
  private List<Runnable> drainCallbacks()
  {
    final List<Runnable> snapshot = List.copyOf(readyCallbacks);
    readyCallbacks.clear();
    return snapshot;
  }

  private static void fireCallbacks(List<Runnable> callbacks)
  {
    for (Runnable cb : callbacks) {
      try {
        cb.run();
      }
      catch (Throwable t) {
        // Best-effort; one bad callback shouldn't break others.
        LOG.warn(t, "AsyncCursorHolder callback exception");
      }
    }
  }
}
