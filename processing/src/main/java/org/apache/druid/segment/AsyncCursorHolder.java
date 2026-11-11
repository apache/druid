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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.asyncresource.SettableAsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * Closeable wrapper around an asynchronously-loaded {@link CursorHolder}, returned by
 * {@link CursorFactory#makeCursorHolderAsync}. Designed to make lifecycle management safe even when the holder is
 * still loading: callers receive a single Closeable handle and can register it once with their cleanup machinery,
 * regardless of where the underlying load is in its lifecycle.
 *
 * <h3>Consumer protocol</h3>
 * Consumers wait for {@link #isReady()} via {@link #addReadyCallback}, and {@link #release()} to transfer ownership of
 * the {@link CursorHolder} (or throw the producer exception). Calling {@link #release()} before {@link #isReady()}
 * returns {@code true}, multiple times, or after this holder has been closed will throw a {@link DruidException}. If
 * {@link #close()} canceled the load before it completed, {@link #isReady()} becomes true and {@link #release()}
 * throws {@link org.apache.druid.common.asyncresource.AsyncResourceCanceledException} instead, so a waiting consumer
 * always finds out.
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
public class AsyncCursorHolder extends SettableAsyncResource<CursorHolder>
{
  /**
   * @param canceler optional callback invoked from {@link #close()} when the wrapper is closed before the load has
   *                 completed ({@link #set} or {@link #setException}). Producers that support cancellation should
   *                 provide one; producers that don't can pass {@code null}, in which case {@link #close()} just stops
   *                 observing the result.
   */
  public AsyncCursorHolder(@Nullable Runnable canceler)
  {
    super(true);
    setCanceler(canceler);
  }

  /**
   * Convenience setter.
   */
  public boolean set(CursorHolder cursorHolder)
  {
    return super.set(cursorHolder, cursorHolder);
  }

  @Override // Overridden to change access from protected to public
  public synchronized CursorHolder release()
  {
    return super.release();
  }

  /**
   * Returns an {@link AsyncCursorHolder} transformed according to a given function.
   *
   * <p>The provided function receives an underlying {@link CursorHolder} that is released from this holder.
   * The provided function should return a {@link CursorHolder} that, when closed, also closes its input
   * {@link CursorHolder}. The provided function should, however, not close the input {@link CursorHolder}
   * if the provided function itself throws an exception. (That will be handled by the callback registered by
   * the "transform" method itself.)
   *
   * <p>Once this method returns, the returned holder is the caller's to close, and the caller must not close
   * this instance.
   */
  public AsyncCursorHolder transform(final Function<CursorHolder, CursorHolder> function)
  {
    final AsyncCursorHolder retVal = new AsyncCursorHolder(this::close);
    addReadyCallback(() -> {
      final CursorHolder baseHolder;
      try {
        baseHolder = release();
      }
      catch (Throwable t) {
        retVal.setException(t);
        return;
      }

      final CursorHolder newHolder;
      try {
        newHolder = function.apply(baseHolder);
      }
      catch (Throwable t) {
        CloseableUtils.closeAndSuppressExceptions(baseHolder, t::addSuppressed);
        retVal.setException(t);
        return;
      }

      if (!retVal.set(newHolder)) {
        // The caller closed the returned holder while we were making this one, so close it here instead.
        newHolder.close();
      }
    });
    return retVal;
  }

  /**
   * Completed {@link AsyncCursorHolder} backed by an already available {@link CursorHolder}
   */
  public static AsyncCursorHolder completed(CursorHolder holder)
  {
    final AsyncCursorHolder result = new AsyncCursorHolder(null);
    result.set(ResourceHolder.fromCloseable(holder));
    return result;
  }
}
