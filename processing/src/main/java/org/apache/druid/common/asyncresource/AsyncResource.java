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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Represents a resource that requires some cleanup and that is acquired asynchronously. This class can in principle
 * be used for resources that do not need cleanup or are not acquired asynchronously, but it is most useful when
 * both are true. The wrapper generally owns the resource lifecycle; see "to consume a resource" below for details.
 *
 * <p>To produce a resource, generally you should create and populate {@link SettableAsyncResource}: complete it
 * exactly once with {@link SettableAsyncResource#set} or {@link SettableAsyncResource#setException}, and leave
 * {@link #close()} to the consumer, which owns the resource and may close it to cancel acquisition early.
 *
 * <p>To consume a resource, use {@link #addReadyCallback(Runnable)}, {@link #await()}, or {@link #await(long)}
 * to wait for the resource to become ready. Then use {@link #get()} to retrieve the resource. When you are done
 * with the resource, call {@link #close()} on the {@code AsyncResource} object to close it. Do *not* close the
 * resource {@code T} itself, even if it is {@link Closeable}, as this may lead to a double-close. The one
 * exception to this rule is if you call {@link SettableAsyncResource#release()}: in this case you actually
 * *must* call close on the resource {@code T} itself.
 *
 * <h3>Why not use Futures?</h3>
 * Often {@link Future} or {@link ListenableFuture} are used for objects that are created asynchronously.
 * These are, however, problematic when the object is a resource that requires cleanup. The biggest issue is
 * handling cancellation. As soon as a caller gets a {@code Future<Closeable>}, it becomes responsible for
 * closing the resource once it is created. If the caller is a query that could itself be canceled, it must
 * still arrange for the resource to be closed.
 *
 * <p>The caller can do something like this to deal with it:
 *
 * <pre>
 * // Upon query cancellation, attach a callback to the future that closes the resource once it becomes available.
 * Futures.addCallback(
 *   resourceFuture,
 *   new FutureCallback<>() {
 *     void onSuccess(Closeable resource) { resource.close(); }
 *     void onFailure(Throwable t) { }
 *   }
 * );
 * </pre>
 *
 * But this is awkward, and doesn't allow resource acquisition to actually be canceled. Canceling the future isn't
 * reliable, because it can lead to an orphaned resource: the asynchronous acquisition can complete in a race with
 * cancellation, and in this case, the resource becomes eligible for GC without completing the future and therefore
 * without being closed.
 *
 * <p>AsyncResource handles this problem by reporting the race to the producer instead of dropping the resource: once
 * this {@link AsyncResource} has been closed, {@link SettableAsyncResource#set(ResourceHolder)} becomes a no-op and
 * returns false, which tells the producer that the object it was handing over is orphaned and that closing it is now
 * the producer's job. Acquisition can also be canceled on the producer side, via
 * {@link SettableAsyncResource#setCanceler(Runnable)}.
 */
public interface AsyncResource<T> extends Closeable
{
  /**
   * Whether resource acquisition is no longer in progress, i.e. it succeeded, failed, was canceled by
   * {@link #close()}, or was released by {@link SettableAsyncResource#release()}. Never goes back to false once true;
   * use {@link #get()} to find out which of those happened. To wait for this to become true asynchronously, use
   * {@link #addReadyCallback(Runnable)}. To block until readiness, use {@link #await()} or {@link #await(long)}.
   */
  boolean isReady();

  /**
   * Register a callback to fire when {@link #isReady()} becomes true (whether the load succeeded or failed). If the
   * holder is already ready, the callback fires immediately in the calling thread. Callbacks also fire when
   * {@link #close()} cancels acquisition before the resource became available, so that a waiting consumer learns it
   * was aborted; {@link #get()} then throws {@link AsyncResourceCanceledException}.
   *
   * <p>Firing on close looks redundant, since the same owner both registers the callbacks and does the closing, but it
   * lets that owner cancel itself in one call: when a callback completes something downstream, such as a future
   * holding a query's result, closing the resource runs the callback, which handles
   * {@link AsyncResourceCanceledException} from {@link #get()} and unwinds the waiter too, with no separate
   * cancellation step.
   *
   * <p>Because of the fires-immediately case, the callback can run on the REGISTERING thread, not just on whatever
   * thread completes the resource, so a callback must not do blocking or expensive work (I/O, deserialization)
   * unless the registering thread can tolerate it; hand such work to an executor from inside the callback instead.
   *
   * <p>Throws {@link DruidException} if {@link #close()} has been called prior to this method.
   */
  void addReadyCallback(Runnable callback);

  /**
   * Retrieve the underlying object. May be called any number of times, and the same object will be returned.
   *
   * <p>Throws {@link AsyncResourceCanceledException} if {@link #close()} canceled acquisition before it completed, and
   * {@link DruidException} if the underlying object is not ready or was closed after becoming ready. Also throws an
   * exception if the resource acquisition failed.
   */
  T get();

  /**
   * Block until {@link #isReady()} returns true. Does not close the resource if interrupted; callers must still
   * call {@link #close()}.
   *
   * <p>A {@link #close()} that cancels acquisition wakes the waiter, which then throws
   * {@link AsyncResourceCanceledException}.
   *
   * <p>Throws {@link DruidException} if {@link #close()} has been called prior to this method.
   */
  default T await() throws InterruptedException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    addReadyCallback(latch::countDown);
    latch.await();
    return get();
  }

  /**
   * Block until {@link #isReady()} returns true, up to some timeout. Does not close the resource if interrupted
   * or if waiting times out; callers must still call {@link #close()}.
   *
   * <p>A {@link #close()} that cancels acquisition wakes the waiter, which then throws
   * {@link AsyncResourceCanceledException}.
   *
   * <p>Throws {@link DruidException} if {@link #close()} has been called prior to this method.
   */
  default T await(long timeoutMillis) throws InterruptedException, TimeoutException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    addReadyCallback(latch::countDown);
    if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException();
    }
    return get();
  }

  /**
   * Closes the resource if it is ready, and has not been released by {@link SettableAsyncResource#release()}.
   * If acquisition is still in progress, it is canceled if possible, and any pending
   * {@link #addReadyCallback(Runnable)} callbacks fire so that waiting consumers learn acquisition was aborted
   * instead of waiting for a completion that will never come. {@link #isReady()} then returns true and
   * {@link #get()} throws {@link AsyncResourceCanceledException}.
   *
   * <p>Only the owner of this resource (the consumer) should call this method.
   *
   * <p>Despite {@link Closeable} requiring this method to be idempotent, it is not necessarily
   * going to be idempotent. Do not close more than once.
   */
  @Override
  void close();
}
