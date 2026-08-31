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
 * <p>To produce a resource, generally you should create and populate {@link SettableAsyncResource}.
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
 * <p>AsyncResource handles this problem by automatically closing the resource in
 * {@link SettableAsyncResource#set(ResourceHolder)} when the {@link SettableAsyncResource} has been canceled.
 */
public interface AsyncResource<T> extends Closeable
{
  /**
   * Whether resource acquisition has completed (successfully or with failure). To wait for this to become true
   * asynchronously, use {@link #addReadyCallback(Runnable)}. To block until readiness, use {@link #await()}
   * or {@link #await(long)}.
   */
  boolean isReady();

  /**
   * Register a callback to fire when {@link #isReady()} becomes true (whether the load succeeded or failed). If the
   * holder is already ready, the callback fires immediately in the calling thread. Callbacks are not fired if
   * {@link #close()} is called prior to the resource becoming available.
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
   * <p>Throws {@link DruidException} if the underlying object is not ready or if {@link #close()} has been called.
   * Also throws an exception if the resource acquisition failed.
   */
  T get();

  /**
   * Block until {@link #isReady()} returns true. Does not close the resource if interrupted; callers must still
   * call {@link #close()}.
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
   * If acquisition is still in progress, it is canceled if possible.
   *
   * <p>Despite {@link Closeable} requiring this method to be idempotent, it is not necessarily
   * going to be idempotent. Do not close more than once.
   */
  @Override
  void close();
}
