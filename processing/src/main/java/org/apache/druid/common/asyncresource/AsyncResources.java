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

import org.apache.druid.collections.ResourceHolder;

import java.io.Closeable;
import java.util.List;
import java.util.function.Function;

/**
 * Utility functions for {@link AsyncResource}.
 */
public class AsyncResources
{
  private AsyncResources()
  {
    // No instantiation.
  }

  /**
   * Returns an {@link AsyncResource} that wraps the underlying object, which does not have a lifecycle.
   */
  public static <T> AsyncResource<T> unmanaged(final T object)
  {
    final SettableAsyncResource<T> retVal = new SettableAsyncResource<>();
    retVal.set(object, null);
    return retVal;
  }

  /**
   * Returns an {@link AsyncResource} that wraps the underlying closeable object.
   */
  public static <T extends Closeable> AsyncResource<T> ofCloseable(final T object)
  {
    final SettableAsyncResource<T> retVal = new SettableAsyncResource<>();
    retVal.set(ResourceHolder.fromCloseable(object));
    return retVal;
  }

  /**
   * Returns an {@link AsyncResource} that collects a list of underlying resources into a single lifecycle.
   * Calling {@link AsyncResource#close()} on the returned async resource causes the underlying async resource
   * to be closed.
   *
   * <p>The transformation generally happens eagerly in the thread that provides the source resource, so it is
   * important that it run quickly.
   *
   * <p>The target of {@code function} need not be {@link Closeable}, and even if it is {@link Closeable}, it
   * is not closed (only the source is closed). This transform utility is meant for transformations that do
   * not introduce new resource lifecycles.
   */
  public static <T, R> AsyncResource<R> transform(
      final AsyncResource<T> sourceResource,
      final Function<T, R> function
  )
  {
    return new TransformAsyncResource<>(sourceResource, function);
  }

  /**
   * Returns an {@link AsyncResource} that collects a list of underlying resources into a single lifecycle.
   * Calling {@link AsyncResource#close()} on the returned async resource causes the underlying async resources
   * to also be closed.
   */
  public static <T> AsyncResource<List<T>> collect(final List<AsyncResource<T>> asyncResources)
  {
    return new CollectAsyncResource<>(asyncResources);
  }

  /**
   * Returns an {@link AsyncResource} that recovers from an exception in {@code sourceResource}.
   *
   * <p>If the source resource suceeds, the recoverFn is not called and the underlying source resource is returned.
   * On the other hand, if the source resource fails, the {@code recoverFn} is called with the exception and is
   * given a chance to substitute a fallback value. Recovery generally happens eagerly in the thread that provides
   * the source resource, so it is important that it run quickly.
   *
   * <p>When recovery happens, the {@code sourceResource} is closed immediately. Otherwise, the {@code sourceResoruce}
   * is closed when the resource returned by this function is closed.
   *
   * <p>The target of {@code function} need not be {@link Closeable}, and even if it is {@link Closeable}, it
   * is not closed (only the source is closed). This transform utility is meant for transformations that do
   * not introduce new resource lifecycles.
   *
   * @param sourceResource the source resource
   * @param recoverFn called when the source resource fails. Returns nonnull to recover, or null to keep the
   *                  error condition as-is.
   */
  public static <T> AsyncResource<T> recover(
      final AsyncResource<T> sourceResource,
      final Function<Throwable, T> recoverFn
  )
  {
    return new RecoverAsyncResource<>(sourceResource, recoverFn);
  }
}
