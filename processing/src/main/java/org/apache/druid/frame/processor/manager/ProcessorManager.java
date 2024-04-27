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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.java.util.common.guava.Sequence;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Used by {@link FrameProcessorExecutor#runAllFully} to manage the launching of processors. Processors returned by
 * this class may run concurrently with each other.
 *
 * This interface allows for simple sequences of processors, such as {@link ProcessorManagers#of(Sequence)}. It also
 * allows for situations where later processors depend on the results of earlier processors. (The result of earlier
 * processors are made available to the manager through {@link ProcessorAndCallback#onComplete(Object)}.)
 *
 * Implementations do not need to be thread-safe.
 *
 * @param <T> return type of {@link org.apache.druid.frame.processor.FrameProcessor} created by this manager
 * @param <R> result type of this manager; see {@link #result()}
 */
public interface ProcessorManager<T, R> extends Closeable
{
  /**
   * Returns the next processor that should be run, along with a callback. The callback is called when the processor
   * completes successfully, along with the result of the processor. If the processor fails, the callback is not called.
   *
   * The callback is called in a thread-safe manner: it will never be called concurrently with another callback, or
   * concurrently with a call to "next" or {@link #close()}. To ensure this, {@link FrameProcessorExecutor#runAllFully}
   * synchronizes executions of callbacks for the same processor manager. Therefore, it is important that the callbacks
   * executed quickly.
   *
   * This method returns a future, so it allows for logic where the construction of later processors depends on the
   * results of earlier processors.
   *
   * Returns an empty Optional if there are no more processors to run.
   *
   * Behavior of this method is undefined if called after {@link #close()}.
   *
   * @throws java.util.NoSuchElementException if a prior call to this method had returned an empty Optional
   */
  ListenableFuture<Optional<ProcessorAndCallback<T>>> next();

  /**
   * Called after all procesors are done, prior to {@link #close()}, to retrieve the result of this computation.
   *
   * Behavior of this method is undefined if called after {@link #close()}, or if called prior to {@link #next()}
   * returning an empty {@link Optional}, or if called prior to all callbacks from {@link #next()} having been called.
   */
  R result();

  /**
   * Called when all processors are done, or when one has failed.
   *
   * This method releases all resources associated with this manager. After calling this method, callers must call no
   * other methods.
   */
  @Override
  void close();

  /**
   * Returns an {@link AccumulatingProcessorManager} that wraps this manager and accumulates a result, to be returned
   * by its {@link #result()} method.
   *
   * @param initialResult initial accumulated value
   * @param accumulateFn  accumulation function, will be provided the current accumulated value and the incremental
   *                      new value.
   * @param <R2>          result type of the returned manager
   */
  default <R2> ProcessorManager<T, R2> withAccumulation(
      R2 initialResult,
      BiFunction<R2, T, R2> accumulateFn
  )
  {
    return new AccumulatingProcessorManager<>(this, initialResult, accumulateFn);
  }
}
