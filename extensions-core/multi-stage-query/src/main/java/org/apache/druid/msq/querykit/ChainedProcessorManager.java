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

package org.apache.druid.msq.querykit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.manager.ProcessorAndCallback;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Function;

/**
 * Manager that chains processors: runs {@link #first} first, then based on its result, creates {@link #restFuture}
 * using {@link #restFactory} and runs that next.
 */
public class ChainedProcessorManager<A, B, R> implements ProcessorManager<Object, R>
{
  /**
   * First processor. This one blocks all the others. The reference is set to null once the processor has been
   * returned by the channel.
   */
  @Nullable
  private FrameProcessor<A> first;

  /**
   * Produces {@link #restFuture}.
   */
  private final Function<A, ProcessorManager<B, R>> restFactory;

  /**
   * The rest of the processors. Produced by {@link #restFactory} once {@link #first} has completed.
   */
  private final SettableFuture<ProcessorManager<B, R>> restFuture = SettableFuture.create();

  /**
   * Whether {@link #close()} has been called.
   */
  private boolean closed;

  public ChainedProcessorManager(
      final FrameProcessor<A> first,
      final Function<A, ProcessorManager<B, R>> restFactory
  )
  {
    this.first = Preconditions.checkNotNull(first, "first");
    this.restFactory = Preconditions.checkNotNull(restFactory, "restFactory");
  }

  @Override
  public ListenableFuture<Optional<ProcessorAndCallback<Object>>> next()
  {
    if (closed) {
      throw new IllegalStateException();
    } else if (first != null) {
      //noinspection unchecked
      final FrameProcessor<Object> tmp = (FrameProcessor<Object>) first;
      first = null;
      return Futures.immediateFuture(Optional.of(new ProcessorAndCallback<>(tmp, this::onFirstProcessorComplete)));
    } else {
      return FutureUtils.transformAsync(
          restFuture,
          rest -> (ListenableFuture) rest.next()
      );
    }
  }

  private void onFirstProcessorComplete(final Object firstResult)
  {
    //noinspection unchecked
    restFuture.set(restFactory.apply((A) firstResult));
  }

  @Override
  public R result()
  {
    return FutureUtils.getUncheckedImmediately(restFuture).result();
  }

  @Override
  public void close()
  {
    if (!closed) {
      closed = true;
      CloseableUtils.closeAndWrapExceptions(() -> CloseableUtils.closeAll(
          first != null ? first::cleanup : null,
          restFuture.isDone() ? FutureUtils.getUnchecked(restFuture, false) : null
      ));
    }
  }
}
