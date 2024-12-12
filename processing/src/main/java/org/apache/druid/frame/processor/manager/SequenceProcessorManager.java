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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.utils.CloseableUtils;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Processor manager based on a {@link Sequence}. Returns the number of processors run.
 */
public class SequenceProcessorManager<T, P extends FrameProcessor<T>> implements ProcessorManager<T, Long>
{
  private final Sequence<P> sequence;
  private Yielder<P> yielder;
  private boolean done;
  private long numProcessors;

  SequenceProcessorManager(final Sequence<P> sequence)
  {
    this.sequence = sequence;
  }

  @Override
  public ListenableFuture<Optional<ProcessorAndCallback<T>>> next()
  {
    initializeYielderIfNeeded();

    if (done) {
      if (yielder == null) {
        // Already closed.
        throw new IllegalStateException();
      } else {
        // Not closed yet, but done.
        throw new NoSuchElementException();
      }
    } else if (yielder.isDone()) {
      done = true;
      return Futures.immediateFuture(Optional.empty());
    } else {
      final P retVal;
      try {
        retVal = Preconditions.checkNotNull(yielder.get(), "processor");
        yielder = yielder.next(null);
      }
      catch (Throwable e) {
        // Some problem with yielder.get() or yielder.next(null). Close the yielder and mark us as done.
        done = true;
        CloseableUtils.closeAndSuppressExceptions(yielder, e::addSuppressed);
        yielder = null;
        throw e;
      }

      return Futures.immediateFuture(Optional.of(new ProcessorAndCallback<>(retVal, r -> numProcessors++)));
    }
  }

  @Override
  public Long result()
  {
    return numProcessors;
  }

  @Override
  public void close()
  {
    done = true;

    if (yielder != null) {
      // Clean up.
      CloseableUtils.closeAndWrapExceptions(yielder);
      yielder = null;
    }
  }

  /**
   * Initialize {@link #yielder} if needed.
   */
  private void initializeYielderIfNeeded()
  {
    if (!done && yielder == null) {
      yielder = Yielders.each(sequence);
    }
  }
}
