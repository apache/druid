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

package org.apache.druid.msq.counters;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.processor.manager.ProcessorAndCallback;
import org.apache.druid.frame.processor.manager.ProcessorManager;

import java.util.Optional;

/**
 * Wrapper around {@link ProcessorManager} that accumulates time taken into a {@link CpuCounter}.
 */
public class CpuTimeAccumulatingProcessorManager<T, R> implements ProcessorManager<T, R>
{
  private final ProcessorManager<T, R> delegate;
  private final CpuCounter counter;

  public CpuTimeAccumulatingProcessorManager(ProcessorManager<T, R> delegate, CpuCounter counter)
  {
    this.delegate = delegate;
    this.counter = counter;
  }

  @Override
  public ListenableFuture<Optional<ProcessorAndCallback<T>>> next()
  {
    // Measure time taken by delegate.next()
    final ListenableFuture<Optional<ProcessorAndCallback<T>>> delegateNext = counter.run(delegate::next);

    return FutureUtils.transform(
        delegateNext,

        // Don't bother measuring time taken by opt.map, it's very quick.
        opt -> opt.map(
            pac -> new ProcessorAndCallback<>(
                new CpuTimeAccumulatingFrameProcessor<>(pac.processor(), counter),
                // Do measure time taken by onComplete(t), though.
                t -> counter.run(() -> pac.onComplete(t))
            )
        )
    );
  }

  @Override
  public R result()
  {
    return counter.run(delegate::result);
  }

  @Override
  public void close()
  {
    counter.run(delegate::close);
  }
}
