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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.ISE;

import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Processor manager that wraps another {@link ProcessorManager} and accumulates a result.
 */
public class AccumulatingProcessorManager<T, R> implements ProcessorManager<T, R>
{
  private final ProcessorManager<T, ?> delegate;
  private final BiFunction<R, T, R> accumulateFn;
  private R currentResult;

  public AccumulatingProcessorManager(
      ProcessorManager<T, ?> delegate,
      R initialResult,
      BiFunction<R, T, R> accumulateFn
  )
  {
    this.delegate = delegate;
    this.currentResult = Preconditions.checkNotNull(initialResult, "initialResult");
    this.accumulateFn = accumulateFn;
  }

  @Override
  public ListenableFuture<Optional<ProcessorAndCallback<T>>> next()
  {
    if (currentResult == null) {
      throw new ISE("Closed");
    }

    return FutureUtils.transform(
        delegate.next(),
        nextProcessor -> nextProcessor.map(
            retVal -> new ProcessorAndCallback<>(
                retVal.processor(),
                r -> {
                  currentResult = accumulateFn.apply(currentResult, r);
                  retVal.onComplete(r);
                }
            )
        )
    );
  }

  @Override
  public R result()
  {
    return Preconditions.checkNotNull(currentResult, "currentResult");
  }

  @Override
  public void close()
  {
    currentResult = null;
    delegate.close();
  }
}
