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
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.processor.Bouncer;

import java.util.Optional;

/**
 * Manager that limits the number of processors that may run concurrently.
 */
public class ConcurrencyLimitedProcessorManager<T, R> implements ProcessorManager<T, R>
{
  private final ProcessorManager<T, R> delegate;
  private final Bouncer bouncer;

  public ConcurrencyLimitedProcessorManager(ProcessorManager<T, R> delegate, int limit)
  {
    this.delegate = delegate;
    this.bouncer = new Bouncer(limit);
  }

  @Override
  public ListenableFuture<Optional<ProcessorAndCallback<T>>> next()
  {
    final ListenableFuture<Bouncer.Ticket> ticket = bouncer.ticket();
    return FutureUtils.transformAsync(
        ticket,
        t -> FutureUtils.transform(
            delegate.next(),
            nextProcessor -> nextProcessor.map(
                retVal -> new ProcessorAndCallback<>(
                    retVal.processor(),
                    r -> {
                      FutureUtils.getUncheckedImmediately(ticket).giveBack();
                      retVal.onComplete(r);
                    }
                )
            )
        )
    );
  }

  @Override
  public R result()
  {
    return delegate.result();
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
