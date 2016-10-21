/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.java.util.common.guava.LazySequence;
import io.druid.java.util.common.guava.Sequence;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncQueryRunner<T> implements QueryRunner<T>
{

  private final QueryRunner<T> baseRunner;
  private final ListeningExecutorService executor;
  private final QueryWatcher queryWatcher;

  public AsyncQueryRunner(QueryRunner<T> baseRunner, ExecutorService executor, QueryWatcher queryWatcher) {
    this.baseRunner = baseRunner;
    this.executor = MoreExecutors.listeningDecorator(executor);
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final int priority = BaseQuery.getContextPriority(query, 0);
    final ListenableFuture<Sequence<T>> future = executor.submit(new AbstractPrioritizedCallable<Sequence<T>>(priority)
        {
          @Override
          public Sequence<T> call() throws Exception
          {
            //Note: this is assumed that baseRunner does most of the work eagerly on call to the
            //run() method and resulting sequence accumulate/yield is fast.
            return baseRunner.run(query, responseContext);
          }
        });
    queryWatcher.registerQuery(query, future);
    
    return new LazySequence<>(new Supplier<Sequence<T>>()
    {
      @Override
      public Sequence<T> get()
      {
        try {
          Number timeout = query.getContextValue(QueryContextKeys.TIMEOUT);
          if (timeout == null) {
            return future.get();
          } else {
            return future.get(timeout.longValue(), TimeUnit.MILLISECONDS);
          }
        } catch (ExecutionException | InterruptedException | TimeoutException ex) {
          throw Throwables.propagate(ex);
        }
      }
    });
  }
}
