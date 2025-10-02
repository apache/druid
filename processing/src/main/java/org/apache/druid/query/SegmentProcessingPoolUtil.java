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

package org.apache.druid.query;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Utility class for standardized query execution across different query runners.
 * Provides common logic for submitting query tasks to processing pools and waiting for results.
 */
public class SegmentProcessingPoolUtil<RunnerType, ResultType>
{
  private static final Logger log = new Logger(SegmentProcessingPoolUtil.class);

  private final QueryProcessingPool queryProcessingPool;
  @Nullable
  private final QueryWatcher queryWatcher;

  /**
   * @param queryProcessingPool The pool to submit tasks to
   * @param queryWatcher        Query watcher for registering query futures
   */
  public SegmentProcessingPoolUtil(
      QueryProcessingPool queryProcessingPool,
      @Nullable QueryWatcher queryWatcher
  )
  {
    this.queryProcessingPool = queryProcessingPool;
    this.queryWatcher = queryWatcher;
  }

  /**
   * Submits query runner tasks to the query segment processing pool and waits for all results to complete.
   * Handles overall/per-segment timeout management, error handling, and future cancellation in a standardized way.
   *
   * @param queryables       QueryRunners to execute
   * @param queryPlus        The query plus object
   * @param taskFactory      Function to create result from each QueryRunner
   * @param isSingleThreaded Whether to execute tasks sequentially (true) or in parallel (false)
   * @param timeoutAt        Absolute deadline timestamp in milliseconds
   * @return Iterable of results from all completed tasks
   * @throws QueryInterruptedException if the query is interrupted
   * @throws QueryTimeoutException     if the query times out
   * @throws RuntimeException          for other execution errors
   */
  public List<ResultType> submitRunnersAndWaitForResults(
      Iterable<QueryRunner<RunnerType>> queryables,
      QueryPlus<RunnerType> queryPlus,
      Function<QueryRunner<RunnerType>, ResultType> taskFactory,
      boolean isSingleThreaded,
      long timeoutAt
  )
  {
    final Query<RunnerType> query = queryPlus.getQuery();
    final QueryContext context = query.context();
    final int priority = context.getPriority();

    if (isSingleThreaded) {
      List<ResultType> results = new ArrayList<>();
      for (QueryRunner<RunnerType> input : queryables) {
        if (input == null) {
          throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
        }
        ListenableFuture<ResultType> future = submitRunnerTask(query, input, taskFactory, priority, timeoutAt);
        results.addAll(waitForCompletion(query, List.of(future), timeoutAt));
      }
      return results;
    } else {
      List<ListenableFuture<ResultType>> futures = new ArrayList<>();
      for (QueryRunner<RunnerType> input : queryables) {
        if (input == null) {
          throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
        }
        ListenableFuture<ResultType> future = submitRunnerTask(query, input, taskFactory, priority, timeoutAt);
        futures.add(future);
      }
      return waitForCompletion(query, futures, timeoutAt);
    }
  }


  private List<ResultType> waitForCompletion(
      Query<?> query,
      List<ListenableFuture<ResultType>> futures,
      long timeoutAt
  )
  {
    final ListenableFuture<List<ResultType>> combinedFuture = Futures.allAsList(futures);

    try {
      if (queryWatcher != null) {
        queryWatcher.registerQueryFuture(query, combinedFuture);
      }

      final QueryContext context = query.context();
      boolean hasTimeout = context.hasTimeout();
      long timeout = timeoutAt - System.currentTimeMillis();

      if (hasTimeout && timeout <= 0) {
        throw new QueryTimeoutException();
      }

      return context.hasTimeout() ? combinedFuture.get(
          timeout,
          TimeUnit.MILLISECONDS
      ) : combinedFuture.get();
    }
    catch (CancellationException | InterruptedException e) {
      log.noStackTrace().warn(e, "Query interrupted, cancelling pending results for query[%s]", query.getId());
      GuavaUtils.cancelAll(true, combinedFuture, futures);
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException | QueryTimeoutException e) {
      log.noStackTrace().warn(e, "Query timed out, cancelling pending results for query[%s]", query.getId());
      GuavaUtils.cancelAll(true, combinedFuture, futures);
      throw new QueryTimeoutException("Query[%] timed out", query.getId());
    }
    catch (ExecutionException e) {
      log.noStackTrace().warn(e, "Query error, cancelling pending results for query[%s]", query.getId());
      GuavaUtils.cancelAll(true, combinedFuture, futures);

      Throwable cause = Throwables.getRootCause(e);
      // nested per-segment futures
      if (cause instanceof TimeoutException) {
        throw new QueryTimeoutException("Query[%] timed out", query.getId());
      }
      Throwables.throwIfUnchecked(cause);
      throw new RuntimeException(cause);
    }
  }

  private ListenableFuture<ResultType> submitRunnerTask(
      Query<RunnerType> query,
      QueryRunner<RunnerType> input,
      Function<QueryRunner<RunnerType>, ResultType> taskFactory,
      int priority,
      long timeoutAt
  )
  {
    final QueryContext context = query.context();
    final long perSegmentTimeout = context.getPerSegmentTimeout();
    final boolean hasPerSegmentTimeout = context.hasPerSegmentTimeout();

    final AbstractPrioritizedQueryRunnerCallable callable = new AbstractPrioritizedQueryRunnerCallable<>(
        priority,
        input
    )
    {
      @Override
      public ResultType call()
      {
        return taskFactory.apply(input);
      }
    };

    if (hasPerSegmentTimeout) {
      return queryProcessingPool.submitRunnerTask(
          callable,
          Math.min(perSegmentTimeout, Math.max(0, timeoutAt - System.currentTimeMillis())),
          TimeUnit.MILLISECONDS
      );
    } else {
      return queryProcessingPool.submitRunnerTask(callable);
    }
  }
}
