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

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class GroupByMergedQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(GroupByMergedQueryRunner.class);
  private final Iterable<QueryRunner<T>> queryables;
  private final ListeningExecutorService exec;
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final QueryWatcher queryWatcher;
  private final NonBlockingPool<ByteBuffer> bufferPool;

  public GroupByMergedQueryRunner(
      ExecutorService exec,
      Supplier<GroupByQueryConfig> configSupplier,
      QueryWatcher queryWatcher,
      NonBlockingPool<ByteBuffer> bufferPool,
      Iterable<QueryRunner<T>> queryables
  )
  {
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.configSupplier = configSupplier;
    this.bufferPool = bufferPool;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryPlus.getQuery();
    final GroupByQueryConfig querySpecificConfig = configSupplier.get().withOverrides(query);
    final boolean isSingleThreaded = querySpecificConfig.isSingleThreaded();
    final Pair<IncrementalIndex, Accumulator<IncrementalIndex, T>> indexAccumulatorPair = GroupByQueryHelper.createIndexAccumulatorPair(
        query,
        null,
        querySpecificConfig,
        bufferPool
    );
    final Pair<Queue, Accumulator<Queue, T>> bySegmentAccumulatorPair = GroupByQueryHelper.createBySegmentAccumulatorPair();
    final boolean bySegment = QueryContexts.isBySegment(query);
    final int priority = QueryContexts.getPriority(query);
    final QueryPlus<T> threadSafeQueryPlus = queryPlus.withoutThreadUnsafeState();
    final List<ListenableFuture<Void>> futures =
        Lists.newArrayList(
            Iterables.transform(
                queryables,
                new Function<QueryRunner<T>, ListenableFuture<Void>>()
                {
                  @Override
                  public ListenableFuture<Void> apply(final QueryRunner<T> input)
                  {
                    if (input == null) {
                      throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
                    }

                    ListenableFuture<Void> future = exec.submit(
                        new AbstractPrioritizedCallable<Void>(priority)
                        {
                          @Override
                          public Void call()
                          {
                            try {
                              if (bySegment) {
                                input.run(threadSafeQueryPlus, responseContext)
                                     .accumulate(bySegmentAccumulatorPair.lhs, bySegmentAccumulatorPair.rhs);
                              } else {
                                input.run(threadSafeQueryPlus, responseContext)
                                     .accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
                              }

                              return null;
                            }
                            catch (QueryInterruptedException e) {
                              throw new RuntimeException(e);
                            }
                            catch (Exception e) {
                              log.error(e, "Exception with one of the sequences!");
                              Throwables.propagateIfPossible(e);
                              throw new RuntimeException(e);
                            }
                          }
                        }
                    );

                    if (isSingleThreaded) {
                      waitForFutureCompletion(query, ImmutableList.of(future), indexAccumulatorPair.lhs);
                    }

                    return future;
                  }
                }
            )
        );

    if (!isSingleThreaded) {
      waitForFutureCompletion(query, futures, indexAccumulatorPair.lhs);
    }

    if (bySegment) {
      return Sequences.simple(bySegmentAccumulatorPair.lhs);
    }

    return Sequences.withBaggage(
        Sequences.simple(
            Iterables.transform(
                indexAccumulatorPair.lhs.iterableWithPostAggregations(null, query.isDescending()),
                new Function<Row, T>()
                {
                  @Override
                  public T apply(Row input)
                  {
                    return (T) input;
                  }
                }
            )
        ), indexAccumulatorPair.lhs
    );
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      List<ListenableFuture<Void>> futures,
      IncrementalIndex<?> closeOnFailure
  )
  {
    ListenableFuture<List<Void>> future = Futures.allAsList(futures);
    try {
      queryWatcher.registerQueryFuture(query, future);
      if (QueryContexts.hasTimeout(query)) {
        future.get(QueryContexts.getTimeout(query), TimeUnit.MILLISECONDS);
      } else {
        future.get();
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      GuavaUtils.cancelAll(true, future, futures);
      closeOnFailure.close();
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      closeOnFailure.close();
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      closeOnFailure.close();
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
    }
    catch (ExecutionException e) {
      GuavaUtils.cancelAll(true, future, futures);
      closeOnFailure.close();
      Throwables.propagateIfPossible(e.getCause());
      throw new RuntimeException(e.getCause());
    }
  }
}
