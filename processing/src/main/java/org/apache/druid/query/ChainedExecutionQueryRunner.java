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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.MergeIterable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A QueryRunner that combines a list of other QueryRunners and executes them in parallel on an executor.
 * <p/>
 * When using this, it is important to make sure that the list of QueryRunners provided is fully flattened.
 * If, for example, you were to pass a list of a Chained QueryRunner (A) and a non-chained QueryRunner (B).  Imagine
 * A has 2 QueryRunner chained together (Aa and Ab), the fact that the Queryables are run in parallel on an
 * executor would mean that the Queryables are actually processed in the order
 * <p/>
 * <pre>A -&gt; B -&gt; Aa -&gt; Ab</pre>
 * <p/>
 * That is, the two sub queryables for A would run *after* B is run, effectively meaning that the results for B
 * must be fully cached in memory before the results for Aa and Ab are computed.
 */
public class ChainedExecutionQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ChainedExecutionQueryRunner.class);

  private final Iterable<QueryRunner<T>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      QueryWatcher queryWatcher,
      QueryRunner<T>... queryables
  )
  {
    this(exec, queryWatcher, Arrays.asList(queryables));
  }

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<T>> queryables
  )
  {
    // listeningDecorator will leave PrioritizedExecutorService unchanged,
    // since it already implements ListeningExecutorService
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryables = Iterables.unmodifiableIterable(queryables);
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    Query<T> query = queryPlus.getQuery();
    final int priority = QueryContexts.getPriority(query);
    final Ordering ordering = query.getResultOrdering();
    final QueryPlus<T> threadSafeQueryPlus = queryPlus.withoutThreadUnsafeState();
    return new BaseSequence<T, Iterator<T>>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            // Make it a List<> to materialize all of the values (so that it will submit everything to the executor)
            List<ListenableFuture<Iterable<T>>> futures =
                Lists.newArrayList(
                    Iterables.transform(
                        queryables,
                        input -> {
                          if (input == null) {
                            throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
                          }

                          return exec.submit(
                              new AbstractPrioritizedCallable<Iterable<T>>(priority)
                              {
                                @Override
                                public Iterable<T> call()
                                {
                                  try {
                                    Sequence<T> result = input.run(threadSafeQueryPlus, responseContext);
                                    if (result == null) {
                                      throw new ISE("Got a null result! Segments are missing!");
                                    }

                                    List<T> retVal = result.toList();
                                    if (retVal == null) {
                                      throw new ISE("Got a null list of results");
                                    }

                                    return retVal;
                                  }
                                  catch (QueryInterruptedException e) {
                                    throw new RuntimeException(e);
                                  }
                                  catch (QueryTimeoutException e) {
                                    throw e;
                                  }
                                  catch (Exception e) {
                                    log.noStackTrace().error(e, "Exception with one of the sequences!");
                                    Throwables.propagateIfPossible(e);
                                    throw new RuntimeException(e);
                                  }
                                }
                              }
                          );
                        }
                    )
                );

            ListenableFuture<List<Iterable<T>>> future = Futures.allAsList(futures);
            queryWatcher.registerQueryFuture(query, future);

            try {
              return new MergeIterable<>(
                  ordering.nullsFirst(),
                  QueryContexts.hasTimeout(query) ?
                      future.get(QueryContexts.getTimeout(query), TimeUnit.MILLISECONDS) :
                      future.get()
              ).iterator();
            }
            catch (InterruptedException e) {
              log.noStackTrace().warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
              //Note: canceling combinedFuture first so that it can complete with INTERRUPTED as its final state. See ChainedExecutionQueryRunnerTest.testQueryTimeout()
              GuavaUtils.cancelAll(true, future, futures);
              throw new QueryInterruptedException(e);
            }
            catch (CancellationException e) {
              throw new QueryInterruptedException(e);
            }
            catch (TimeoutException e) {
              log.warn("Query timeout, cancelling pending results for query id [%s]", query.getId());
              GuavaUtils.cancelAll(true, future, futures);
              throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
            }
            catch (ExecutionException e) {
              GuavaUtils.cancelAll(true, future, futures);
              Throwables.propagateIfPossible(e.getCause());
              throw new RuntimeException(e.getCause());
            }
          }

          @Override
          public void cleanup(Iterator<T> tIterator)
          {

          }
        }
    );
  }
}
