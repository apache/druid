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

package io.druid.query;

import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.JodaUtils;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.MergeIterable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

  private final Stream<QueryRunner<T>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      QueryWatcher queryWatcher,
      QueryRunner<T>... queryables
  )
  {
    this(exec, queryWatcher, Arrays.stream(queryables));
  }

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<T>> queryables
  )
  {
    this(exec, queryWatcher, StreamSupport.stream(queryables.spliterator(), false));
  }

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Stream<QueryRunner<T>> queryables
  )
  {
    // listeningDecorator will leave PrioritizedExecutorService unchanged,
    // since it already implements ListeningExecutorService
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.queryables = queryables;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final Map<String, Object> responseContext)
  {
    final Query<T> query = queryPlus.getQuery();
    final int priority = QueryContexts.getPriority(query);
    final Ordering<T> ordering = query.getResultOrdering();
    final QueryPlus<T> threadSafeQueryPlus = queryPlus.withoutThreadUnsafeState();
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            // Make it a List<> to materialize all of the values (so that it will submit everything to the executor)
            final ListenableFuture<List<Iterable<T>>> futures = GuavaUtils.allFuturesAsList(
                queryables.peek(
                    queryRunner -> {
                      if (queryRunner == null) {
                        throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
                      }
                    }
                ).map(
                    queryRunner -> new AbstractPrioritizedCallable<Iterable<T>>(priority)
                    {
                      @Override
                      public Iterable<T> call()
                      {
                        try {
                          Sequence<T> result = queryRunner.run(threadSafeQueryPlus, responseContext);
                          if (result == null) {
                            throw new ISE("Got a null result! Segments are missing!");
                          }

                          List<T> retVal = result.toList();
                          if (retVal == null) {
                            throw new ISE("Got a null list of results! WTF?!");
                          }

                          return retVal;
                        }
                        catch (QueryInterruptedException e) {
                          throw Throwables.propagate(e);
                        }
                        catch (Exception e) {
                          log.error(e, "Exception with one of the sequences!");
                          throw Throwables.propagate(e);
                        }
                      }
                    }
                ).map(exec::submit)
            );

            queryWatcher.registerQuery(query, futures);

            try {
              final DateTime deadline;
              if (QueryContexts.hasTimeout(query)) {
                deadline = DateTimes.nowUtc().plusMillis((int) QueryContexts.getTimeout(query));
              } else {
                deadline = DateTimes.utc(JodaUtils.MAX_INSTANT);
              }
              ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker()
              {
                @Override
                public boolean block() throws InterruptedException
                {
                  try {
                    futures.get(JodaUtils.timeoutForDeadline(deadline), TimeUnit.MILLISECONDS);
                  }
                  catch (ExecutionException | TimeoutException e) {
                    // Will get caught later
                  }
                  return true;
                }

                @Override
                public boolean isReleasable()
                {
                  return futures.isDone() || deadline.isBefore(DateTimes.nowUtc());
                }
              });
              return new MergeIterable<>(
                  ordering.nullsFirst(),
                  futures.get(JodaUtils.timeoutForDeadline(deadline), TimeUnit.MILLISECONDS)
              ).iterator();
            }
            catch (InterruptedException e) {
              log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
              futures.cancel(true);
              throw new QueryInterruptedException(e);
            }
            catch (CancellationException e) {
              throw new QueryInterruptedException(e);
            }
            catch (TimeoutException e) {
              log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
              futures.cancel(true);
              throw new QueryInterruptedException(e);
            }
            catch (ExecutionException e) {
              throw Throwables.propagate(e.getCause());
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
