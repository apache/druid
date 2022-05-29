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

package org.apache.druid.queryng.operators.general;

import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.AbstractPrioritizedQueryRunnerCallable;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.QueryManager;
import org.apache.druid.queryng.operators.MergeResultIterator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs the "scatter" portion of a scatter/gather operation using a processing pool,
 * then "gathers" the results to a single result iterator using the strategy defined
 * by the subclass.
 * <p>
 * Follows the pattern from {@code ChainedExecutionQueryRunner} which materializes
 * each child result set for delivery via a {@code Future}. A good optimization
 * would be to deliver results via a producer/consumer queue so that the merge
 * fragment can operate concurrently with the child fragments. That mechanism does
 * not seem to exist in Druid, so we'll do that later: our goal here is to repackage
 * the existing functionality.
 *
 * @see org.apache.druid.query.ChainedExecutionQueryRunner
 */
public abstract class ScatterGatherOperator<T> implements Operator<T>
{
  private static final Logger log = new Logger(ScatterGatherOperator.class);

  protected final QueryPlus<T> queryPlus;
  private final QueryProcessingPool queryProcessingPool;
  private final Iterable<QueryRunner<T>> queryables;
  protected final QueryWatcher queryWatcher;
  protected int inputCount;

  public ScatterGatherOperator(
      final QueryPlus<T> queryPlus,
      final QueryProcessingPool queryProcessingPool,
      final Iterable<QueryRunner<T>> queryables,
      final QueryWatcher queryWatcher
  )
  {
    this.queryPlus = queryPlus;
    this.queryProcessingPool = queryProcessingPool;
    this.queryables = queryables;
    this.queryWatcher = queryWatcher;
    queryPlus.fragment().register(this);
  }

  @Override
  public ResultIterator<T> open()
  {
    return gather(scatter());
  }

  List<ListenableFuture<Iterable<T>>> scatter()
  {
    FragmentManager fragment = queryPlus.fragment();
    fragment.registerChild(this, 0, 2);
    QueryManager queryManager = fragment.query();
    final int priority = QueryContexts.getPriority(queryPlus.getQuery());
    List<ListenableFuture<Iterable<T>>> futures = new ArrayList<>();
    for (QueryRunner<T> childRunner : queryables) {
      inputCount++;
      FragmentManager childFragment = queryManager.createChildFragment(
          queryPlus.getQuery().getId(),
          queryPlus.fragment().responseContext()
      );
      final QueryPlus<T> childQueryPlus = queryPlus.withoutThreadUnsafeState().withFragment(childFragment);
      ChildFragmentCallable<T> child = new ChildFragmentCallable<>(childRunner, childQueryPlus, priority);
      futures.add(queryProcessingPool.submitRunnerTask(child));
    }
    return futures;
  }

  protected abstract ResultIterator.CloseableResultIterator<T> gather(List<ListenableFuture<Iterable<T>>> children);

  public static class OrderedScatterGatherOperator<T> extends ScatterGatherOperator<T>
  {
    private final Ordering<T> ordering;
    private MergeResultIterator<T> resultIter;

    public OrderedScatterGatherOperator(
        QueryPlus<T> queryPlus,
        QueryProcessingPool queryProcessingPool,
        Iterable<QueryRunner<T>> queryables,
        final Ordering<T> ordering,
        QueryWatcher queryWatcher
    )
    {
      super(queryPlus, queryProcessingPool, queryables, queryWatcher);
      this.ordering = ordering;
    }

    @Override
    protected ResultIterator.CloseableResultIterator<T> gather(List<ListenableFuture<Iterable<T>>> children)
    {
      List<Iterable<T>> results = materializeChildren(children);
      resultIter = new MergeResultIterator<T>(ordering, results.size());
      for (Iterable<T> iterable : results) {
        Iterator<T> iter = iterable.iterator();
        if (iter.hasNext()) {
          resultIter.add(new IteratorInput<T>(iter));
        }
      }
      return resultIter;
    }

    private List<Iterable<T>> materializeChildren(List<ListenableFuture<Iterable<T>>> children)
    {
      ListenableFuture<List<Iterable<T>>> future = Futures.allAsList(children);
      Query<T> query = queryPlus.getQuery();
      queryWatcher.registerQueryFuture(queryPlus.getQuery(), future);
      try {
        return QueryContexts.hasTimeout(query) ?
                future.get(QueryContexts.getTimeout(query), TimeUnit.MILLISECONDS) :
                future.get();
      }
      catch (InterruptedException e) {
        log.noStackTrace().warn(e, "Query interrupted, cancelling pending results, query ID [%s]", query.getId());
        // Note: canceling combinedFuture first so that it can complete with INTERRUPTED as its final state. See ChainedExecutionQueryRunnerTest.testQueryTimeout()
        GuavaUtils.cancelAll(true, future, children);
        throw new QueryInterruptedException(e);
      }
      catch (CancellationException e) {
        throw new QueryInterruptedException(e);
      }
      catch (TimeoutException e) {
        log.warn("Query timeout, canceling pending results for query ID [%s]", query.getId());
        GuavaUtils.cancelAll(true, future, children);
        throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
      }
      catch (ExecutionException e) {
        GuavaUtils.cancelAll(true, future, children);
        Throwables.propagateIfPossible(e.getCause());
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public void close(boolean cascade)
    {
      if (resultIter == null) {
        return;
      }
      OperatorProfile profile = new OperatorProfile("ordered-scatter-gather");
      profile.add(OperatorProfile.ROW_COUNT_METRIC, resultIter.rowCount);
      profile.add("input-count", inputCount);
      queryPlus.fragment().updateProfile(this, profile);
      resultIter.close(cascade);
      resultIter = null;
    }
  }

  //  public static class UnorderedScatterGatherOperator<T> extends ScatterGatherOperator<T>
  //  {
  //
  //    @Override
  //    protected Operator<T> gather(List<ListenableFuture<Iterable<T>>> children)
  //    {
  //
  //    }
  //  }

  private static class ChildFragmentCallable<T> extends AbstractPrioritizedQueryRunnerCallable<Iterable<T>, T>
  {
    private final QueryPlus<T> queryPlus;

    public ChildFragmentCallable(
        QueryRunner<T> runner,
        QueryPlus<T> queryPlus,
        int priority
    )
    {
      super(priority, runner);
      this.queryPlus = queryPlus;
    }

    @Override
    public Iterable<T> call() throws Exception
    {
      FragmentManager fragment = queryPlus.fragment();
      try {
        Sequence<T> result = getRunner().run(queryPlus, fragment.responseContext());
        if (result == null) {
          throw new ISE("Got a null result! Segments are missing!");
        }
        fragment.registerRoot(result);
        return fragment.toList();
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

  private static class IteratorInput<T> implements MergeResultIterator.Input<T>
  {
    private final Iterator<T> iter;
    private T currentValue;

    public IteratorInput(Iterator<T> iter)
    {
      this.iter = iter;
      this.currentValue = iter.next();
    }

    @Override
    public boolean next()
    {
      if (!iter.hasNext()) {
        return false;
      }
      currentValue = iter.next();
      return true;
    }

    @Override
    public T get()
    {
      return currentValue;
    }

    @Override
    public void close()
    {
    }
  }
}
