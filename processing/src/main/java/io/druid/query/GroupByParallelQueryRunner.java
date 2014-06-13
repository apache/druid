/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.segment.incremental.IncrementalIndex;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class GroupByParallelQueryRunner implements QueryRunner<Row>
{
  private static final Logger log = new Logger(GroupByParallelQueryRunner.class);
  private final Iterable<QueryRunner<Row>> queryables;
  private final ListeningExecutorService exec;
  private final Ordering<Row> ordering;
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final QueryWatcher queryWatcher;


  public GroupByParallelQueryRunner(
      ExecutorService exec,
      Ordering<Row> ordering,
      Supplier<GroupByQueryConfig> configSupplier,
      QueryWatcher queryWatcher,
      QueryRunner<Row>... queryables
  )
  {
    this(exec, ordering, configSupplier, queryWatcher, Arrays.asList(queryables));
  }

  public GroupByParallelQueryRunner(
      ExecutorService exec,
      Ordering<Row> ordering, Supplier<GroupByQueryConfig> configSupplier,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<Row>> queryables
  )
  {
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.ordering = ordering;
    this.queryWatcher = queryWatcher;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.configSupplier = configSupplier;
  }

  @Override
  public Sequence<Row> run(final Query<Row> queryParam)
  {
    final GroupByQuery query = (GroupByQuery) queryParam;
    final Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> indexAccumulatorPair = GroupByQueryHelper.createIndexAccumulatorPair(
        query,
        configSupplier.get()
    );
    final int priority = query.getContextPriority(0);

    if (Iterables.isEmpty(queryables)) {
      log.warn("No queryables found.");
    }
    ListenableFuture<List<Boolean>> futures = Futures.allAsList(
        Lists.newArrayList(
            Iterables.transform(
                queryables,
                new Function<QueryRunner<Row>, ListenableFuture<Boolean>>()
                {
                  @Override
                  public ListenableFuture<Boolean> apply(final QueryRunner<Row> input)
                  {
                    return exec.submit(
                        new AbstractPrioritizedCallable<Boolean>(priority)
                        {
                          @Override
                          public Boolean call() throws Exception
                          {
                            try {
                              input.run(queryParam).accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
                              return true;
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
                    );
                  }
                }
            )
        )
    );

    // Let the runners complete
    try {
      queryWatcher.registerQuery(query, futures);
      final Number timeout = query.getContextValue("timeout", (Number) null);
      if(timeout == null) {
        futures.get();
      } else {
        futures.get(timeout.longValue(), TimeUnit.MILLISECONDS);
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      futures.cancel(true);
      throw new QueryInterruptedException("Query interrupted");
    }
    catch(CancellationException e) {
      throw new QueryInterruptedException("Query cancelled");
    }
    catch(TimeoutException e) {
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      futures.cancel(true);
      throw new QueryInterruptedException("Query timeout");
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }

    return Sequences.simple(indexAccumulatorPair.lhs.iterableWithPostAggregations(null));
  }

}
