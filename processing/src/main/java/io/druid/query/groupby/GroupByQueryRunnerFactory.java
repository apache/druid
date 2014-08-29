/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Row;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.GroupByParallelQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.incremental.IncrementalIndex;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<Row, GroupByQuery>
{
  private final GroupByQueryEngine engine;
  private final QueryWatcher queryWatcher;
  private final Supplier<GroupByQueryConfig> config;
  private final GroupByQueryQueryToolChest toolChest;

  private static final Logger log = new Logger(GroupByQueryRunnerFactory.class);

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByQueryEngine engine,
      QueryWatcher queryWatcher,
      Supplier<GroupByQueryConfig> config,
      GroupByQueryQueryToolChest toolChest
  )
  {
    this.engine = engine;
    this.queryWatcher = queryWatcher;
    this.config = config;
    this.toolChest = toolChest;
  }

  @Override
  public QueryRunner<Row> createRunner(final Segment segment)
  {
    return new GroupByQueryRunner(segment, engine);
  }

  @Override
  public QueryRunner<Row> mergeRunners(final ExecutorService exec, Iterable<QueryRunner<Row>> queryRunners)
  {
    // mergeRunners should take ListeningExecutorService at some point
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);

    if (config.get().isSingleThreaded()) {
      return new ConcatQueryRunner<>(
          Sequences.map(
              Sequences.simple(queryRunners),
              new Function<QueryRunner<Row>, QueryRunner<Row>>()
              {
                @Override
                public QueryRunner<Row> apply(final QueryRunner<Row> input)
                {
                  return new QueryRunner<Row>()
                  {
                    @Override
                    public Sequence<Row> run(final Query<Row> query)
                    {
                      final GroupByQuery queryParam = (GroupByQuery) query;
                      final Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> indexAccumulatorPair = GroupByQueryHelper
                          .createIndexAccumulatorPair(
                              queryParam,
                              config.get()
                          );
                      final Pair<List, Accumulator<List, Row>> bySegmentAccumulatorPair = GroupByQueryHelper.createBySegmentAccumulatorPair();
                      final int priority = query.getContextPriority(0);
                      final boolean bySegment = query.getContextBySegment(false);

                      final ListenableFuture<Void> future = queryExecutor.submit(
                          new AbstractPrioritizedCallable<Void>(priority)
                          {
                            @Override
                            public Void call() throws Exception
                            {
                              if (bySegment) {
                                input.run(queryParam)
                                     .accumulate(
                                         bySegmentAccumulatorPair.lhs,
                                         bySegmentAccumulatorPair.rhs
                                     );
                              } else {
                                input.run(query).accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
                              }

                              return null;
                            }
                          }
                      );
                      try {
                        queryWatcher.registerQuery(query, future);
                        final Number timeout = query.getContextValue("timeout", (Number) null);
                        if (timeout == null) {
                          future.get();
                        } else {
                          future.get(timeout.longValue(), TimeUnit.MILLISECONDS);
                        }
                      }
                      catch (InterruptedException e) {
                        log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
                        future.cancel(true);
                        throw new QueryInterruptedException("Query interrupted");
                      }
                      catch (CancellationException e) {
                        throw new QueryInterruptedException("Query cancelled");
                      }
                      catch (TimeoutException e) {
                        log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
                        future.cancel(true);
                        throw new QueryInterruptedException("Query timeout");
                      }
                      catch (ExecutionException e) {
                        throw Throwables.propagate(e.getCause());
                      }

                      if (bySegment) {
                        return Sequences.simple(bySegmentAccumulatorPair.lhs);
                      }

                      return Sequences.simple(indexAccumulatorPair.lhs.iterableWithPostAggregations(null));
                    }
                  };
                }
              }
          )
      );
    } else {
      return new GroupByParallelQueryRunner(queryExecutor, config, queryWatcher, queryRunners);
    }
  }

  @Override
  public QueryToolChest<Row, GroupByQuery> getToolchest()
  {
    return toolChest;
  }

  private static class GroupByQueryRunner implements QueryRunner<Row>
  {
    private final StorageAdapter adapter;
    private final GroupByQueryEngine engine;

    public GroupByQueryRunner(Segment segment, final GroupByQueryEngine engine)
    {
      this.adapter = segment.asStorageAdapter();
      this.engine = engine;
    }

    @Override
    public Sequence<Row> run(Query<Row> input)
    {
      if (!(input instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
      }

      return engine.process((GroupByQuery) input, adapter);
    }
  }
}
