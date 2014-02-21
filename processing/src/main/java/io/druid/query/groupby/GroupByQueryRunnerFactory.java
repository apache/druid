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
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.ExecutorExecutingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.GroupByParallelQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<Row, GroupByQuery>
{
  private final GroupByQueryEngine engine;
  private final Supplier<GroupByQueryConfig> config;
  private final GroupByQueryQueryToolChest toolChest;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByQueryEngine engine,
      Supplier<GroupByQueryConfig> config,
      GroupByQueryQueryToolChest toolChest
  )
  {
    this.engine = engine;
    this.config = config;
    this.toolChest = toolChest;
  }

  @Override
  public QueryRunner<Row> createRunner(final Segment segment)
  {
    return new GroupByQueryRunner(segment, engine);
  }

  @Override
  public QueryRunner<Row> mergeRunners(final ExecutorService queryExecutor, Iterable<QueryRunner<Row>> queryRunners)
  {
    if (config.get().isSingleThreaded()) {
      return new ConcatQueryRunner<Row>(
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

                      Future<Sequence<Row>> future = queryExecutor.submit(
                          new Callable<Sequence<Row>>()
                          {
                            @Override
                            public Sequence<Row> call() throws Exception
                            {
                              return new ExecutorExecutingSequence<Row>(
                                  input.run(query),
                                  queryExecutor
                              );
                            }
                          }
                      );
                      try {
                        return future.get();
                      }
                      catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                      }
                      catch (ExecutionException e) {
                        throw Throwables.propagate(e);
                      }
                    }
                  };
                }
              }
          )
      );
    } else {
      return new GroupByParallelQueryRunner(queryExecutor, new RowOrdering(), config, queryRunners);
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

  private static class RowOrdering extends Ordering<Row>
  {
    @Override
    public int compare(Row left, Row right)
    {
      return Longs.compare(left.getTimestampFromEpoch(), right.getTimestampFromEpoch());
    }
  }
}
