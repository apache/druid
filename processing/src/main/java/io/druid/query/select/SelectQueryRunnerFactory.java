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

package io.druid.query.select;

import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class SelectQueryRunnerFactory
    implements QueryRunnerFactory<Result<SelectResultValue>, SelectQuery>
{
  private final SelectQueryQueryToolChest toolChest;
  private final SelectQueryEngine engine;
  private final QueryWatcher queryWatcher;

  @Inject
  public SelectQueryRunnerFactory(
      SelectQueryQueryToolChest toolChest,
      SelectQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> createRunner(final Segment segment)
  {
    return new SelectQueryRunner(engine, segment);
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<SelectResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<Result<SelectResultValue>>(
        queryExecutor, toolChest.getOrdering(), queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<SelectResultValue>, SelectQuery> getToolchest()
  {
    return toolChest;
  }

  private static class SelectQueryRunner implements QueryRunner<Result<SelectResultValue>>
  {
    private final SelectQueryEngine engine;
    private final Segment segment;

    private SelectQueryRunner(SelectQueryEngine engine, Segment segment)
    {
      this.engine = engine;
      this.segment = segment;
    }

    @Override
    public Sequence<Result<SelectResultValue>> run(
        Query<Result<SelectResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof SelectQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SelectQuery.class);
      }

      return engine.process((SelectQuery) input, segment);
    }
  }
}
