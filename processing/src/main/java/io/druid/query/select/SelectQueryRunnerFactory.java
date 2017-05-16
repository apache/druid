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

package io.druid.query.select;

import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
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
        queryExecutor, queryWatcher, queryRunners
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
        QueryPlus<Result<SelectResultValue>> queryPlus,
        Map<String, Object> responseContext
    )
    {
      Query<Result<SelectResultValue>> input = queryPlus.getQuery();
      if (!(input instanceof SelectQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SelectQuery.class);
      }

      return engine.process((SelectQuery) input, segment);
    }
  }
}
