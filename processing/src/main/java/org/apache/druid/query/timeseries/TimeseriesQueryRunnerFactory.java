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

package org.apache.druid.query.timeseries;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.Result;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;

import java.util.concurrent.ExecutorService;

/**
 */
public class TimeseriesQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private final TimeseriesQueryQueryToolChest toolChest;
  private final TimeseriesQueryEngine engine;
  private final QueryWatcher queryWatcher;

  @Inject
  public TimeseriesQueryRunnerFactory(
      TimeseriesQueryQueryToolChest toolChest,
      TimeseriesQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> createRunner(final Segment segment)
  {
    return new TimeseriesQueryRunner(engine, segment.asStorageAdapter());
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<Result<TimeseriesResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> getToolchest()
  {
    return toolChest;
  }

  private static class TimeseriesQueryRunner implements QueryRunner<Result<TimeseriesResultValue>>
  {
    private final TimeseriesQueryEngine engine;
    private final StorageAdapter adapter;

    private TimeseriesQueryRunner(TimeseriesQueryEngine engine, StorageAdapter adapter)
    {
      this.engine = engine;
      this.adapter = adapter;
    }

    @Override
    public Sequence<Result<TimeseriesResultValue>> run(
        QueryPlus<Result<TimeseriesResultValue>> queryPlus,
        ResponseContext responseContext
    )
    {
      Query<Result<TimeseriesResultValue>> input = queryPlus.getQuery();
      if (!(input instanceof TimeseriesQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeseriesQuery.class);
      }

      return engine.process((TimeseriesQuery) input, adapter);
    }
  }

}
