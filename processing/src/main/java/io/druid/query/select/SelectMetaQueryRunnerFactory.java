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
public class SelectMetaQueryRunnerFactory
    implements QueryRunnerFactory<Result<SelectMetaResultValue>, SelectMetaQuery>
{
  private final SelectMetaQueryToolChest toolChest;
  private final SelectMetaQueryEngine engine;
  private final QueryWatcher queryWatcher;

  @Inject
  public SelectMetaQueryRunnerFactory(
      SelectMetaQueryToolChest toolChest,
      SelectMetaQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<SelectMetaResultValue>> createRunner(final Segment segment)
  {
    return new QueryRunner<Result<SelectMetaResultValue>>()
    {
      @Override
      public Sequence<Result<SelectMetaResultValue>> run(
          final Query<Result<SelectMetaResultValue>> query, Map<String, Object> responseContext
      )
      {
        return engine.process((SelectMetaQuery) query, segment);
      }
    };
  }

  @Override
  public QueryRunner<Result<SelectMetaResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<SelectMetaResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<Result<SelectMetaResultValue>>(
        queryExecutor, queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<SelectMetaResultValue>, SelectMetaQuery> getToolchest()
  {
    return toolChest;
  }
}
