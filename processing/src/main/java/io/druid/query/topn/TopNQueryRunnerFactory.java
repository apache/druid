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

package io.druid.query.topn;

import com.google.inject.Inject;
import io.druid.collections.StupidPool;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class TopNQueryRunnerFactory implements QueryRunnerFactory<Result<TopNResultValue>, TopNQuery>
{
  private final StupidPool<ByteBuffer> computationBufferPool;
  private final TopNQueryQueryToolChest toolchest;
  private final QueryWatcher queryWatcher;

  @Inject
  public TopNQueryRunnerFactory(
      @Global StupidPool<ByteBuffer> computationBufferPool,
      TopNQueryQueryToolChest toolchest,
      QueryWatcher queryWatcher
  )
  {
    this.computationBufferPool = computationBufferPool;
    this.toolchest = toolchest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> createRunner(final Segment segment)
  {
    final TopNQueryEngine queryEngine = new TopNQueryEngine(computationBufferPool);
    return new QueryRunner<Result<TopNResultValue>>()
    {
      @Override
      public Sequence<Result<TopNResultValue>> run(
          Query<Result<TopNResultValue>> input,
          Map<String, Object> responseContext
      )
      {
        if (!(input instanceof TopNQuery)) {
          throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TopNQuery.class);
        }

        return queryEngine.query((TopNQuery) input, segment.asStorageAdapter());
      }
    };

  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TopNResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(
        queryExecutor, queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<TopNResultValue>, TopNQuery> getToolchest()
  {
    return toolchest;
  }
}
