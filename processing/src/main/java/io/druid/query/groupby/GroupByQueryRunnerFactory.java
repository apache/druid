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

package io.druid.query.groupby;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.data.input.Row;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<Row, GroupByQuery>
{
  private final GroupByStrategySelector strategySelector;
  private final GroupByQueryQueryToolChest toolChest;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByStrategySelector strategySelector,
      GroupByQueryQueryToolChest toolChest
  )
  {
    this.strategySelector = strategySelector;
    this.toolChest = toolChest;
  }

  @Override
  public QueryRunner<Row> createRunner(final Segment segment)
  {
    return new GroupByQueryRunner(segment, strategySelector);
  }

  @Override
  public QueryRunner<Row> mergeRunners(final ExecutorService exec, final Iterable<QueryRunner<Row>> queryRunners)
  {
    // mergeRunners should take ListeningExecutorService at some point
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);

    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        return strategySelector.strategize((GroupByQuery) query).mergeRunners(queryExecutor, queryRunners).run(
            query,
            responseContext
        );
      }
    };
  }

  @Override
  public QueryToolChest<Row, GroupByQuery> getToolchest()
  {
    return toolChest;
  }

  private static class GroupByQueryRunner implements QueryRunner<Row>
  {
    private final StorageAdapter adapter;
    private final GroupByStrategySelector strategySelector;

    public GroupByQueryRunner(Segment segment, final GroupByStrategySelector strategySelector)
    {
      this.adapter = segment.asStorageAdapter();
      this.strategySelector = strategySelector;
    }

    @Override
    public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
    {
      if (!(input instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
      }

      return strategySelector.strategize((GroupByQuery) input).process((GroupByQuery) input, adapter);
    }
  }
}
