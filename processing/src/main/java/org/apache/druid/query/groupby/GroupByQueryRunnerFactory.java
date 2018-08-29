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

package org.apache.druid.query.groupby;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;

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
      public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
      {
        QueryRunner<Row> rowQueryRunner = strategySelector.strategize((GroupByQuery) queryPlus.getQuery()).mergeRunners(
            queryExecutor,
            queryRunners
        );
        return rowQueryRunner.run(queryPlus, responseContext);
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
    public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
    {
      Query<Row> query = queryPlus.getQuery();
      if (!(query instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), GroupByQuery.class);
      }

      return strategySelector.strategize((GroupByQuery) query).process((GroupByQuery) query, adapter);
    }
  }
}
