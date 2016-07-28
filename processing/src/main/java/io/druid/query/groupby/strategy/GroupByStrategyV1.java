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

package io.druid.query.groupby.strategy;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.query.GroupByMergedQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.segment.StorageAdapter;
import io.druid.segment.incremental.IncrementalIndex;

import java.nio.ByteBuffer;
import java.util.Map;

public class GroupByStrategyV1 implements GroupByStrategy
{
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final GroupByQueryEngine engine;
  private final QueryWatcher queryWatcher;
  private final StupidPool<ByteBuffer> bufferPool;

  @Inject
  public GroupByStrategyV1(
      Supplier<GroupByQueryConfig> configSupplier,
      GroupByQueryEngine engine,
      QueryWatcher queryWatcher,
      @Global StupidPool<ByteBuffer> bufferPool
  )
  {
    this.configSupplier = configSupplier;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
    this.bufferPool = bufferPool;
  }

  @Override
  public Sequence<Row> mergeResults(
      final QueryRunner<Row> baseRunner,
      final GroupByQuery query,
      final Map<String, Object> responseContext
  )
  {
    final IncrementalIndex index = GroupByQueryHelper.makeIncrementalIndex(
        query,
        configSupplier.get(),
        bufferPool,
        baseRunner.run(
            new GroupByQuery(
                query.getDataSource(),
                query.getQuerySegmentSpec(),
                query.getDimFilter(),
                query.getGranularity(),
                query.getDimensions(),
                query.getAggregatorSpecs(),
                // Don't do post aggs until the end of this method.
                ImmutableList.<PostAggregator>of(),
                // Don't do "having" clause until the end of this method.
                null,
                null,
                query.getContext()
            ).withOverriddenContext(
                ImmutableMap.<String, Object>of(
                    "finalize", false,
                    //setting sort to false avoids unnecessary sorting while merging results. we only need to sort
                    //in the end when returning results to user.
                    GroupByQueryHelper.CTX_KEY_SORT_RESULTS, false,
                    //no merging needed at historicals because GroupByQueryRunnerFactory.mergeRunners(..) would return
                    //merged results
                    GroupByQueryQueryToolChest.GROUP_BY_MERGE_KEY, false,
                    GroupByQueryConfig.CTX_KEY_STRATEGY, GroupByStrategySelector.STRATEGY_V1
                )
            ),
            responseContext
        )
    );

    return new ResourceClosingSequence<>(query.applyLimit(GroupByQueryHelper.postAggregate(query, index)), index);
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      final ListeningExecutorService exec,
      final Iterable<QueryRunner<Row>> queryRunners
  )
  {
    return new GroupByMergedQueryRunner<>(exec, configSupplier, queryWatcher, bufferPool, queryRunners);
  }

  @Override
  public Sequence<Row> process(
      final GroupByQuery query,
      final StorageAdapter storageAdapter
  )
  {
    return engine.process(query, storageAdapter);
  }
}
