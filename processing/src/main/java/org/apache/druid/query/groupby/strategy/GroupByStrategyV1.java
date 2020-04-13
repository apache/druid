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

package org.apache.druid.query.groupby.strategy;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.GroupByMergedQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryEngine;
import org.apache.druid.query.groupby.GroupByQueryHelper;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class GroupByStrategyV1 implements GroupByStrategy
{
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final GroupByQueryEngine engine;
  private final QueryWatcher queryWatcher;
  private final NonBlockingPool<ByteBuffer> bufferPool;

  @Inject
  public GroupByStrategyV1(
      Supplier<GroupByQueryConfig> configSupplier,
      GroupByQueryEngine engine,
      QueryWatcher queryWatcher,
      @Global NonBlockingPool<ByteBuffer> bufferPool
  )
  {
    this.configSupplier = configSupplier;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
    this.bufferPool = bufferPool;
  }

  @Override
  public GroupByQueryResource prepareResource(GroupByQuery query)
  {
    return new GroupByQueryResource();
  }

  @Override
  public boolean isCacheable(boolean willMergeRunners)
  {
    return true;
  }

  @Override
  public boolean doMergeResults(final GroupByQuery query)
  {
    return query.getContextBoolean(GroupByQueryQueryToolChest.GROUP_BY_MERGE_KEY, true);
  }

  @Override
  public Sequence<ResultRow> mergeResults(
      final QueryRunner<ResultRow> baseRunner,
      final GroupByQuery query,
      final ResponseContext responseContext
  )
  {
    final IncrementalIndex index = GroupByQueryHelper.makeIncrementalIndex(
        query,
        null,
        configSupplier.get(),
        bufferPool,
        baseRunner.run(
            QueryPlus.wrap(
                new GroupByQuery.Builder(query)
                    // Don't do post aggs until the end of this method.
                    .setPostAggregatorSpecs(ImmutableList.of())
                    // Don't do "having" clause until the end of this method.
                    .setHavingSpec(null)
                    .setLimitSpec(NoopLimitSpec.instance())
                    .overrideContext(
                        ImmutableMap.<String, Object>builder()
                            .put(GroupByQueryConfig.CTX_KEY_STRATEGY, GroupByStrategySelector.STRATEGY_V1)
                            .put("finalize", false)

                            // Always request array result rows when passing the query down.
                            .put(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true)

                            // Set sort to false avoids unnecessary sorting while merging results. we only need to sort
                            // in the end when returning results to user. (note this is only respected by groupBy v1)
                            .put(GroupByQueryHelper.CTX_KEY_SORT_RESULTS, false)

                            // No merging needed at historicals because GroupByQueryRunnerFactory.mergeRunners(..) would
                            // return merged results. (note this is only respected by groupBy v1)
                            .put(GroupByQueryQueryToolChest.GROUP_BY_MERGE_KEY, false)
                            .build()
                    )
                    .build()
            ),
            responseContext
        )
    );

    return Sequences.withBaggage(GroupByQueryHelper.postAggregate(query, index), index);
  }

  @Override
  public Sequence<ResultRow> applyPostProcessing(Sequence<ResultRow> results, GroupByQuery query)
  {
    return query.postProcess(results);
  }

  @Override
  public Sequence<ResultRow> processSubqueryResult(
      GroupByQuery subquery,
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<ResultRow> subqueryResult,
      boolean wasQueryPushedDown
  )
  {
    final Set<AggregatorFactory> aggs = new HashSet<>();

    // Nested group-bys work by first running the inner query and then materializing the results in an incremental
    // index which the outer query is then run against. To build the incremental index, we use the fieldNames from
    // the aggregators for the outer query to define the column names so that the index will match the query. If
    // there are multiple types of aggregators in the outer query referencing the same fieldName, we will try to build
    // multiple columns of the same name using different aggregator types and will fail. Here, we permit multiple
    // aggregators of the same type referencing the same fieldName (and skip creating identical columns for the
    // subsequent ones) and return an error if the aggregator types are different.
    final Set<String> dimensionNames = new HashSet<>();
    for (DimensionSpec dimension : subquery.getDimensions()) {
      dimensionNames.add(dimension.getOutputName());
    }
    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      for (final AggregatorFactory transferAgg : aggregatorFactory.getRequiredColumns()) {
        if (dimensionNames.contains(transferAgg.getName())) {
          // This transferAgg is already represented in the subquery's dimensions. Assume that the outer aggregator
          // *probably* wants the dimension and just ignore it. This is a gross workaround for cases like having
          // a cardinality aggregator in the outer query. It is necessary because what this block of code is trying to
          // do is use aggregators to "transfer" values from the inner results to an incremental index, but aggregators
          // can't transfer all kinds of values (strings are a common one). If you don't like it, use groupBy v2, which
          // doesn't have this problem.
          continue;
        }
        if (Iterables.any(aggs, new Predicate<AggregatorFactory>()
        {
          @Override
          public boolean apply(AggregatorFactory agg)
          {
            return agg.getName().equals(transferAgg.getName()) && !agg.equals(transferAgg);
          }
        })) {
          throw new IAE("Inner aggregator can currently only be referenced by a single type of outer aggregator" +
                        " for '%s'", transferAgg.getName());
        }

        aggs.add(transferAgg);
      }
    }

    // We need the inner incremental index to have all the columns required by the outer query
    final GroupByQuery innerQuery = new GroupByQuery.Builder(subquery)
        .setAggregatorSpecs(ImmutableList.copyOf(aggs))
        .setInterval(subquery.getIntervals())
        .setPostAggregatorSpecs(new ArrayList<>())
        .build();

    final GroupByQuery outerQuery = new GroupByQuery.Builder(query)
        .setLimitSpec(query.getLimitSpec().merge(subquery.getLimitSpec()))
        .build();

    final IncrementalIndex innerQueryResultIndex = GroupByQueryHelper.makeIncrementalIndex(
        innerQuery.withOverriddenContext(
            ImmutableMap.of(
                GroupByQueryHelper.CTX_KEY_SORT_RESULTS, true
            )
        ),
        subquery,
        configSupplier.get(),
        bufferPool,
        subqueryResult
    );

    //Outer query might have multiple intervals, but they are expected to be non-overlapping and sorted which
    //is ensured by QuerySegmentSpec.
    //GroupByQueryEngine can only process one interval at a time, so we need to call it once per interval
    //and concatenate the results.
    final IncrementalIndex outerQueryResultIndex = GroupByQueryHelper.makeIncrementalIndex(
        outerQuery,
        null,
        configSupplier.get(),
        bufferPool,
        Sequences.concat(
            Sequences.map(
                Sequences.simple(outerQuery.getIntervals()),
                new Function<Interval, Sequence<ResultRow>>()
                {
                  @Override
                  public Sequence<ResultRow> apply(Interval interval)
                  {
                    return process(
                        outerQuery.withQuerySegmentSpec(
                            new MultipleIntervalSegmentSpec(ImmutableList.of(interval))
                        ),
                        new IncrementalIndexStorageAdapter(innerQueryResultIndex)
                    );
                  }
                }
            )
        )
    );

    innerQueryResultIndex.close();

    return Sequences.withBaggage(
        outerQuery.postProcess(GroupByQueryHelper.postAggregate(query, outerQueryResultIndex)),
        outerQueryResultIndex
    );
  }

  @Override
  public Sequence<ResultRow> processSubtotalsSpec(
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<ResultRow> queryResult
  )
  {
    throw new UnsupportedOperationException("subtotalsSpec is not supported for v1 groupBy strategy.");
  }

  @Override
  public QueryRunner<ResultRow> mergeRunners(
      final ListeningExecutorService exec,
      final Iterable<QueryRunner<ResultRow>> queryRunners
  )
  {
    return new GroupByMergedQueryRunner<>(exec, configSupplier, queryWatcher, bufferPool, queryRunners);
  }

  @Override
  public Sequence<ResultRow> process(final GroupByQuery query, final StorageAdapter storageAdapter)
  {
    return Sequences.map(
        engine.process(query, storageAdapter),
        row -> GroupByQueryHelper.toResultRow(query, row)
    );
  }

  @Override
  public boolean supportsNestedQueryPushDown()
  {
    return false;
  }
}
