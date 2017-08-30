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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.druid.collections.NonBlockingPool;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.GroupByMergedQueryRunner;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.groupby.resource.GroupByQueryResource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.StorageAdapter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Map;
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
  public GroupByQueryResource prepareResource(GroupByQuery query, boolean willMergeRunners)
  {
    return new GroupByQueryResource();
  }

  @Override
  public boolean isCacheable(boolean willMergeRunners)
  {
    return true;
  }

  @Override
  public QueryRunner<Row> createIntervalChunkingRunner(
      final IntervalChunkingQueryRunnerDecorator decorator,
      final QueryRunner<Row> runner,
      final GroupByQueryQueryToolChest toolChest
  )
  {
    return decorator.decorate(runner, toolChest);
  }

  @Override
  public boolean doMergeResults(final GroupByQuery query)
  {
    return query.getContextBoolean(GroupByQueryQueryToolChest.GROUP_BY_MERGE_KEY, true);
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
            QueryPlus.wrap(
                new GroupByQuery.Builder(query)
                    // Don't do post aggs until the end of this method.
                    .setPostAggregatorSpecs(ImmutableList.of())
                    // Don't do "having" clause until the end of this method.
                    .setHavingSpec(null)
                    .setLimitSpec(NoopLimitSpec.instance())
                    .overrideContext(
                        ImmutableMap.of(
                            "finalize", false,
                            //set sort to false avoids unnecessary sorting while merging results. we only need to sort
                            //in the end when returning results to user. (note this is only respected by groupBy v1)
                            GroupByQueryHelper.CTX_KEY_SORT_RESULTS, false,
                            //no merging needed at historicals because GroupByQueryRunnerFactory.mergeRunners(..) would
                            //return merged results. (note this is only respected by groupBy v1)
                            GroupByQueryQueryToolChest.GROUP_BY_MERGE_KEY, false,
                            GroupByQueryConfig.CTX_KEY_STRATEGY, GroupByStrategySelector.STRATEGY_V1
                        )
                    )
                    .build()
            ),
            responseContext
        ),
        true
    );

    return Sequences.withBaggage(query.postProcess(GroupByQueryHelper.postAggregate(query, index)), index);
  }

  @Override
  public Sequence<Row> processSubqueryResult(
      GroupByQuery subquery, GroupByQuery query, GroupByQueryResource resource, Sequence<Row> subqueryResult
  )
  {
    final Set<AggregatorFactory> aggs = Sets.newHashSet();

    // Nested group-bys work by first running the inner query and then materializing the results in an incremental
    // index which the outer query is then run against. To build the incremental index, we use the fieldNames from
    // the aggregators for the outer query to define the column names so that the index will match the query. If
    // there are multiple types of aggregators in the outer query referencing the same fieldName, we will try to build
    // multiple columns of the same name using different aggregator types and will fail. Here, we permit multiple
    // aggregators of the same type referencing the same fieldName (and skip creating identical columns for the
    // subsequent ones) and return an error if the aggregator types are different.
    final Set<String> dimensionNames = Sets.newHashSet();
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
        .setAggregatorSpecs(Lists.newArrayList(aggs))
        .setInterval(subquery.getIntervals())
        .setPostAggregatorSpecs(Lists.<PostAggregator>newArrayList())
        .build();

    final GroupByQuery outerQuery = new GroupByQuery.Builder(query)
        .setLimitSpec(query.getLimitSpec().merge(subquery.getLimitSpec()))
        .build();

    final IncrementalIndex innerQueryResultIndex = GroupByQueryHelper.makeIncrementalIndex(
        innerQuery.withOverriddenContext(
            ImmutableMap.<String, Object>of(
                GroupByQueryHelper.CTX_KEY_SORT_RESULTS, true
            )
        ),
        configSupplier.get(),
        bufferPool,
        subqueryResult,
        false
    );

    //Outer query might have multiple intervals, but they are expected to be non-overlapping and sorted which
    //is ensured by QuerySegmentSpec.
    //GroupByQueryEngine can only process one interval at a time, so we need to call it once per interval
    //and concatenate the results.
    final IncrementalIndex outerQueryResultIndex = GroupByQueryHelper.makeIncrementalIndex(
        outerQuery,
        configSupplier.get(),
        bufferPool,
        Sequences.concat(
            Sequences.map(
                Sequences.simple(outerQuery.getIntervals()),
                new Function<Interval, Sequence<Row>>()
                {
                  @Override
                  public Sequence<Row> apply(Interval interval)
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
        ),
        true
    );

    innerQueryResultIndex.close();

    return Sequences.withBaggage(
        outerQuery.postProcess(GroupByQueryHelper.postAggregate(query, outerQueryResultIndex)),
        outerQueryResultIndex
    );
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
