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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.InsufficientResourcesException;
import org.apache.druid.query.IntervalChunkingQueryRunnerDecorator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.ResultMergeQueryRunner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByBinaryFnV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByRowProcessor;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.StorageAdapter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

public class GroupByStrategyV2 implements GroupByStrategy
{
  public static final String CTX_KEY_FUDGE_TIMESTAMP = "fudgeTimestamp";
  public static final String CTX_KEY_OUTERMOST = "groupByOutermost";

  // see countRequiredMergeBufferNum() for explanation
  private static final int MAX_MERGE_BUFFER_NUM = 2;

  private final DruidProcessingConfig processingConfig;
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final NonBlockingPool<ByteBuffer> bufferPool;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final ObjectMapper spillMapper;
  private final QueryWatcher queryWatcher;

  @Inject
  public GroupByStrategyV2(
      DruidProcessingConfig processingConfig,
      Supplier<GroupByQueryConfig> configSupplier,
      @Global NonBlockingPool<ByteBuffer> bufferPool,
      @Merging BlockingPool<ByteBuffer> mergeBufferPool,
      @Smile ObjectMapper spillMapper,
      QueryWatcher queryWatcher
  )
  {
    this.processingConfig = processingConfig;
    this.configSupplier = configSupplier;
    this.bufferPool = bufferPool;
    this.mergeBufferPool = mergeBufferPool;
    this.spillMapper = spillMapper;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public GroupByQueryResource prepareResource(GroupByQuery query)
  {
    final int requiredMergeBufferNum = countRequiredMergeBufferNum(query, 1) +
                                       (query.getSubtotalsSpec() != null ? 1 : 0);

    if (requiredMergeBufferNum > mergeBufferPool.maxSize()) {
      throw new ResourceLimitExceededException(
          "Query needs " + requiredMergeBufferNum + " merge buffers, but only "
          + mergeBufferPool.maxSize() + " merge buffers were configured"
      );
    } else if (requiredMergeBufferNum == 0) {
      return new GroupByQueryResource();
    } else {
      final List<ReferenceCountingResourceHolder<ByteBuffer>> mergeBufferHolders;
      if (QueryContexts.hasTimeout(query)) {
        mergeBufferHolders = mergeBufferPool.takeBatch(requiredMergeBufferNum, QueryContexts.getTimeout(query));
      } else {
        mergeBufferHolders = mergeBufferPool.takeBatch(requiredMergeBufferNum);
      }
      if (mergeBufferHolders.isEmpty()) {
        throw new InsufficientResourcesException("Cannot acquire enough merge buffers");
      } else {
        return new GroupByQueryResource(mergeBufferHolders);
      }
    }
  }

  private static int countRequiredMergeBufferNum(Query query, int foundNum)
  {
    // Note: A broker requires merge buffers for processing the groupBy layers beyond the inner-most one.
    // For example, the number of required merge buffers for a nested groupBy (groupBy -> groupBy -> table) is 1.
    // If the broker processes an outer groupBy which reads input from an inner groupBy,
    // it requires two merge buffers for inner and outer groupBys to keep the intermediate result of inner groupBy
    // until the outer groupBy processing completes.
    // This is same for subsequent groupBy layers, and thus the maximum number of required merge buffers becomes 2.

    final DataSource dataSource = query.getDataSource();
    if (foundNum == MAX_MERGE_BUFFER_NUM + 1 || !(dataSource instanceof QueryDataSource)) {
      return foundNum - 1;
    } else {
      return countRequiredMergeBufferNum(((QueryDataSource) dataSource).getQuery(), foundNum + 1);
    }
  }

  @Override
  public boolean isCacheable(boolean willMergeRunners)
  {
    return willMergeRunners;
  }

  @Override
  public boolean doMergeResults(final GroupByQuery query)
  {
    return true;
  }

  @Override
  public QueryRunner<ResultRow> createIntervalChunkingRunner(
      final IntervalChunkingQueryRunnerDecorator decorator,
      final QueryRunner<ResultRow> runner,
      final GroupByQueryQueryToolChest toolChest
  )
  {
    // No chunkPeriod-based interval chunking for groupBy v2.
    //  1) It concats query chunks for consecutive intervals, which won't generate correct results.
    //  2) Merging instead of concating isn't a good idea, since it requires all chunks to run simultaneously,
    //     which may take more resources than the cluster has.
    // See also https://github.com/apache/incubator-druid/pull/4004
    return runner;
  }

  @Override
  public Comparator<ResultRow> createResultComparator(Query<ResultRow> queryParam)
  {
    return ((GroupByQuery) queryParam).getRowOrdering(true);
  }

  @Override
  public BinaryOperator<ResultRow> createMergeFn(Query<ResultRow> queryParam)
  {
    return new GroupByBinaryFnV2((GroupByQuery) queryParam);
  }

  @Override
  public Sequence<ResultRow> mergeResults(
      final QueryRunner<ResultRow> baseRunner,
      final GroupByQuery query,
      final ResponseContext responseContext
  )
  {
    // Merge streams using ResultMergeQueryRunner, then apply postaggregators, then apply limit (which may
    // involve materialization)
    final ResultMergeQueryRunner<ResultRow> mergingQueryRunner = new ResultMergeQueryRunner<>(
        baseRunner,
        this::createResultComparator,
        this::createMergeFn
    );

    // Set up downstream context.
    final ImmutableMap.Builder<String, Object> context = ImmutableMap.builder();
    context.put("finalize", false);
    context.put(GroupByQueryConfig.CTX_KEY_STRATEGY, GroupByStrategySelector.STRATEGY_V2);
    context.put(CTX_KEY_OUTERMOST, false);
    if (query.getUniversalTimestamp() != null) {
      context.put(CTX_KEY_FUDGE_TIMESTAMP, String.valueOf(query.getUniversalTimestamp().getMillis()));
    }

    // The having spec shouldn't be passed down, so we need to convey the existing limit push down status
    context.put(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, query.isApplyLimitPushDown());

    // Always request array result rows when passing the query downstream.
    context.put(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true);

    final GroupByQuery newQuery = new GroupByQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        query.getVirtualColumns(),
        query.getDimFilter(),
        query.getGranularity(),
        query.getDimensions(),
        query.getAggregatorSpecs(),
        query.getPostAggregatorSpecs(),
        // Don't do "having" clause until the end of this method.
        null,
        query.getLimitSpec(),
        query.getSubtotalsSpec(),
        query.getContext()
    ).withOverriddenContext(
        context.build()
    );

    final Sequence<ResultRow> mergedResults = mergingQueryRunner.run(QueryPlus.wrap(newQuery), responseContext);

    // Apply postaggregators if this is the outermost mergeResults (CTX_KEY_OUTERMOST) and we are not executing a
    // pushed-down subquery (CTX_KEY_EXECUTING_NESTED_QUERY).

    if (!query.getContextBoolean(CTX_KEY_OUTERMOST, true)
        || query.getPostAggregatorSpecs().isEmpty()
        || query.getContextBoolean(GroupByQueryConfig.CTX_KEY_EXECUTING_NESTED_QUERY, false)) {
      return mergedResults;
    } else {
      return Sequences.map(
          mergedResults,
          row -> {
            // This function's purpose is to apply PostAggregators.

            final ResultRow rowWithPostAggregations = ResultRow.create(query.getResultRowSizeWithPostAggregators());

            // Copy everything that comes before the postaggregations.
            for (int i = 0; i < query.getResultRowPostAggregatorStart(); i++) {
              rowWithPostAggregations.set(i, row.get(i));
            }

            // Compute postaggregations. We need to do this with a result-row map because PostAggregator.compute
            // expects a map. Some further design adjustment may eliminate the need for it, and speed up this function.
            final Map<String, Object> mapForPostAggregationComputation = rowWithPostAggregations.toMap(query);

            for (int i = 0; i < query.getPostAggregatorSpecs().size(); i++) {
              final PostAggregator postAggregator = query.getPostAggregatorSpecs().get(i);
              final Object value = postAggregator.compute(mapForPostAggregationComputation);

              rowWithPostAggregations.set(query.getResultRowPostAggregatorStart() + i, value);
              mapForPostAggregationComputation.put(postAggregator.getName(), value);
            }

            return rowWithPostAggregations;
          }
      );
    }
  }

  @Override
  public Sequence<ResultRow> applyPostProcessing(Sequence<ResultRow> results, GroupByQuery query)
  {
    // Don't apply limit here for inner results, that will be pushed down to the BufferHashGrouper
    if (query.getContextBoolean(CTX_KEY_OUTERMOST, true)) {
      return query.postProcess(results);
    } else {
      return results;
    }
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
    // Keep a reference to resultSupplier outside the "try" so we can close it if something goes wrong
    // while creating the sequence.
    GroupByRowProcessor.ResultSupplier resultSupplier = null;

    try {
      final GroupByQuery queryToRun;

      if (wasQueryPushedDown) {
        // If the query was pushed down, filters would have been applied downstream, so skip it here.
        queryToRun = query.withDimFilter(null)
                          .withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY));
      } else {
        queryToRun = query;
      }

      resultSupplier = GroupByRowProcessor.process(
          queryToRun,
          wasQueryPushedDown ? queryToRun : subquery,
          subqueryResult,
          configSupplier.get(),
          resource,
          spillMapper,
          processingConfig.getTmpDir(),
          processingConfig.intermediateComputeSizeBytes()
      );

      final GroupByRowProcessor.ResultSupplier finalResultSupplier = resultSupplier;
      return Sequences.withBaggage(
          mergeResults(
              (queryPlus, responseContext) -> finalResultSupplier.results(null),
              query,
              null
          ),
          finalResultSupplier
      );
    }
    catch (Exception ex) {
      CloseQuietly.close(resultSupplier);
      throw ex;
    }
  }

  @Override
  public Sequence<ResultRow> processSubtotalsSpec(
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<ResultRow> queryResult
  )
  {
    // Note: the approach used here is not always correct; see https://github.com/apache/incubator-druid/issues/8091.

    // Keep a reference to resultSupplier outside the "try" so we can close it if something goes wrong
    // while creating the sequence.
    GroupByRowProcessor.ResultSupplier resultSupplier = null;

    try {
      GroupByQuery queryWithoutSubtotalsSpec = query.withSubtotalsSpec(null).withDimFilter(null);
      List<List<String>> subtotals = query.getSubtotalsSpec();

      resultSupplier = GroupByRowProcessor.process(
          queryWithoutSubtotalsSpec
              .withAggregatorSpecs(
                  Lists.transform(
                      queryWithoutSubtotalsSpec.getAggregatorSpecs(),
                      AggregatorFactory::getCombiningFactory
                  )
              )
              .withDimensionSpecs(
                  Lists.transform(
                      queryWithoutSubtotalsSpec.getDimensions(),
                      dimSpec ->
                          new DefaultDimensionSpec(
                              dimSpec.getOutputName(),
                              dimSpec.getOutputName(),
                              dimSpec.getOutputType()
                          )
                  )
              ),
          queryWithoutSubtotalsSpec,
          queryResult,
          configSupplier.get(),
          resource,
          spillMapper,
          processingConfig.getTmpDir(),
          processingConfig.intermediateComputeSizeBytes()
      );
      List<Sequence<ResultRow>> subtotalsResults = new ArrayList<>(subtotals.size());

      for (List<String> subtotalSpec : subtotals) {
        final ImmutableSet<String> dimsInSubtotalSpec = ImmutableSet.copyOf(subtotalSpec);
        final List<DimensionSpec> dimensions = query.getDimensions();
        final List<DimensionSpec> newDimensions = new ArrayList<>();

        for (int i = 0; i < dimensions.size(); i++) {
          DimensionSpec dimensionSpec = dimensions.get(i);
          if (dimsInSubtotalSpec.contains(dimensionSpec.getOutputName())) {
            newDimensions.add(
                new DefaultDimensionSpec(
                    dimensionSpec.getOutputName(),
                    dimensionSpec.getOutputName(),
                    dimensionSpec.getOutputType()
                )
            );
          } else {
            // Insert dummy dimension so all subtotals queries have ResultRows with the same shape.
            // Use a field name that does not appear in the main query result, to assure the result will be null.
            String dimName = "_" + i;
            while (query.getResultRowPositionLookup().getInt(dimName) >= 0) {
              dimName = "_" + dimName;
            }
            newDimensions.add(DefaultDimensionSpec.of(dimName));
          }
        }

        GroupByQuery subtotalQuery = queryWithoutSubtotalsSpec.withDimensionSpecs(newDimensions);

        final GroupByRowProcessor.ResultSupplier finalResultSupplier = resultSupplier;
        subtotalsResults.add(
            applyPostProcessing(
                mergeResults(
                    (queryPlus, responseContext) -> finalResultSupplier.results(subtotalSpec),
                    subtotalQuery,
                    null
                ),
                subtotalQuery
            )
        );
      }

      return Sequences.withBaggage(
          Sequences.concat(subtotalsResults),
          resultSupplier
      );
    }
    catch (Exception ex) {
      CloseQuietly.close(resultSupplier);
      throw ex;
    }
  }

  @Override
  public QueryRunner<ResultRow> mergeRunners(
      final ListeningExecutorService exec,
      final Iterable<QueryRunner<ResultRow>> queryRunners
  )
  {
    return new GroupByMergingQueryRunnerV2(
        configSupplier.get(),
        exec,
        queryWatcher,
        queryRunners,
        processingConfig.getNumThreads(),
        mergeBufferPool,
        processingConfig.intermediateComputeSizeBytes(),
        spillMapper,
        processingConfig.getTmpDir()
    );
  }

  @Override
  public Sequence<ResultRow> process(GroupByQuery query, StorageAdapter storageAdapter)
  {
    return GroupByQueryEngineV2.process(query, storageAdapter, bufferPool, configSupplier.get().withOverrides(query));
  }

  @Override
  public boolean supportsNestedQueryPushDown()
  {
    return true;
  }
}
