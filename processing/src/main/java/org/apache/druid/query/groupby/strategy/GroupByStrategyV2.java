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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
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
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByBinaryFnV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByRowProcessor;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

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
                                       numMergeBuffersNeededForSubtotalsSpec(query);

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
        throw QueryCapacityExceededException.withErrorMessageAndResolvedHost(
            StringUtils.format(
                "Cannot acquire %s merge buffers. Try again after current running queries are finished.",
                requiredMergeBufferNum
            )
        );
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
        // Potentially pass limit down the stack (i.e. limit pushdown). Notes:
        //   (1) Limit pushdown is only supported for DefaultLimitSpec.
        //   (2) When pushing down a limit, it must be extended to include the offset (the offset will be applied
        //       higher-up).
        query.isApplyLimitPushDown() ? ((DefaultLimitSpec) query.getLimitSpec()).withOffsetToLimit() : null,
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
    // How it works?
    // First we accumulate the result of top level base query aka queryResult arg inside a resultSupplierOne object.
    // Next for each subtotalSpec
    //   If subtotalSpec is a prefix of top level dims then we iterate on rows in resultSupplierOne object which are still
    //   sorted by subtotalSpec, stream merge them and return.
    //
    //   If subtotalSpec is not a prefix of top level dims then we create a resultSupplierTwo object filled with rows from
    //   resultSupplierOne object with only dims from subtotalSpec. Then we iterate on rows in resultSupplierTwo object which are
    //   of course sorted by subtotalSpec, stream merge them and return.

    // Keep a reference to resultSupplier outside the "try" so we can close it if something goes wrong
    // while creating the sequence.
    GroupByRowProcessor.ResultSupplier resultSupplierOne = null;

    try {
      // baseSubtotalQuery is the original query with dimensions and aggregators rewritten to apply to the *results*
      // rather than *inputs* of that query. It has its virtual columns and dim filter removed, because those only
      // make sense when applied to inputs. Finally, it has subtotalsSpec removed, since we'll be computing them
      // one-by-one soon enough.
      GroupByQuery baseSubtotalQuery = query
          .withDimensionSpecs(query.getDimensions().stream().map(
              dimSpec -> new DefaultDimensionSpec(
                  dimSpec.getOutputName(),
                  dimSpec.getOutputName(),
                  dimSpec.getOutputType()
              )).collect(Collectors.toList())
          )
          .withAggregatorSpecs(
              query.getAggregatorSpecs()
                   .stream()
                   .map(AggregatorFactory::getCombiningFactory)
                   .collect(Collectors.toList())
          )
          .withVirtualColumns(VirtualColumns.EMPTY)
          .withDimFilter(null)
          .withSubtotalsSpec(null);

      resultSupplierOne = GroupByRowProcessor.process(
          baseSubtotalQuery,
          baseSubtotalQuery,
          queryResult,
          configSupplier.get(),
          resource,
          spillMapper,
          processingConfig.getTmpDir(),
          processingConfig.intermediateComputeSizeBytes()
      );

      List<String> queryDimNames = baseSubtotalQuery.getDimensions().stream().map(DimensionSpec::getOutputName)
                                                    .collect(Collectors.toList());

      // Only needed to make LimitSpec.filterColumns(..) call later in case base query has a non default LimitSpec.
      Set<String> aggsAndPostAggs = null;
      if (!(baseSubtotalQuery.getLimitSpec() instanceof NoopLimitSpec)) {
        aggsAndPostAggs = getAggregatorAndPostAggregatorNames(baseSubtotalQuery);
      }

      List<List<String>> subtotals = query.getSubtotalsSpec();
      List<Sequence<ResultRow>> subtotalsResults = new ArrayList<>(subtotals.size());

      // Iterate through each subtotalSpec, build results for it and add to subtotalsResults
      for (List<String> subtotalSpec : subtotals) {
        final ImmutableSet<String> dimsInSubtotalSpec = ImmutableSet.copyOf(subtotalSpec);
        // Dimension spec including dimension name and output name
        final List<DimensionSpec> subTotalDimensionSpec = new ArrayList<>(dimsInSubtotalSpec.size());
        final List<DimensionSpec> dimensions = query.getDimensions();

        for (DimensionSpec dimensionSpec : dimensions) {
          if (dimsInSubtotalSpec.contains(dimensionSpec.getOutputName())) {
            subTotalDimensionSpec.add(dimensionSpec);
          }
        }

        // Create appropriate LimitSpec for subtotal query
        LimitSpec subtotalQueryLimitSpec = NoopLimitSpec.instance();
        if (!(baseSubtotalQuery.getLimitSpec() instanceof NoopLimitSpec)) {
          Set<String> columns = new HashSet<>(aggsAndPostAggs);
          columns.addAll(subtotalSpec);

          subtotalQueryLimitSpec = baseSubtotalQuery.getLimitSpec().filterColumns(columns);
        }

        GroupByQuery subtotalQuery = baseSubtotalQuery
            .withLimitSpec(subtotalQueryLimitSpec);

        final GroupByRowProcessor.ResultSupplier resultSupplierOneFinal = resultSupplierOne;
        if (Utils.isPrefix(subtotalSpec, queryDimNames)) {
          // Since subtotalSpec is a prefix of base query dimensions, so results from base query are also sorted
          // by subtotalSpec as needed by stream merging.
          subtotalsResults.add(
              processSubtotalsResultAndOptionallyClose(() -> resultSupplierOneFinal, subTotalDimensionSpec, subtotalQuery, false)
          );
        } else {
          // Since subtotalSpec is not a prefix of base query dimensions, so results from base query are not sorted
          // by subtotalSpec. So we first add the result of base query into another resultSupplier which are sorted
          // by subtotalSpec and then stream merge them.

          // Also note, we can't create the ResultSupplier eagerly here or as we don't want to eagerly allocate
          // merge buffers for processing subtotal.
          Supplier<GroupByRowProcessor.ResultSupplier> resultSupplierTwo = () -> GroupByRowProcessor.process(
              baseSubtotalQuery,
              subtotalQuery,
              resultSupplierOneFinal.results(subTotalDimensionSpec),
              configSupplier.get(),
              resource,
              spillMapper,
              processingConfig.getTmpDir(),
              processingConfig.intermediateComputeSizeBytes()
          );

          subtotalsResults.add(
              processSubtotalsResultAndOptionallyClose(resultSupplierTwo, subTotalDimensionSpec, subtotalQuery, true)
          );
        }
      }

      return Sequences.withBaggage(
          query.postProcess(Sequences.concat(subtotalsResults)),
          resultSupplierOne //this will close resources allocated by resultSupplierOne after sequence read
      );
    }
    catch (Exception ex) {
      CloseQuietly.close(resultSupplierOne);
      throw ex;
    }
  }

  private Sequence<ResultRow> processSubtotalsResultAndOptionallyClose(
      Supplier<GroupByRowProcessor.ResultSupplier> baseResultsSupplier,
      List<DimensionSpec> dimsToInclude,
      GroupByQuery subtotalQuery,
      boolean closeOnSequenceRead
  )
  {
    // This closes the ResultSupplier in case of any exception here or arranges for it to be closed
    // on sequence read if closeOnSequenceRead is true.
    try {
      Supplier<GroupByRowProcessor.ResultSupplier> memoizedSupplier = Suppliers.memoize(baseResultsSupplier);
      return mergeResults(
          (queryPlus, responseContext) ->
              new LazySequence<>(
                  () -> Sequences.withBaggage(
                      memoizedSupplier.get().results(dimsToInclude),
                      closeOnSequenceRead ? () -> CloseQuietly.close(memoizedSupplier.get()) : () -> {}
                  )
              ),
          subtotalQuery,
          null
      );
    }
    catch (Exception ex) {
      CloseQuietly.close(baseResultsSupplier.get());
      throw ex;
    }
  }

  private Set<String> getAggregatorAndPostAggregatorNames(GroupByQuery query)
  {
    Set<String> aggsAndPostAggs = new HashSet();
    if (query.getAggregatorSpecs() != null) {
      for (AggregatorFactory af : query.getAggregatorSpecs()) {
        aggsAndPostAggs.add(af.getName());
      }
    }

    if (query.getPostAggregatorSpecs() != null) {
      for (PostAggregator pa : query.getPostAggregatorSpecs()) {
        aggsAndPostAggs.add(pa.getName());
      }
    }

    return aggsAndPostAggs;
  }

  private int numMergeBuffersNeededForSubtotalsSpec(GroupByQuery query)
  {
    List<List<String>> subtotalSpecs = query.getSubtotalsSpec();
    if (subtotalSpecs == null || subtotalSpecs.size() == 0) {
      return 0;
    }

    List<String> queryDimOutputNames = query.getDimensions().stream().map(DimensionSpec::getOutputName).collect(
        Collectors.toList());
    for (List<String> subtotalSpec : subtotalSpecs) {
      if (!Utils.isPrefix(subtotalSpec, queryDimOutputNames)) {
        return 2;
      }
    }

    return 1;
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
    return GroupByQueryEngineV2.process(
        query,
        storageAdapter,
        bufferPool,
        configSupplier.get().withOverrides(query)
    );
  }

  @Override
  public boolean supportsNestedQueryPushDown()
  {
    return true;
  }
}
