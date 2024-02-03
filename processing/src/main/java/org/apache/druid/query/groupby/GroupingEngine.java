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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.ResultMergeQueryRunner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.epinephelinae.BufferArrayGrouper;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunner;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngine;
import org.apache.druid.query.groupby.epinephelinae.GroupByResultMergeFn;
import org.apache.druid.query.groupby.epinephelinae.GroupByRowProcessor;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.filter.AllNullColumnSelectorFactory;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * Common code for processing {@link GroupByQuery}.
 */
public class GroupingEngine
{
  public static final String CTX_KEY_FUDGE_TIMESTAMP = "fudgeTimestamp";
  public static final String CTX_KEY_OUTERMOST = "groupByOutermost";

  private final DruidProcessingConfig processingConfig;
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final NonBlockingPool<ByteBuffer> bufferPool;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper spillMapper;
  private final QueryWatcher queryWatcher;

  @Inject
  public GroupingEngine(
      DruidProcessingConfig processingConfig,
      Supplier<GroupByQueryConfig> configSupplier,
      @Global NonBlockingPool<ByteBuffer> bufferPool,
      @Merging BlockingPool<ByteBuffer> mergeBufferPool,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper spillMapper,
      QueryWatcher queryWatcher
  )
  {
    this.processingConfig = processingConfig;
    this.configSupplier = configSupplier;
    this.bufferPool = bufferPool;
    this.mergeBufferPool = mergeBufferPool;
    this.jsonMapper = jsonMapper;
    this.spillMapper = spillMapper;
    this.queryWatcher = queryWatcher;
  }

  /**
   * Initializes resources required to run {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)} for a
   * particular query. That method is also the primary caller of this method.
   *
   * Used by {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)}.
   *
   * @param query a groupBy query to be processed
   *
   * @return broker resource
   */
  public GroupByQueryResources prepareResource(GroupByQuery query)
  {
    final int requiredMergeBufferNum = GroupByQueryResources.countRequiredMergeBufferNum(query);

    if (requiredMergeBufferNum > mergeBufferPool.maxSize()) {
      throw new ResourceLimitExceededException(
          "Query needs " + requiredMergeBufferNum + " merge buffers, but only "
          + mergeBufferPool.maxSize() + " merge buffers were configured"
      );
    } else if (requiredMergeBufferNum == 0) {
      return new GroupByQueryResources();
    } else {
      final List<ReferenceCountingResourceHolder<ByteBuffer>> mergeBufferHolders;
      final QueryContext context = query.context();
      if (context.hasTimeout()) {
        mergeBufferHolders = mergeBufferPool.takeBatch(requiredMergeBufferNum, context.getTimeout());
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
        return new GroupByQueryResources(mergeBufferHolders);
      }
    }
  }

  /**
   * See {@link org.apache.druid.query.QueryToolChest#createResultComparator(Query)}, allows
   * {@link GroupByQueryQueryToolChest} to delegate implementation to the strategy
   */
  public Comparator<ResultRow> createResultComparator(Query<ResultRow> queryParam)
  {
    return ((GroupByQuery) queryParam).getRowOrdering(true);
  }

  /**
   * See {@link org.apache.druid.query.QueryToolChest#createMergeFn(Query)} for details, allows
   * {@link GroupByQueryQueryToolChest} to delegate implementation to the strategy
   */
  public BinaryOperator<ResultRow> createMergeFn(Query<ResultRow> queryParam)
  {
    return new GroupByResultMergeFn((GroupByQuery) queryParam);
  }

  public GroupByQuery prepareGroupByQuery(GroupByQuery query)
  {
    // Set up downstream context.
    final ImmutableMap.Builder<String, Object> context = ImmutableMap.builder();
    context.put(QueryContexts.FINALIZE_KEY, false);
    context.put(CTX_KEY_OUTERMOST, false);

    Granularity granularity = query.getGranularity();
    List<DimensionSpec> dimensionSpecs = query.getDimensions();
    // the CTX_TIMESTAMP_RESULT_FIELD is set in DruidQuery.java
    final QueryContext queryContext = query.context();
    final String timestampResultField = queryContext.getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD);
    final boolean hasTimestampResultField = (timestampResultField != null && !timestampResultField.isEmpty())
                                            && queryContext.getBoolean(CTX_KEY_OUTERMOST, true)
                                            && !query.isApplyLimitPushDown();
    if (hasTimestampResultField) {
      // sql like "group by city_id,time_floor(__time to day)",
      // the original translated query is granularity=all and dimensions:[d0, d1]
      // the better plan is granularity=day and dimensions:[d0]
      // but the ResultRow structure is changed from [d0, d1] to [__time, d0]
      // this structure should be fixed as [d0, d1] (actually it is [d0, __time]) before postAggs are called.
      //
      // the above is the general idea of this optimization.
      // but from coding perspective, the granularity=all and "d0" dimension are referenced by many places,
      // eg: subtotals, having, grouping set, post agg,
      // there would be many many places need to be fixed if "d0" dimension is removed from query.dimensions
      // and the same to the granularity change.
      // so from easier coding perspective, this optimization is coded as groupby engine-level inner process change.
      // the most part of codes are in GroupByStrategyV2 about the process change between broker and compute node.
      // the basic logic like nested queries and subtotals are kept unchanged,
      // they will still see the granularity=all and the "d0" dimension.
      //
      // the tradeoff is that GroupByStrategyV2 behaviors differently according to the query contexts set in DruidQuery
      // in another word,
      // the query generated by "explain plan for select ..." doesn't match to the native query ACTUALLY being executed,
      // the granularity and dimensions are slightly different.
      // now, part of the query plan logic is handled in GroupByStrategyV2, not only in DruidQuery.toGroupByQuery()
      final Granularity timestampResultFieldGranularity
          = queryContext.getGranularity(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY, jsonMapper);
      dimensionSpecs =
          query.getDimensions()
               .stream()
               .filter(dimensionSpec -> !dimensionSpec.getOutputName().equals(timestampResultField))
               .collect(Collectors.toList());
      granularity = timestampResultFieldGranularity;
      // when timestampResultField is the last dimension, should set sortByDimsFirst=true,
      // otherwise the downstream is sorted by row's timestamp first which makes the final ordering not as expected
      int timestampResultFieldIndex = queryContext.getInt(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_INDEX, 0);
      if (!query.getContextSortByDimsFirst() && timestampResultFieldIndex == query.getDimensions().size() - 1) {
        context.put(GroupByQuery.CTX_KEY_SORT_BY_DIMS_FIRST, true);
      }
      // when timestampResultField is the first dimension and sortByDimsFirst=true,
      // it is actually equals to sortByDimsFirst=false
      if (query.getContextSortByDimsFirst() && timestampResultFieldIndex == 0) {
        context.put(GroupByQuery.CTX_KEY_SORT_BY_DIMS_FIRST, false);
      }
      // when hasTimestampResultField=true and timestampResultField is neither first nor last dimension,
      // the DefaultLimitSpec will always do the reordering
    }
    if (query.getUniversalTimestamp() != null && !hasTimestampResultField) {
      // universalTimestamp works only when granularity is all
      // hasTimestampResultField works only when granularity is all
      // fudgeTimestamp should not be used when hasTimestampResultField=true due to the row's actual timestamp is used
      context.put(CTX_KEY_FUDGE_TIMESTAMP, String.valueOf(query.getUniversalTimestamp().getMillis()));
    }

    // The having spec shouldn't be passed down, so we need to convey the existing limit push down status
    context.put(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, query.isApplyLimitPushDown());

    // Always request array result rows when passing the query downstream.
    context.put(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true);

    return new GroupByQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        query.getVirtualColumns(),
        query.getDimFilter(),
        granularity,
        dimensionSpecs,
        query.getAggregatorSpecs(),
        // Don't apply postaggregators on compute nodes
        ImmutableList.of(),
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
  }

  /**
   * Runs a provided {@link QueryRunner} on a provided {@link GroupByQuery}, which is assumed to return rows that are
   * properly sorted (by timestamp and dimensions) but not necessarily fully merged (that is, there may be adjacent
   * rows with the same timestamp and dimensions) and without PostAggregators computed. This method will fully merge
   * the rows, apply PostAggregators, and return the resulting {@link Sequence}.
   *
   * The query will be modified using {@link #prepareGroupByQuery(GroupByQuery)} before passing it down to the base
   * runner. For example, "having" clauses will be removed and various context parameters will be adjusted.
   *
   * Despite the similar name, this method is much reduced in scope compared to
   * {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)}. That method does delegate to this one at some points,
   * but has a truckload of other responsibility, including computing outer query results (if there are subqueries),
   * computing subtotals (like GROUPING SETS), and computing the havingSpec and limitSpec.
   *
   * @param baseRunner      base query runner
   * @param query           the groupBy query to run inside the base query runner
   * @param responseContext the response context to pass to the base query runner
   *
   * @return merged result sequence
   */
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

    final QueryContext queryContext = query.context();
    final String timestampResultField = queryContext.getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD);
    final boolean hasTimestampResultField = (timestampResultField != null && !timestampResultField.isEmpty())
                                            && queryContext.getBoolean(CTX_KEY_OUTERMOST, true)
                                            && !query.isApplyLimitPushDown();
    final int timestampResultFieldIndexInOriginalDimensions = hasTimestampResultField ? queryContext.getInt(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_INDEX) : 0;
    final GroupByQuery newQuery = prepareGroupByQuery(query);

    final Sequence<ResultRow> mergedResults = mergingQueryRunner.run(QueryPlus.wrap(newQuery), responseContext);

    // Apply postaggregators if this is the outermost mergeResults (CTX_KEY_OUTERMOST) and we are not executing a
    // pushed-down subquery (CTX_KEY_EXECUTING_NESTED_QUERY).

    if (!queryContext.getBoolean(CTX_KEY_OUTERMOST, true)
        || queryContext.getBoolean(GroupByQueryConfig.CTX_KEY_EXECUTING_NESTED_QUERY, false)) {
      return mergedResults;
    } else if (query.getPostAggregatorSpecs().isEmpty()) {
      if (!hasTimestampResultField) {
        return mergedResults;
      }
      return Sequences.map(
          mergedResults,
          row -> {
            final ResultRow resultRow = ResultRow.create(query.getResultRowSizeWithoutPostAggregators());
            moveOrReplicateTimestampInRow(
                query,
                timestampResultFieldIndexInOriginalDimensions,
                row,
                resultRow
            );

            return resultRow;
          }
      );
    } else {
      return Sequences.map(
          mergedResults,
          row -> {
            // This function's purpose is to apply PostAggregators.

            final ResultRow rowWithPostAggregations = ResultRow.create(query.getResultRowSizeWithPostAggregators());

            // Copy everything that comes before the postaggregations.
            if (hasTimestampResultField) {
              moveOrReplicateTimestampInRow(
                  query,
                  timestampResultFieldIndexInOriginalDimensions,
                  row,
                  rowWithPostAggregations
              );
            } else {
              for (int i = 0; i < query.getResultRowPostAggregatorStart(); i++) {
                rowWithPostAggregations.set(i, row.get(i));
              }
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

  /**
   * Merge a variety of single-segment query runners into a combined runner. Used by
   * {@link GroupByQueryRunnerFactory#mergeRunners(QueryProcessingPool, Iterable)}. In
   * that sense, it is intended to go along with {@link #process(GroupByQuery, StorageAdapter, GroupByQueryMetrics)} (the runners created
   * by that method will be fed into this method).
   * <p>
   * This method is only called on data servers, like Historicals (not the Broker).
   *
   * @param queryProcessingPool {@link QueryProcessingPool} service used for parallel execution of the query runners
   * @param queryRunners  collection of query runners to merge
   * @return merged query runner
   */
  public QueryRunner<ResultRow> mergeRunners(
      final QueryProcessingPool queryProcessingPool,
      final Iterable<QueryRunner<ResultRow>> queryRunners
  )
  {
    return new GroupByMergingQueryRunner(
        configSupplier.get(),
        processingConfig,
        queryProcessingPool,
        queryWatcher,
        queryRunners,
        processingConfig.getNumThreads(),
        mergeBufferPool,
        processingConfig.intermediateComputeSizeBytes(),
        spillMapper,
        processingConfig.getTmpDir()
    );
  }

  /**
   * Process a groupBy query on a single {@link StorageAdapter}. This is used by
   * {@link GroupByQueryRunnerFactory#createRunner} to create per-segment
   * QueryRunners.
   *
   * This method is only called on data servers, like Historicals (not the Broker).
   *
   * @param query          the groupBy query
   * @param storageAdapter storage adatper for the segment in question
   *
   * @return result sequence for the storage adapter
   */
  public Sequence<ResultRow> process(
      GroupByQuery query,
      StorageAdapter storageAdapter,
      @Nullable GroupByQueryMetrics groupByQueryMetrics
  )
  {
    final GroupByQueryConfig querySpecificConfig = configSupplier.get().withOverrides(query);

    if (storageAdapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }

    final ResourceHolder<ByteBuffer> bufferHolder = bufferPool.take();

    try {
      final String fudgeTimestampString = NullHandling.emptyToNullIfNeeded(
          query.context().getString(GroupingEngine.CTX_KEY_FUDGE_TIMESTAMP)
      );

      final DateTime fudgeTimestamp = fudgeTimestampString == null
                                      ? null
                                      : DateTimes.utc(Long.parseLong(fudgeTimestampString));

      final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));
      final Interval interval = Iterables.getOnlyElement(query.getIntervals());

      final boolean doVectorize = query.context().getVectorize().shouldVectorize(
          VectorGroupByEngine.canVectorize(query, storageAdapter, filter)
      );

      final Sequence<ResultRow> result;

      if (doVectorize) {
        result = VectorGroupByEngine.process(
            query,
            storageAdapter,
            bufferHolder.get(),
            fudgeTimestamp,
            filter,
            interval,
            querySpecificConfig,
            processingConfig,
            groupByQueryMetrics
        );
      } else {
        result = GroupByQueryEngine.process(
            query,
            storageAdapter,
            bufferHolder.get(),
            fudgeTimestamp,
            querySpecificConfig,
            processingConfig,
            filter,
            interval,
            groupByQueryMetrics
        );
      }

      return result.withBaggage(bufferHolder);
    }
    catch (Throwable e) {
      bufferHolder.close();
      throw e;
    }
  }

  /**
   * Apply the {@link GroupByQuery} "postProcessingFn", which is responsible for HavingSpec and LimitSpec.
   *
   * @param results sequence of results
   * @param query   the groupBy query
   *
   * @return post-processed results, with HavingSpec and LimitSpec applied
   */
  public Sequence<ResultRow> applyPostProcessing(Sequence<ResultRow> results, GroupByQuery query)
  {
    results = wrapSummaryRowIfNeeded(query, results);

    // Don't apply limit here for inner results, that will be pushed down to the BufferHashGrouper
    if (query.context().getBoolean(CTX_KEY_OUTERMOST, true)) {
      return query.postProcess(results);
    } else {
      return results;
    }
  }

  /**
   * Called by {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)} when it needs to process a subquery.
   *
   * @param subquery           inner query
   * @param query              outer query
   * @param resource           resources returned by {@link #prepareResource(GroupByQuery)}
   * @param subqueryResult     result rows from the subquery
   * @param wasQueryPushedDown true if the outer query was pushed down (so we only need to merge the outer query's
   *                           results, not run it from scratch like a normal outer query)
   *
   * @return results of the outer query
   */
  public Sequence<ResultRow> processSubqueryResult(
      GroupByQuery subquery,
      GroupByQuery query,
      GroupByQueryResources resource,
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
          processingConfig,
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
              ResponseContext.createEmpty()
          ),
          finalResultSupplier
      );
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, resultSupplier);
    }
  }

  /**
   * Called by {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)} when it needs to generate subtotals.
   *
   * @param query       query that has a "subtotalsSpec"
   * @param resource    resources returned by {@link #prepareResource(GroupByQuery)}
   * @param queryResult result rows from the main query
   *
   * @return results for each list of subtotals in the query, concatenated together
   */
  public Sequence<ResultRow> processSubtotalsSpec(
      GroupByQuery query,
      GroupByQueryResources resource,
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
          .withSubtotalsSpec(null)
          // timestampResult optimization is not for subtotal scenario, so disable it
          .withOverriddenContext(ImmutableMap.of(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD, ""));

      resultSupplierOne = GroupByRowProcessor.process(
          baseSubtotalQuery,
          baseSubtotalQuery,
          queryResult,
          configSupplier.get(),
          processingConfig,
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
              processingConfig,
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
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, resultSupplierOne);
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
                      closeOnSequenceRead
                      ? () -> CloseableUtils.closeAndWrapExceptions(memoizedSupplier.get())
                      : () -> {}
                  )
              ),
          subtotalQuery,
          ResponseContext.createEmpty()
      );
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, baseResultsSupplier.get());
    }
  }

  private void moveOrReplicateTimestampInRow(
      GroupByQuery query,
      int timestampResultFieldIndexInOriginalDimensions,
      ResultRow before,
      ResultRow after
  )
  {
    // d1 is the __time
    // when query.granularity=all:  convert [__time, d0] to [d0, d1] (actually, [d0, __time])
    // when query.granularity!=all: convert [__time, d0] to [__time, d0, d1] (actually, [__time, d0, __time])
    // overall, insert the removed d1 at the position where it is removed and remove the first __time if granularity=all
    Object theTimestamp = before.get(0);
    int expectedDimensionStartInAfterRow = 0;
    if (query.getResultRowHasTimestamp()) {
      expectedDimensionStartInAfterRow = 1;
      after.set(0, theTimestamp);
    }
    int timestampResultFieldIndexInAfterRow = timestampResultFieldIndexInOriginalDimensions + expectedDimensionStartInAfterRow;
    for (int i = expectedDimensionStartInAfterRow; i < timestampResultFieldIndexInAfterRow; i++) {
      // 0 in beforeRow is the timestamp, so plus 1 is the start of dimension in beforeRow
      after.set(i, before.get(i + 1));
    }
    after.set(timestampResultFieldIndexInAfterRow, theTimestamp);
    for (int i = timestampResultFieldIndexInAfterRow + 1; i < before.length() + expectedDimensionStartInAfterRow; i++) {
      after.set(i, before.get(i - expectedDimensionStartInAfterRow));
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


  /**
   * Returns the cardinality of array needed to do array-based aggregation, or -1 if array-based aggregation
   * is impossible.
   */
  public static int getCardinalityForArrayAggregation(
      GroupByQueryConfig querySpecificConfig,
      GroupByQuery query,
      StorageAdapter storageAdapter,
      ByteBuffer buffer
  )
  {
    if (querySpecificConfig.isForceHashAggregation()) {
      return -1;
    }

    final List<DimensionSpec> dimensions = query.getDimensions();
    final ColumnCapabilities columnCapabilities;
    final int cardinality;

    // Find cardinality
    if (dimensions.isEmpty()) {
      columnCapabilities = null;
      cardinality = 1;
    } else if (dimensions.size() == 1) {
      // Only real columns can use array-based aggregation, since virtual columns cannot currently report their
      // cardinality. We need to check if a virtual column exists with the same name, since virtual columns can shadow
      // real columns, and we might miss that since we're going directly to the StorageAdapter (which only knows about
      // real columns).
      if (query.getVirtualColumns().exists(Iterables.getOnlyElement(dimensions).getDimension())) {
        return -1;
      }
      // We cannot support array-based aggregation on array based grouping as we we donot have all the indexes up front
      // to allocate appropriate values
      if (dimensions.get(0).getOutputType().isArray()) {
        return -1;
      }

      final String columnName = Iterables.getOnlyElement(dimensions).getDimension();
      columnCapabilities = storageAdapter.getColumnCapabilities(columnName);
      cardinality = storageAdapter.getDimensionCardinality(columnName);
    } else {
      // Cannot use array-based aggregation with more than one dimension.
      return -1;
    }

    // Choose array-based aggregation if the grouping key is a single string dimension of a known cardinality
    if (Types.is(columnCapabilities, ValueType.STRING) && cardinality > 0) {
      final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs().toArray(new AggregatorFactory[0]);
      final long requiredBufferCapacity = BufferArrayGrouper.requiredBufferCapacity(
          cardinality,
          aggregatorFactories
      );

      // Check that all keys and aggregated values can be contained in the buffer
      if (requiredBufferCapacity < 0 || requiredBufferCapacity > buffer.capacity()) {
        return -1;
      } else {
        return cardinality;
      }
    } else {
      return -1;
    }
  }

  public static void convertRowTypesToOutputTypes(
      final List<DimensionSpec> dimensionSpecs,
      final ResultRow resultRow,
      final int resultRowDimensionStart
  )
  {
    for (int i = 0; i < dimensionSpecs.size(); i++) {
      DimensionSpec dimSpec = dimensionSpecs.get(i);
      final int resultRowIndex = resultRowDimensionStart + i;
      final ColumnType outputType = dimSpec.getOutputType();

      resultRow.set(
          resultRowIndex,
          DimensionHandlerUtils.convertObjectToType(resultRow.get(resultRowIndex), outputType)
      );
    }
  }

  /**
   * Wraps the sequence around if for this query a summary row might be needed in case the input becomes empty.
   */
  public static Sequence<ResultRow> wrapSummaryRowIfNeeded(GroupByQuery query, Sequence<ResultRow> process)
  {
    if (!summaryRowPreconditions(query)) {
      return process;
    }

    final AtomicBoolean t = new AtomicBoolean();

    return Sequences.concat(
        Sequences.map(process, ent -> {
          t.set(true);
          return ent;
        }),
        Sequences.simple(() -> {
          if (t.get()) {
            return Collections.emptyIterator();
          }
          return summaryRowIterator(query);
        }));
  }

  private static boolean summaryRowPreconditions(GroupByQuery query)
  {
    LimitSpec limit = query.getLimitSpec();
    if (limit instanceof DefaultLimitSpec) {
      DefaultLimitSpec limitSpec = (DefaultLimitSpec) limit;
      if (limitSpec.getLimit() == 0 || limitSpec.getOffset() > 0) {
        return false;
      }
    }
    if (!query.getDimensions().isEmpty()) {
      return false;
    }
    if (query.getGranularity().isFinerThan(Granularities.ALL)) {
      return false;
    }
    return true;
  }

  private static Iterator<ResultRow> summaryRowIterator(GroupByQuery q)
  {
    List<AggregatorFactory> aggSpec = q.getAggregatorSpecs();
    ResultRow resultRow = ResultRow.create(q.getResultRowSizeWithPostAggregators());
    for (int i = 0; i < aggSpec.size(); i++) {
      resultRow.set(
          q.getResultRowAggregatorStart() + i,
          aggSpec.get(i).factorize(new AllNullColumnSelectorFactory()).get()
      );
    }
    Map<String, Object> map = resultRow.toMap(q);
    for (int i = 0; i < q.getPostAggregatorSpecs().size(); i++) {
      final PostAggregator postAggregator = q.getPostAggregatorSpecs().get(i);
      final Object value = postAggregator.compute(map);

      resultRow.set(q.getResultRowPostAggregatorStart() + i, value);
      map.put(postAggregator.getName(), value);
    }
    return Collections.singleton(resultRow).iterator();
  }

}
