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
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
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
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryHelper;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.epinephelinae.GroupByBinaryFnV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByRowProcessor;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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

  /**
   * If "query" has a single universal timestamp, return it. Otherwise return null. This is useful
   * for keeping timestamps in sync across partial queries that may have different intervals.
   *
   * @param query the query
   *
   * @return universal timestamp, or null
   */
  public static DateTime getUniversalTimestamp(final GroupByQuery query)
  {
    final Granularity gran = query.getGranularity();
    final String timestampStringFromContext = query.getContextValue(CTX_KEY_FUDGE_TIMESTAMP, "");

    if (!timestampStringFromContext.isEmpty()) {
      return DateTimes.utc(Long.parseLong(timestampStringFromContext));
    } else if (Granularities.ALL.equals(gran)) {
      final DateTime timeStart = query.getIntervals().get(0).getStart();
      return gran.getIterable(new Interval(timeStart, timeStart.plus(1))).iterator().next().getStart();
    } else {
      return null;
    }
  }

  @Override
  public GroupByQueryResource prepareResource(GroupByQuery query, boolean willMergeRunners)
  {
    if (!willMergeRunners) {
      final int requiredMergeBufferNum = countRequiredMergeBufferNum(query, 1) +
                                         numMergeBuffersForSubtotalsSpec(query);

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
    } else {
      return new GroupByQueryResource();
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
  public QueryRunner<Row> createIntervalChunkingRunner(
      final IntervalChunkingQueryRunnerDecorator decorator,
      final QueryRunner<Row> runner,
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
  public Comparator<Row> createResultComparator(Query<Row> queryParam)
  {
    return ((GroupByQuery) queryParam).getRowOrdering(true);
  }

  @Override
  public BinaryOperator<Row> createMergeFn(Query<Row> queryParam)
  {
    return new GroupByBinaryFnV2((GroupByQuery) queryParam);
  }

  @Override
  public Sequence<Row> mergeResults(
      final QueryRunner<Row> baseRunner,
      final GroupByQuery query,
      final Map<String, Object> responseContext
  )
  {
    // Merge streams using ResultMergeQueryRunner, then apply postaggregators, then apply limit (which may
    // involve materialization)
    final ResultMergeQueryRunner<Row> mergingQueryRunner = new ResultMergeQueryRunner<>(
        baseRunner,
        this::createResultComparator,
        this::createMergeFn
    );

    // Fudge timestamp, maybe.
    final DateTime fudgeTimestamp = getUniversalTimestamp(query);
    ImmutableMap.Builder<String, Object> context = ImmutableMap.builder();
    context.put("finalize", false);
    context.put(GroupByQueryConfig.CTX_KEY_STRATEGY, GroupByStrategySelector.STRATEGY_V2);
    if (fudgeTimestamp != null) {
      context.put(CTX_KEY_FUDGE_TIMESTAMP, String.valueOf(fudgeTimestamp.getMillis()));
    }
    context.put(CTX_KEY_OUTERMOST, false);
    // the having spec shouldn't be passed down, so we need to convey the existing limit push down status
    context.put(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, query.isApplyLimitPushDown());

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

    return Sequences.map(
        mergingQueryRunner.run(
            QueryPlus.wrap(newQuery),
            responseContext
        ),
        new Function<Row, Row>()
        {
          @Override
          public Row apply(final Row row)
          {
            if (query.getContextBoolean(GroupByQueryConfig.CTX_KEY_EXECUTING_NESTED_QUERY, false)) {
              // When executing nested queries, we need to make sure that we are
              // extracting out the event from the row.  Post aggregators are not invoked since
              // they should only be used after merging all the nested query responses. Timestamp
              // if it needs to be fudged, it is ok to do here.
              return new MapBasedRow(
                  fudgeTimestamp != null ? fudgeTimestamp : row.getTimestamp(),
                  ((MapBasedRow) row).getEvent()
              );
            }
            // Apply postAggregators and fudgeTimestamp if present and if this is the outermost mergeResults.

            if (!query.getContextBoolean(CTX_KEY_OUTERMOST, true)) {
              return row;
            }

            if (query.getPostAggregatorSpecs().isEmpty() && fudgeTimestamp == null) {
              return row;
            }

            final Map<String, Object> newMap;

            if (query.getPostAggregatorSpecs().isEmpty()) {
              newMap = ((MapBasedRow) row).getEvent();
            } else {
              newMap = Maps.newLinkedHashMap(((MapBasedRow) row).getEvent());

              for (PostAggregator postAggregator : query.getPostAggregatorSpecs()) {
                newMap.put(postAggregator.getName(), postAggregator.compute(newMap));
              }
            }

            return new MapBasedRow(fudgeTimestamp != null ? fudgeTimestamp : row.getTimestamp(), newMap);
          }
        }
    );
  }

  @Override
  public Sequence<Row> applyPostProcessing(Sequence<Row> results, GroupByQuery query)
  {
    // Don't apply limit here for inner results, that will be pushed down to the BufferHashGrouper
    if (query.getContextBoolean(CTX_KEY_OUTERMOST, true)) {
      return query.postProcess(results);
    } else {
      return results;
    }
  }

  @Override
  public Sequence<Row> processSubqueryResult(
      GroupByQuery subquery,
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<Row> subqueryResult,
      boolean wasQueryPushedDown
  )
  {
    // This contains all closeable objects which are closed when the returned iterator iterates all the elements,
    // or an exceptions is thrown. The objects are closed in their reverse order.
    final List<Closeable> closeOnExit = new ArrayList<>();

    try {
      Supplier<Grouper> grouperSupplier = Suppliers.memoize(
          () -> GroupByRowProcessor.createGrouper(
              query,
              subqueryResult,
              GroupByQueryHelper.rowSignatureFor(wasQueryPushedDown ? query : subquery),
              configSupplier.get(),
              resource,
              spillMapper,
              processingConfig.getTmpDir(),
              processingConfig.intermediateComputeSizeBytes(),
              closeOnExit,
              wasQueryPushedDown,
              true
          )
      );

      return Sequences.withBaggage(
          mergeResults(new QueryRunner<Row>()
          {
            @Override
            public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
            {
              return GroupByRowProcessor.getRowsFromGrouper(
                  query,
                  null,
                  grouperSupplier
              );
            }
          }, query, null),
          () -> Lists.reverse(closeOnExit).forEach(closeable -> CloseQuietly.close(closeable))
      );
    }
    catch (Exception ex) {
      Lists.reverse(closeOnExit).forEach(closeable -> CloseQuietly.close(closeable));
      throw ex;
    }
  }

  @Override
  public Sequence<Row> processSubtotalsSpec(
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<Row> queryResult
  )
  {
    // How it works?
    // First we accumulate the result of top level query aka queryResult arg inside a GrouperOne object.
    // Next for each subtotalSpec
    //   If subtotalSpec is a prefix of top level dims then we iterate on rows in GrouperTwo object which are still
    //   sorted by subtotalSpec, stream merge them and return.
    //
    //   If subtotalSpec is not a prefix of top level dims then we create a Grouper#2 object filled with rows from
    //   Grouper#1 object with only dims from subtotalSpec. Then we iterate on rows in Grouper#2 object which are
    //   of course sorted by subtotalSpec, stream merge them and return.

    // This contains all closeable objects which are closed when the returned iterator iterates all the elements,
    // or an exceptions is thrown. The objects are closed in their reverse order.
    final List<Closeable> closeOnExit = new ArrayList<>();

    try {
      GroupByQuery queryWithoutSubtotalsSpec = query
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
          .withSubtotalsSpec(null)
          .withDimFilter(null);

      List<List<String>> subtotals = query.getSubtotalsSpec();

      Supplier<Grouper> grouperOneSupplier = Suppliers.memoize(
          () -> GroupByRowProcessor.createGrouper(
              queryWithoutSubtotalsSpec,
              queryResult,
              GroupByQueryHelper.rowSignatureFor(queryWithoutSubtotalsSpec),
              configSupplier.get(),
              resource,
              spillMapper,
              processingConfig.getTmpDir(),
              processingConfig.intermediateComputeSizeBytes(),
              closeOnExit,
              false,
              false
          )
      );


      List<Sequence<Row>> subtotalsResults = new ArrayList<>(subtotals.size());

      Map<String, DimensionSpec> queryDimensionSpecs = new HashMap(queryWithoutSubtotalsSpec.getDimensions().size());
      List<String> queryDimNames = new ArrayList(queryWithoutSubtotalsSpec.getDimensions().size());

      for (DimensionSpec dimSpec : queryWithoutSubtotalsSpec.getDimensions()) {
        queryDimensionSpecs.put(dimSpec.getOutputName(), dimSpec);
        queryDimNames.add(dimSpec.getOutputName());
      }

      // Only needed to make LimitSpec.filterColumns(..) call later in case base query has a non default LimitSpec.
      Set<String> aggsAndPostAggs = null;
      if (queryWithoutSubtotalsSpec.getLimitSpec() != null && !(queryWithoutSubtotalsSpec.getLimitSpec() instanceof NoopLimitSpec)) {
        aggsAndPostAggs = getAggregatorAndPostAggregatorNames(queryWithoutSubtotalsSpec);
      }

      for (List<String> subtotalSpec : subtotals) {

        // Create appropriate LimitSpec for subtotal query
        LimitSpec subtotalQueryLimitSpec = NoopLimitSpec.instance();
        if (queryWithoutSubtotalsSpec.getLimitSpec() != null && !(queryWithoutSubtotalsSpec.getLimitSpec() instanceof NoopLimitSpec)) {
          Set<String> columns = new HashSet(aggsAndPostAggs);
          columns.addAll(subtotalSpec);

          subtotalQueryLimitSpec = queryWithoutSubtotalsSpec.getLimitSpec().filterColumns(columns);
        }

        GroupByQuery subtotalQuery = queryWithoutSubtotalsSpec
            .withLimitSpec(subtotalQueryLimitSpec)
            .withDimensionSpecs(
                subtotalSpec.stream()
                            .map(queryDimensionSpecs::get)
                            .collect(Collectors.toList())
            );



        if (Utils.isPrefix(subtotalSpec, queryDimNames)) {
          subtotalsResults.add(
              applyPostProcessing(
                  mergeResults(
                      new QueryRunner<Row>()
                      {
                        @Override
                        public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
                        {
                          return GroupByRowProcessor.getRowsFromGrouper(
                              queryWithoutSubtotalsSpec,
                              subtotalSpec,
                              grouperOneSupplier
                          );
                        }
                      },
                      subtotalQuery,
                      null
                  ),
                  subtotalQuery
              )
          );
        } else {
          subtotalsResults.add(
              applyPostProcessing(
                  mergeResults(
                      new QueryRunner<Row>()
                      {
                        @Override
                        public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
                        {
                          List<Closeable> closeables = new ArrayList<>();

                          Supplier<Grouper> grouperTwoSupplier = Suppliers.memoize(() -> GroupByRowProcessor.createGrouper(
                              subtotalQuery,
                              GroupByRowProcessor.getRowsFromGrouper(
                                  queryWithoutSubtotalsSpec,
                                  subtotalSpec,
                                  grouperOneSupplier
                              ),
                              GroupByQueryHelper.rowSignatureFor(subtotalQuery),
                              configSupplier.get(),
                              resource,
                              spillMapper,
                              processingConfig.getTmpDir(),
                              processingConfig.intermediateComputeSizeBytes(),
                              closeables,
                              false,
                              false
                          ));

                          Sequence<Row> result = GroupByRowProcessor.getRowsFromGrouper(
                              subtotalQuery,
                              subtotalSpec,
                              grouperTwoSupplier
                          );

                          // Close all the resources associated with grouperTwo as soon as all results for this
                          // subtotal spec are read.
                          return Sequences.withBaggage(
                              result,
                              () -> Lists.reverse(closeables)
                                         .forEach(closeable -> CloseQuietly.close(closeable))
                          );
                        }
                      },
                      subtotalQuery,
                      null
                  ),
                  subtotalQuery
              )
          );
        }
      }

      // Close all resources associated with grouperOne when all results have been read.
      return Sequences.withBaggage(
          Sequences.concat(subtotalsResults),
          () -> Lists.reverse(closeOnExit).forEach(closeable -> CloseQuietly.close(closeable))
      );
    }
    catch (Exception ex) {
      Lists.reverse(closeOnExit).forEach(closeable -> CloseQuietly.close(closeable));
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

  private int numMergeBuffersForSubtotalsSpec(GroupByQuery query)
  {
    List<List<String>> subtotalSpecs = query.getSubtotalsSpec();
    if (subtotalSpecs == null) {
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
  public QueryRunner<Row> mergeRunners(ListeningExecutorService exec, Iterable<QueryRunner<Row>> queryRunners)
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
  public Sequence<Row> process(GroupByQuery query, StorageAdapter storageAdapter)
  {
    return GroupByQueryEngineV2.process(query, storageAdapter, bufferPool, configSupplier.get().withOverrides(query));
  }

  @Override
  public boolean supportsNestedQueryPushDown()
  {
    return true;
  }
}
