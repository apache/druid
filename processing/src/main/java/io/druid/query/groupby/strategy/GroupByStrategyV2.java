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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.druid.collections.BlockingPool;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Merging;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.DataSource;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.InsufficientResourcesException;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryWatcher;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.epinephelinae.GroupByBinaryFnV2;
import io.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2;
import io.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import io.druid.query.groupby.epinephelinae.GroupByRowProcessor;
import io.druid.query.groupby.resource.GroupByQueryResource;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

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
      return new DateTime(Long.parseLong(timestampStringFromContext));
    } else if (Granularities.ALL.equals(gran)) {
      final long timeStart = query.getIntervals().get(0).getStartMillis();
      return gran.getIterable(new Interval(timeStart, timeStart + 1)).iterator().next().getStart();
    } else {
      return null;
    }
  }

  @Override
  public GroupByQueryResource prepareResource(GroupByQuery query, boolean willMergeRunners)
  {
    if (!willMergeRunners) {
      final int requiredMergeBufferNum = countRequiredMergeBufferNum(query, 1);

      if (requiredMergeBufferNum > mergeBufferPool.maxSize()) {
        throw new ResourceLimitExceededException(
            "Query needs " + requiredMergeBufferNum + " merge buffers, but only "
            + mergeBufferPool.maxSize() + " merge buffers are configured"
        );
      } else if (requiredMergeBufferNum == 0) {
        return new GroupByQueryResource();
      } else {
        final ResourceHolder<List<ByteBuffer>> mergeBufferHolders;
        if (QueryContexts.hasTimeout(query)) {
          mergeBufferHolders = mergeBufferPool.takeBatch(requiredMergeBufferNum, QueryContexts.getTimeout(query));
        } else {
          mergeBufferHolders = mergeBufferPool.takeBatch(requiredMergeBufferNum);
        }
        if (mergeBufferHolders == null) {
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
    // See also https://github.com/druid-io/druid/pull/4004
    return runner;
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
    final ResultMergeQueryRunner<Row> mergingQueryRunner = new ResultMergeQueryRunner<Row>(baseRunner)
    {
      @Override
      protected Ordering<Row> makeOrdering(Query<Row> queryParam)
      {
        return ((GroupByQuery) queryParam).getRowOrdering(true);
      }

      @Override
      protected BinaryFn<Row, Row, Row> createMergeFn(Query<Row> queryParam)
      {
        return new GroupByBinaryFnV2((GroupByQuery) queryParam);
      }
    };

    // Fudge timestamp, maybe.
    final DateTime fudgeTimestamp = getUniversalTimestamp(query);

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
        query.getContext()
    ).withOverriddenContext(
        ImmutableMap.<String, Object>of(
            "finalize", false,
            GroupByQueryConfig.CTX_KEY_STRATEGY, GroupByStrategySelector.STRATEGY_V2,
            CTX_KEY_FUDGE_TIMESTAMP, fudgeTimestamp == null ? "" : String.valueOf(fudgeTimestamp.getMillis()),
            CTX_KEY_OUTERMOST, false
        )
    );

    Sequence<Row> rowSequence = Sequences.map(
        mergingQueryRunner.run(
            QueryPlus.wrap(newQuery),
            responseContext
        ),
        new Function<Row, Row>()
        {
          @Override
          public Row apply(final Row row)
          {
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

    // Don't apply limit here for inner results, that will be pushed down to the BufferGrouper
    if (query.getContextBoolean(CTX_KEY_OUTERMOST, true)) {
      return query.postProcess(rowSequence);
    } else {
      return rowSequence;
    }
  }

  @Override
  public Sequence<Row> processSubqueryResult(
      GroupByQuery subquery,
      GroupByQuery query,
      GroupByQueryResource resource,
      Sequence<Row> subqueryResult
  )
  {
    final Sequence<Row> results = GroupByRowProcessor.process(
        query,
        subqueryResult,
        GroupByQueryHelper.rowSignatureFor(subquery),
        configSupplier.get(),
        resource,
        spillMapper,
        processingConfig.getTmpDir()
    );
    return mergeResults(new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
      {
        return results;
      }
    }, query, null);
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      ListeningExecutorService exec,
      Iterable<QueryRunner<Row>> queryRunners
  )
  {
    return new GroupByMergingQueryRunnerV2(
        configSupplier.get(),
        exec,
        queryWatcher,
        queryRunners,
        processingConfig.getNumThreads(),
        mergeBufferPool,
        spillMapper,
        processingConfig.getTmpDir()
    );
  }

  @Override
  public Sequence<Row> process(
      GroupByQuery query,
      StorageAdapter storageAdapter
  )
  {
    return GroupByQueryEngineV2.process(query, storageAdapter, bufferPool, configSupplier.get());
  }
}
