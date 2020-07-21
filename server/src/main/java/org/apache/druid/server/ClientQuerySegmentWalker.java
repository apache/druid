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

package org.apache.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.PostProcessingOperator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.ResultLevelCachingQueryRunner;
import org.apache.druid.query.RetryQueryRunner;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.initialization.ServerConfig;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Query handler for Broker processes (see CliBroker).
 *
 * This class is responsible for:
 *
 * 1) Running queries on the cluster using its 'clusterClient'
 * 2) Running queries locally (when all datasources are global) using its 'localClient'
 * 3) Inlining subqueries if necessary, in service of the above two goals
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final ServiceEmitter emitter;
  private final QuerySegmentWalker clusterClient;
  private final QuerySegmentWalker localClient;
  private final QueryToolChestWarehouse warehouse;
  private final JoinableFactory joinableFactory;
  private final RetryQueryRunnerConfig retryConfig;
  private final ObjectMapper objectMapper;
  private final ServerConfig serverConfig;
  private final Cache cache;
  private final CacheConfig cacheConfig;

  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      QuerySegmentWalker clusterClient,
      QuerySegmentWalker localClient,
      QueryToolChestWarehouse warehouse,
      JoinableFactory joinableFactory,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper,
      ServerConfig serverConfig,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.emitter = emitter;
    this.clusterClient = clusterClient;
    this.localClient = localClient;
    this.warehouse = warehouse;
    this.joinableFactory = joinableFactory;
    this.retryConfig = retryConfig;
    this.objectMapper = objectMapper;
    this.serverConfig = serverConfig;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
  }

  @Inject
  ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient clusterClient,
      LocalQuerySegmentWalker localClient,
      QueryToolChestWarehouse warehouse,
      JoinableFactory joinableFactory,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper,
      ServerConfig serverConfig,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this(
        emitter,
        (QuerySegmentWalker) clusterClient,
        (QuerySegmentWalker) localClient,
        warehouse,
        joinableFactory,
        retryConfig,
        objectMapper,
        serverConfig,
        cache,
        cacheConfig
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    // transform TableDataSource to GlobalTableDataSource when eligible
    // before further transformation to potentially inline
    final DataSource freeTradeDataSource = globalizeIfPossible(query.getDataSource());
    // do an inlining dry run to see if any inlining is necessary, without actually running the queries.
    final int maxSubqueryRows = QueryContexts.getMaxSubqueryRows(query, serverConfig.getMaxSubqueryRows());
    final DataSource inlineDryRun = inlineIfNecessary(
        freeTradeDataSource,
        toolChest,
        new AtomicInteger(),
        maxSubqueryRows,
        true
    );

    if (!canRunQueryUsingClusterWalker(query.withDataSource(inlineDryRun))
        && !canRunQueryUsingLocalWalker(query.withDataSource(inlineDryRun))) {
      // Dry run didn't go well.
      throw new ISE("Cannot handle subquery structure for dataSource: %s", query.getDataSource());
    }

    // Now that we know the structure is workable, actually do the inlining (if necessary).
    final Query<T> newQuery = query.withDataSource(
        inlineIfNecessary(
            freeTradeDataSource,
            toolChest,
            new AtomicInteger(),
            maxSubqueryRows,
            false
        )
    );

    if (canRunQueryUsingLocalWalker(newQuery)) {
      // No need to decorate since LocalQuerySegmentWalker does its own.
      return new QuerySwappingQueryRunner<>(
          localClient.getQueryRunnerForIntervals(newQuery, intervals),
          query,
          newQuery
      );
    } else if (canRunQueryUsingClusterWalker(newQuery)) {
      // Note: clusterClient.getQueryRunnerForIntervals() can return an empty sequence if there is no segment
      // to query, but this is not correct when there's a right or full outer join going on.
      // See https://github.com/apache/druid/issues/9229 for details.
      return new QuerySwappingQueryRunner<>(
          decorateClusterRunner(newQuery, clusterClient.getQueryRunnerForIntervals(newQuery, intervals)),
          query,
          newQuery
      );
    } else {
      // We don't expect to ever get here, because the logic earlier in this method should have rejected any query
      // that can't be run with either the local or cluster walkers. If this message ever shows up it is a bug.
      throw new ISE("Inlined query could not be run");
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    // Inlining isn't done for segments-based queries, but we still globalify the table datasources if possible
    final Query<T> freeTradeQuery = query.withDataSource(globalizeIfPossible(query.getDataSource()));

    if (canRunQueryUsingClusterWalker(query)) {
      return new QuerySwappingQueryRunner<>(
          decorateClusterRunner(freeTradeQuery, clusterClient.getQueryRunnerForSegments(freeTradeQuery, specs)),
          query,
          freeTradeQuery
      );
    } else {
      // We don't expect end-users to see this message, since it only happens when specific segments are requested;
      // this is not typical end-user behavior.
      throw new ISE(
          "Cannot run query on specific segments (must be table-based; outer query, if present, must be "
          + "handleable by the query toolchest natively)");
    }
  }

  /**
   * Checks if a query can be handled wholly by {@link #localClient}. Assumes that it is a
   * {@link LocalQuerySegmentWalker} or something that behaves similarly.
   */
  private <T> boolean canRunQueryUsingLocalWalker(Query<T> query)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    // 1) Must be based on a concrete datasource that is not a table.
    // 2) Must be based on globally available data (so we have a copy here on the Broker).
    // 3) If there is an outer query, it must be handleable by the query toolchest (the local walker does not handle
    //    subqueries on its own).
    return analysis.isConcreteBased() && !analysis.isConcreteTableBased() && analysis.isGlobal()
           && (!analysis.isQuery()
               || toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery()));
  }

  /**
   * Checks if a query can be handled wholly by {@link #clusterClient}. Assumes that it is a
   * {@link CachingClusteredClient} or something that behaves similarly.
   */
  private <T> boolean canRunQueryUsingClusterWalker(Query<T> query)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    // 1) Must be based on a concrete table (the only shape the Druid cluster can handle).
    // 2) If there is an outer query, it must be handleable by the query toolchest (the cluster walker does not handle
    //    subqueries on its own).
    return analysis.isConcreteTableBased()
           && (!analysis.isQuery()
               || toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery()));
  }


  private DataSource globalizeIfPossible(
      final DataSource dataSource
  )
  {
    if (dataSource instanceof TableDataSource) {
      GlobalTableDataSource maybeGlobal = new GlobalTableDataSource(((TableDataSource) dataSource).getName());
      if (joinableFactory.isDirectlyJoinable(maybeGlobal)) {
        return maybeGlobal;
      }
      return dataSource;
    } else {
      List<DataSource> currentChildren = dataSource.getChildren();
      List<DataSource> newChildren = new ArrayList<>(currentChildren.size());
      for (DataSource child : currentChildren) {
        newChildren.add(globalizeIfPossible(child));
      }
      return dataSource.withChildren(newChildren);
    }
  }

  /**
   * Replace QueryDataSources with InlineDataSources when necessary and possible. "Necessary" is defined as:
   *
   * 1) For outermost subqueries: inlining is necessary if the toolchest cannot handle it.
   * 2) For all other subqueries (e.g. those nested under a join): inlining is always necessary.
   *
   * @param dataSource           datasource to process.
   * @param toolChestIfOutermost if provided, and if the provided datasource is a {@link QueryDataSource}, this method
   *                             will consider whether the toolchest can handle a subquery on the datasource using
   *                             {@link QueryToolChest#canPerformSubquery}. If the toolchest can handle it, then it will
   *                             not be inlined. See {@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest}
   *                             for an example of a toolchest that can handle subqueries.
   * @param dryRun               if true, does not actually execute any subqueries, but will inline empty result sets.
   */
  @SuppressWarnings({"rawtypes", "unchecked"}) // Subquery, toolchest, runner handling all use raw types
  private DataSource inlineIfNecessary(
      final DataSource dataSource,
      @Nullable final QueryToolChest toolChestIfOutermost,
      final AtomicInteger subqueryRowLimitAccumulator,
      final int maxSubqueryRows,
      final boolean dryRun
  )
  {
    if (dataSource instanceof QueryDataSource) {
      // This datasource is a subquery.
      final Query subQuery = ((QueryDataSource) dataSource).getQuery();
      final QueryToolChest toolChest = warehouse.getToolChest(subQuery);

      if (toolChestIfOutermost != null && toolChestIfOutermost.canPerformSubquery(subQuery)) {
        // Strip outer queries that are handleable by the toolchest, and inline subqueries that may be underneath
        // them (e.g. subqueries nested under a join).
        final Stack<DataSource> stack = new Stack<>();

        DataSource current = dataSource;
        while (current instanceof QueryDataSource) {
          stack.push(current);
          current = Iterables.getOnlyElement(current.getChildren());
        }

        assert !(current instanceof QueryDataSource); // lgtm [java/contradictory-type-checks]
        current = inlineIfNecessary(current, null, subqueryRowLimitAccumulator, maxSubqueryRows, dryRun);

        while (!stack.isEmpty()) {
          current = stack.pop().withChildren(Collections.singletonList(current));
        }

        assert current instanceof QueryDataSource;

        if (toolChest.canPerformSubquery(((QueryDataSource) current).getQuery())) {
          return current;
        } else {
          // Something happened during inlining that means the toolchest is no longer able to handle this subquery.
          // We need to consider inlining it.
          return inlineIfNecessary(current, toolChestIfOutermost, subqueryRowLimitAccumulator, maxSubqueryRows, dryRun);
        }
      } else if (canRunQueryUsingLocalWalker(subQuery) || canRunQueryUsingClusterWalker(subQuery)) {
        // Subquery needs to be inlined. Assign it a subquery id and run it.
        final Query subQueryWithId = subQuery.withDefaultSubQueryId();

        final Sequence<?> queryResults;

        if (dryRun) {
          queryResults = Sequences.empty();
        } else {
          final QueryRunner subqueryRunner = subQueryWithId.getRunner(this);
          queryResults = subqueryRunner.run(
              QueryPlus.wrap(subQueryWithId),
              DirectDruidClient.makeResponseContextForQuery()
          );
        }

        return toInlineDataSource(
            subQueryWithId,
            queryResults,
            warehouse.getToolChest(subQueryWithId),
            subqueryRowLimitAccumulator,
            maxSubqueryRows
        );
      } else {
        // Cannot inline subquery. Attempt to inline one level deeper, and then try again.
        return inlineIfNecessary(
            dataSource.withChildren(
                Collections.singletonList(
                    inlineIfNecessary(
                        Iterables.getOnlyElement(dataSource.getChildren()),
                        null,
                        subqueryRowLimitAccumulator,
                        maxSubqueryRows,
                        dryRun
                    )
                )
            ),
            toolChestIfOutermost,
            subqueryRowLimitAccumulator,
            maxSubqueryRows,
            dryRun
        );
      }
    } else {
      // Not a query datasource. Walk children and see if there's anything to inline.
      return dataSource.withChildren(
          dataSource.getChildren()
                    .stream()
                    .map(child -> inlineIfNecessary(child, null, subqueryRowLimitAccumulator, maxSubqueryRows, dryRun))
                    .collect(Collectors.toList())
      );
    }
  }

  /**
   * Decorate query runners created by {@link #clusterClient}, adding result caching, result merging, metric
   * emission, etc. Not to be used on runners from {@link #localClient}, since we expect it to do this kind
   * of decoration to itself.
   *
   * @param query             the query
   * @param baseClusterRunner runner from {@link #clusterClient}
   */
  private <T> QueryRunner<T> decorateClusterRunner(Query<T> query, QueryRunner<T> baseClusterRunner)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    return new FluentQueryRunnerBuilder<>(toolChest)
        .create(
            new SetAndVerifyContextQueryRunner<>(
                serverConfig,
                new RetryQueryRunner<>(
                    baseClusterRunner,
                    clusterClient::getQueryRunnerForSegments,
                    retryConfig,
                    objectMapper
                )
            )
        )
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration()
        .emitCPUTimeMetric(emitter)
        .postProcess(
            objectMapper.convertValue(
                query.<String>getContextValue("postProcessing"),
                new TypeReference<PostProcessingOperator<T>>() {}
            )
        )
        .map(
            runner ->
                new ResultLevelCachingQueryRunner<>(
                    runner,
                    toolChest,
                    query,
                    objectMapper,
                    cache,
                    cacheConfig
                )
        );
  }

  /**
   * Convert the results of a particular query into a materialized (List-based) InlineDataSource.
   *
   * @param query            the query
   * @param results          query results
   * @param toolChest        toolchest for the query
   * @param limitAccumulator an accumulator for tracking the number of accumulated rows in all subqueries for a
   *                         particular master query
   * @param limit            user-configured limit. If negative, will be treated as {@link Integer#MAX_VALUE}.
   *                         If zero, this method will throw an error immediately.
   *
   * @throws ResourceLimitExceededException if the limit is exceeded
   */
  private static <T, QueryType extends Query<T>> InlineDataSource toInlineDataSource(
      final QueryType query,
      final Sequence<T> results,
      final QueryToolChest<T, QueryType> toolChest,
      final AtomicInteger limitAccumulator,
      final int limit
  )
  {
    final int limitToUse = limit < 0 ? Integer.MAX_VALUE : limit;

    if (limitAccumulator.get() >= limitToUse) {
      throw new ResourceLimitExceededException("Cannot issue subquery, maximum[%d] reached", limitToUse);
    }

    final RowSignature signature = toolChest.resultArraySignature(query);

    final List<Object[]> resultList = new ArrayList<>();

    toolChest.resultsAsArrays(query, results).accumulate(
        resultList,
        (acc, in) -> {
          if (limitAccumulator.getAndIncrement() >= limitToUse) {
            throw new ResourceLimitExceededException(
                "Subquery generated results beyond maximum[%d]",
                limitToUse
            );
          }
          acc.add(in);
          return acc;
        }
    );

    return InlineDataSource.fromIterable(resultList, signature);
  }

  /**
   * A {@link QueryRunner} which validates that a *specific* query is passed in, and then swaps it with another one.
   * Useful since the inlining we do relies on passing the modified query to the underlying {@link QuerySegmentWalker},
   * and callers of {@link #getQueryRunnerForIntervals} aren't able to do this themselves.
   */
  private static class QuerySwappingQueryRunner<T> implements QueryRunner<T>
  {
    private final QueryRunner<T> baseRunner;
    private final Query<T> query;
    private final Query<T> newQuery;

    public QuerySwappingQueryRunner(QueryRunner<T> baseRunner, Query<T> query, Query<T> newQuery)
    {
      this.baseRunner = baseRunner;
      this.query = query;
      this.newQuery = newQuery;
    }

    @Override
    public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
    {
      //noinspection ObjectEquality
      if (queryPlus.getQuery() != query) {
        throw new ISE("Unexpected query received");
      }

      return baseRunner.run(queryPlus.withQuery(newQuery), responseContext);
    }
  }
}
