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

package org.apache.druid.queryng.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.ResultLevelCachingQueryRunner;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operator.general.ResultLevelCacheOperator;
import org.apache.druid.queryng.operator.general.RetryOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.general.ResponseContextInitializationOperator;
import org.apache.druid.queryng.operators.general.ThrottleOperator;
import org.apache.druid.queryng.operators.general.ThrottleOperator.Throttle;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryScheduler.LaneToken;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Operator-based query planner for server-level functionality. Includes
 * planning for the test (local) server.
 */
public class ServerExecutionPlanner
{
  private static class ThrottleImpl implements Throttle
  {
    private final QueryScheduler scheduler;
    private final Query<?> query;
    private LaneToken token;

    public ThrottleImpl(QueryScheduler scheduler, Query<?> query)
    {
      this.scheduler = scheduler;
      this.query = query;
    }

    @Override
    public void accept()
    {
      token = scheduler.accept(query);
    }

    @Override
    public void release()
    {
      token.release();
    }
  }

  /**
   * Plan execution on a local "test cluster".
   *
   * @see {@link org.apache.druid.server.TestClusterQuerySegmentWalker}
   */
  public static <T> Sequence<T> testRun(
      final QueryPlus<T> queryPlus,
      final QueryRunner<T> input,
      final ResponseContext responseContext,
      final Iterable<SegmentDescriptor> specs,
      @Nullable final QueryScheduler scheduler
  )
  {
    // Rewrites the QuerySegmentSpec to mention
    // the specific segments. This mimics what CachingClusteredClient on the Broker
    // does, and is required for certain queries (like Scan) to function properly.
    // SegmentServerSelector does not currently mimic CachingClusteredClient, it uses
    // the LocalQuerySegmentWalker constructor instead since this walker does not
    // mimic remote DruidServer objects to actually serve the queries.
    QueryPlus<T> rewrittenQuery = queryPlus.withQuery(
        Queries.withSpecificSegments(
            queryPlus.getQuery(),
            ImmutableList.copyOf(specs)));
    Operator<T> op = Operators.toOperator(input, rewrittenQuery);
    FragmentContext fragmentContext = queryPlus.fragment();
    if (scheduler != null) {
      Set<SegmentServerSelector> segments = new HashSet<>();
      specs.forEach(spec -> segments.add(new SegmentServerSelector(spec)));
      op = new ThrottleOperator<T>(
            fragmentContext,
            op,
            new ThrottleImpl(
                scheduler,
                scheduler.prioritizeAndLaneQuery(
                    queryPlus,
                    segments
                )
            )
      );
    }
    op = new ResponseContextInitializationOperator<T>(
        fragmentContext,
        op,
        queryPlus.getQuery());
    return Operators.toSequence(op);
  }

  public static <T> Sequence<T> retryRun(
      final QueryPlus<T> queryPlus,
      final QueryRunner<T> baseRunner,
      final BiFunction<Query<T>, List<SegmentDescriptor>, QueryRunner<T>> retryRunnerCreateFn,
      final RetryQueryRunnerConfig config,
      final ObjectMapper jsonMapper
  )
  {
    Operator<T> op = new RetryOperator<T>(
        queryPlus.fragment(),
        queryPlus,
        Operators.toOperator(baseRunner, queryPlus),
        queryPlus.getQuery().getResultOrdering(),
        retryRunnerCreateFn,
        (id, rc) -> RetryOperator.getMissingSegments(id, rc, jsonMapper),
        QueryContexts.getNumRetriesOnMissingSegments(
            queryPlus.getQuery(),
            config.getNumTries()
        ),
        QueryContexts.allowReturnPartialResults(
            queryPlus.getQuery(),
            config.isReturnPartialResults()
        ),
        () -> { }
    );
    return Operators.toSequence(op);
  }

  public static <T> Sequence<T> throttle(
      final QueryPlus<T> queryPlus,
      final QueryRunner<T> baseRunner,
      final Throttle throttle
  )
  {
    Operator<T> op = new ThrottleOperator<T>(
        queryPlus.fragment(),
        Operators.toOperator(baseRunner, queryPlus),
        throttle
    );
    return Operators.toSequence(op);
  }

  public static <T> Sequence<T> planCache(
      final QueryRunner<T> baseRunner,
      final CacheStrategy<T, Object, Query<T>> strategy,
      final ObjectMapper objectMapper,
      final Cache cache,
      final CacheConfig cacheConfig,
      final QueryPlus<T> queryPlus
  )
  {
    final Query<T> query = queryPlus.getQuery();
    final boolean populateResultCache = CacheUtil.isPopulateResultCache(
        query,
        strategy,
        cacheConfig,
        CacheUtil.ServerType.BROKER
    );
    final boolean useResultCache = CacheUtil.isUseResultCache(query, strategy, cacheConfig, CacheUtil.ServerType.BROKER);
    if (!useResultCache && !populateResultCache) {
      return baseRunner.run(queryPlus, queryPlus.fragment().responseContext());
    }
    final String cacheKeyStr = StringUtils.fromUtf8(strategy.computeResultLevelCacheKey(query));
    final byte[] cachedResultSet;
    final String existingResultSetId;
    if (useResultCache) {
      // TODO: This form is not very efficient: materializes the entire cached
      // result set at plan time. Better to a) verify that the result set exists,
      // b) lock it in place for the duration of the query so that c) we can stream it
      // at run time if needed. That is, treat the cache file more like we treat a
      // segment.
      cachedResultSet = cache.get(CacheUtil.computeResultLevelCacheKey(cacheKeyStr));
      if (cachedResultSet == null) {
        if (!populateResultCache) {
          return baseRunner.run(queryPlus, queryPlus.fragment().responseContext());
        }
        existingResultSetId = "";
      } else {
        existingResultSetId = ResultLevelCachingQueryRunner.extractEtagFromResults(
            query,
            cachedResultSet
        );
      }
    } else {
      cachedResultSet = null;
      existingResultSetId = "";
    }
    QueryPlus<T> childQuery = queryPlus.withQuery(queryPlus
        .getQuery()
        .withOverriddenContext(
            ImmutableMap.of(QueryResource.HEADER_IF_NONE_MATCH, existingResultSetId)));
    Operator<T> input = Operators.toOperator(baseRunner, childQuery);
    Operator<T> op = new ResultLevelCacheOperator<T>(
        queryPlus.fragment(),
        input,
        strategy,
        cache,
        cacheConfig,
        objectMapper,
        cacheKeyStr,
        existingResultSetId,
        cachedResultSet,
        populateResultCache
    );
    return Operators.toSequence(op);
  }
}
