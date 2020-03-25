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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.server.initialization.ServerConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * QueryScheduler (potentially) assigns any {@link Query} that is to be executed to a 'query lane' using the
 * {@link QueryLaningStrategy} that is defined in {@link QuerySchedulerConfig}.
 *
 * As a {@link QueryWatcher}, it also provides cancellation facilities to brokers, historicals, and realtime tasks.
 *
 * This class is shared by all requests on the HTTP theadpool and must be thread safe.
 */
public class QueryScheduler implements QueryWatcher
{
  public static final int UNAVAILABLE = -1;
  public static final String TOTAL = "total";
  private final int totalCapacity;
  private final QueryPrioritizationStrategy prioritizationStrategy;
  private final QueryLaningStrategy laningStrategy;
  private final BulkheadRegistry laneRegistry;
  /**
   * mapping of query id to set of futures associated with the query
   */
  private final SetMultimap<String, ListenableFuture<?>> queryFutures;
  /**
   * mapping of query id to set of datasource names that are being queried, used for authorization
   */
  private final SetMultimap<String, String> queryDatasources;

  public QueryScheduler(
      int totalNumThreads,
      QueryPrioritizationStrategy prioritizationStrategy,
      QueryLaningStrategy laningStrategy,
      ServerConfig serverConfig
  )
  {
    this.prioritizationStrategy = prioritizationStrategy;
    this.laningStrategy = laningStrategy;
    this.queryFutures = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    this.queryDatasources = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    // if totalNumThreads is above 0 and less than druid.server.http.numThreads, enforce total limit
    final boolean limitTotal;
    if (totalNumThreads > 0 && totalNumThreads < serverConfig.getNumThreads()) {
      limitTotal = true;
      this.totalCapacity = totalNumThreads;
    } else {
      limitTotal = false;
      this.totalCapacity = serverConfig.getNumThreads();
    }
    this.laneRegistry = BulkheadRegistry.of(getLaneConfigs(limitTotal));
  }

  @Override
  public void registerQueryFuture(Query<?> query, ListenableFuture<?> future)
  {
    final String id = query.getId();
    final Set<String> datasources = query.getDataSource().getTableNames();
    queryFutures.put(id, future);
    queryDatasources.putAll(id, datasources);
    future.addListener(
        () -> {
          queryFutures.remove(id, future);
          for (String datasource : datasources) {
            queryDatasources.remove(id, datasource);
          }
        },
        Execs.directExecutor()
    );
  }

  /**
   * Assign a query a priority and lane (if not set)
   */
  public <T> Query<T> prioritizeAndLaneQuery(QueryPlus<T> queryPlus, Set<SegmentServerSelector> segments)
  {
    Query<T> query = queryPlus.getQuery();
    Optional<Integer> priority = prioritizationStrategy.computePriority(queryPlus, segments);
    query = priority.map(query::withPriority).orElse(query);
    Optional<String> lane = laningStrategy.computeLane(queryPlus.withQuery(query), segments);
    return lane.map(query::withLane).orElse(query);
  }

  /**
   * Run a query with the scheduler, attempting to acquire a semaphore from the total and lane specific query capacities
   *
   * Note that {@link #cancelQuery} should not interrupt the thread that calls run, in all current usages it only
   * cancels any {@link ListenableFuture} created downstream. If this ever commonly changes, we should add
   * synchronization between {@link #cancelQuery} and the acquisition of the {@link Bulkhead} to continue to ensure that
   * anything acquired is also released.
   *
   * In the meantime, if a {@link ListenableFuture} is registered for the query that calls this method, it MUST handle
   * this synchronization itself to ensure that no {@link Bulkhead} is acquired without releasing it.
   */
  public <T> Sequence<T> run(Query<?> query, Sequence<T> resultSequence)
  {
    List<Bulkhead> bulkheads = acquireLanes(query);
    return resultSequence.withBaggage(() -> finishLanes(bulkheads));
  }

  /**
   * Returns a {@link QueryRunner} that will call {@link QueryScheduler#run} when {@link QueryRunner#run} is called.
   */
  public <T> QueryRunner<T> wrapQueryRunner(QueryRunner<T> baseRunner)
  {
    return (queryPlus, responseContext) ->
        QueryScheduler.this.run(
            queryPlus.getQuery(), new LazySequence<>(() -> baseRunner.run(queryPlus, responseContext))
        );
  }

  /**
   * Forcibly cancel all futures that have been registered to a specific query id
   */
  public boolean cancelQuery(String id)
  {
    // if multiple independent queries from the same or different users share a query id, all will be cancelled due
    // to the collision
    queryDatasources.removeAll(id);
    Set<ListenableFuture<?>> futures = queryFutures.removeAll(id);
    boolean success = true;
    for (ListenableFuture<?> future : futures) {
      success = success && future.cancel(true);
    }
    return success;
  }

  /**
   * Get a {@link Set} of datasource names for a {@link Query} id, used by {@link QueryResource#cancelQuery} to
   * authorize that a user may call {@link #cancelQuery} for the given id and datasources
   */
  public Set<String> getQueryDatasources(final String queryId)
  {
    return queryDatasources.get(queryId);
  }

  /**
   * Get the maximum number of concurrent queries that {@link #run} can support
   */
  @VisibleForTesting
  int getTotalAvailableCapacity()
  {
    return laneRegistry.getConfiguration(TOTAL)
                       .map(config -> laneRegistry.bulkhead(TOTAL, config).getMetrics().getAvailableConcurrentCalls())
                       .orElse(UNAVAILABLE);
  }

  /**
   * Get the maximum number of concurrent queries that {@link #run} can support for a given lane
   */
  @VisibleForTesting
  int getLaneAvailableCapacity(String lane)
  {
    return laneRegistry.getConfiguration(lane)
                       .map(config -> laneRegistry.bulkhead(lane, config).getMetrics().getAvailableConcurrentCalls())
                       .orElse(UNAVAILABLE);
  }

  /**
   * Acquire a semaphore for both the 'total' and a lane, if any is associated with a query
   */
  @VisibleForTesting
  List<Bulkhead> acquireLanes(Query<?> query)
  {
    final String lane = QueryContexts.getLane(query);
    final Optional<BulkheadConfig> laneConfig = lane == null ? Optional.empty() : laneRegistry.getConfiguration(lane);
    final Optional<BulkheadConfig> totalConfig = laneRegistry.getConfiguration(TOTAL);
    List<Bulkhead> hallPasses = new ArrayList<>(2);
    try {
      // if we have a lane, get it first
      laneConfig.ifPresent(config -> {
        Bulkhead laneLimiter = laneRegistry.bulkhead(lane, config);
        if (!laneLimiter.tryAcquirePermission()) {
          throw new QueryCapacityExceededException(lane, config.getMaxConcurrentCalls());
        }
        hallPasses.add(laneLimiter);
      });

      // everyone needs to take one from the total lane; to ensure we don't acquire a lane and never release it, we want
      // to check for total capacity exceeded and release the lane (if present) before throwing capacity exceeded
      // note that this isn't strictly fair: the bulkhead doesn't use a fair semaphore, the first to acquire the lane
      // might lose to one that came after it when acquiring the total, or an unlaned query might lose to a laned query
      totalConfig.ifPresent(config -> {
        Bulkhead totalLimiter = laneRegistry.bulkhead(TOTAL, config);
        if (!totalLimiter.tryAcquirePermission()) {
          throw new QueryCapacityExceededException(config.getMaxConcurrentCalls());
        }
        hallPasses.add(totalLimiter);
      });
      return hallPasses;
    }
    catch (Exception ex) {
      releaseLanes(hallPasses);
      throw ex;
    }
  }

  /**
   * Release all {@link Bulkhead} semaphores in the list
   */
  @VisibleForTesting
  void releaseLanes(List<Bulkhead> bulkheads)
  {
    bulkheads.forEach(Bulkhead::releasePermission);
  }

  @VisibleForTesting
  void finishLanes(List<Bulkhead> bulkheads)
  {
    bulkheads.forEach(Bulkhead::onComplete);
  }

  /**
   * With a total thread count and {@link QueryLaningStrategy#getLaneLimits}, create a map of lane name to
   * {@link BulkheadConfig} to be used to create the {@link #laneRegistry}. This accepts the configured value of
   * numThreads rather than using {@link #totalCapacity} so that we only have a total {@link Bulkhead} if
   * {@link QuerySchedulerConfig#getNumThreads()} is set
   */
  private Map<String, BulkheadConfig> getLaneConfigs(boolean hasTotalLimit)
  {
    Map<String, BulkheadConfig> configs = new HashMap<>();
    if (hasTotalLimit) {
      configs.put(
          TOTAL,
          BulkheadConfig.custom().maxConcurrentCalls(totalCapacity).maxWaitDuration(Duration.ZERO).build()
      );
    }
    for (Object2IntMap.Entry<String> entry : laningStrategy.getLaneLimits(totalCapacity).object2IntEntrySet()) {
      configs.put(
          entry.getKey(),
          BulkheadConfig.custom().maxConcurrentCalls(entry.getIntValue()).maxWaitDuration(Duration.ZERO).build()
      );
    }
    return configs;
  }
}
