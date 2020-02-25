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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServer;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryWatcher;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * QueryScheduler (potentially) assigns any {@link Query} that is to be executed to a 'query lane' and potentially
 * priority using the
 * {@link QuerySchedulingStrategy} that is defined in {@link QuerySchedulerConfig}
 * The purpose of the QueryScheduler is to give overall visibility into queries running
 * or pending at the QueryRunner level. This is currently used to cancel all the
 * parts of a pending query, but may be expanded in the future to offer more direct
 * visibility into query execution and resource usage.
 *
 * QueryRunners executing any computation asynchronously must register their queries with the QueryScheduler.
 *
 */
public class QueryScheduler implements QueryWatcher
{
  private final QuerySchedulingStrategy strategy;
  // maybe instead use a fancy library?
  private final Optional<Semaphore> totalQueryLimit;
  private final Map<String, Semaphore> laneLimits;

  private final SetMultimap<String, ListenableFuture<?>> queries;
  private final SetMultimap<String, String> queryDatasources;
  private final Set<Query<?>> runningQueries;

  public QueryScheduler(int totalNumThreads, QuerySchedulingStrategy strategy)
  {
    this.strategy = strategy;
    if (totalNumThreads > 0) {
      this.totalQueryLimit = Optional.of(new Semaphore(totalNumThreads));
    } else {
      this.totalQueryLimit = Optional.empty();
    }
    this.laneLimits = new HashMap<>();

    for (Object2IntMap.Entry<String> entry : strategy.getLaneLimits().object2IntEntrySet()) {
      laneLimits.put(entry.getKey(), new Semaphore(entry.getIntValue()));
    }
    this.queries = Multimaps.synchronizedSetMultimap(
        HashMultimap.create()
    );
    this.queryDatasources = Multimaps.synchronizedSetMultimap(
        HashMultimap.create()
    );
    this.runningQueries = Sets.newConcurrentHashSet();
  }

  public <T> Query<T> prioritizeAndLaneQuery(QueryPlus<T> query, Set<SegmentServer> segments)
  {
    return strategy.prioritizeAndLaneQuery(query, segments);
  }

  public void scheduleQuery(Query<?> query)
  {
    final String lane;
    try {
      if (!totalQueryLimit.isPresent() || totalQueryLimit.get().tryAcquire(0, TimeUnit.MILLISECONDS)) {
        lane = QueryContexts.getLane(query);
        if (!laneLimits.containsKey(lane)) {
          runningQueries.add(query);
          return;
        }
      } else {
        throw new QueryCapacityExceededException();
      }
    }
    catch (InterruptedException ex) {
      throw new QueryCapacityExceededException();
    }
    try {
      if (laneLimits.get(lane).tryAcquire(0, TimeUnit.MILLISECONDS)) {
        runningQueries.add(query);
        return;
      } else {
        throw new QueryCapacityExceededException(lane);
      }
    }
    catch (InterruptedException e) {
      throw new QueryCapacityExceededException(lane);
    }
  }

  public void completeQuery(Query<?> query)
  {
    if (runningQueries.remove(query)) {
      String lane = QueryContexts.getLane(query);
      if (laneLimits.containsKey(lane)) {
        laneLimits.get(lane).release();
      }
      totalQueryLimit.ifPresent(Semaphore::release);
    }
  }

  public <T> Sequence<T> run(Query<?> query, Sequence<T> resultSequence)
  {
    scheduleQuery(query);
    return resultSequence.withBaggage(() -> completeQuery(query));
  }

  public int getTotalAvailableCapacity()
  {
    return totalQueryLimit.map(Semaphore::availablePermits).orElse(-1);
  }

  public int getLaneAvailableCapacity(String lane)
  {
    if (laneLimits.containsKey(lane)) {
      return laneLimits.get(lane).availablePermits();
    }
    return -1;
  }


  public boolean cancelQuery(String id)
  {
    queryDatasources.removeAll(id);
    Set<ListenableFuture<?>> futures = queries.removeAll(id);
    boolean success = true;
    for (ListenableFuture<?> future : futures) {
      success = success && future.cancel(true);
    }
    return success;
  }

  /**
   * QueryRunners must use this method to register any pending queries.
   *
   * The given future may have cancel(true) called at any time, if cancellation of this query has been requested.
   *
   * @param query a query, which may be a subset of a larger query, as long as the underlying queryId is unchanged
   * @param future the future holding the execution status of the query
   */
  public void registerQueryFuture(Query<?> query, final ListenableFuture<?> future)
  {
    final String id = query.getId();
    final Set<String> datasources = query.getDataSource().getTableNames();
    queries.put(id, future);
    queryDatasources.putAll(id, datasources);
    future.addListener(
        () -> {
          // if you re-use queryId and cancel queries... you are going to have a bad time
          queries.remove(id, future);

          for (String datasource : datasources) {
            queryDatasources.remove(id, datasource);
          }
        },
        Execs.directExecutor()
    );
  }

  public Set<String> getQueryDatasources(final String queryId)
  {
    return queryDatasources.get(queryId);
  }

  @Override
  public void registerQuery(Query<?> query, ListenableFuture<?> future)
  {
    registerQueryFuture(query, future);
  }
}
