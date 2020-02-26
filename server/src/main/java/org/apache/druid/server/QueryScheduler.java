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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
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
 * QueryScheduler (potentially) assigns any {@link Query} that is to be executed to a 'query lane' using the
 * {@link QueryLaningStrategy} that is defined in {@link QuerySchedulerConfig}.
 *
 * As a {@link QueryWatcher}, it also provides cancellation facilities.
 *
 */
public class QueryScheduler implements QueryWatcher
{
  private final QueryLaningStrategy laningStrategy;
  private final Optional<Semaphore> totalQueryLimit;
  private final Map<String, Semaphore> laneLimits;

  private final SetMultimap<String, ListenableFuture<?>> queries;
  private final SetMultimap<String, String> queryDatasources;
  private final Set<Query<?>> runningQueries;

  public QueryScheduler(int totalNumThreads, QueryLaningStrategy laningStrategy)
  {
    this.laningStrategy = laningStrategy;
    if (totalNumThreads > 0) {
      this.totalQueryLimit = Optional.of(new Semaphore(totalNumThreads));
    } else {
      this.totalQueryLimit = Optional.empty();
    }
    this.laneLimits = new HashMap<>();

    for (Object2IntMap.Entry<String> entry : laningStrategy.getLaneLimits().object2IntEntrySet()) {
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

  @Override
  public void registerQueryFuture(Query<?> query, ListenableFuture<?> future)
  {
    final String id = query.getId();
    final Set<String> datasources = query.getDataSource().getTableNames();
    queries.put(id, future);
    queryDatasources.putAll(id, datasources);
    future.addListener(
        () -> {
          queries.remove(id, future);
          for (String datasource : datasources) {
            queryDatasources.remove(id, datasource);
          }
        },
        Execs.directExecutor()
    );
  }

  /**
   * Assign a query a lane (if not set)
   */
  public <T> Query<T> laneQuery(QueryPlus<T> query, Set<SegmentServerSelector> segments)
  {
    if (QueryContexts.getLane(query.getQuery()) != null) {
      return query.getQuery();
    }
    return laningStrategy.laneQuery(query, segments);
  }

  /**
   * Run a query with the scheduler, attempting to acquire a semaphore from the total and lane specific query capacities
   */
  public <T> Sequence<T> run(Query<?> query, Sequence<T> resultSequence)
  {
    scheduleQuery(query);
    return resultSequence.withBaggage(() -> completeQuery(query));
  }

  /**
   * Forcibly cancel all futures that have been registered to a specific query id
   */
  public boolean cancelQuery(String id)
  {
    // if you re-use queryId and cancel queries... you are going to have a bad time
    queryDatasources.removeAll(id);
    Set<ListenableFuture<?>> futures = queries.removeAll(id);
    boolean success = true;
    for (ListenableFuture<?> future : futures) {
      success = success && future.cancel(true);
    }
    return success;
  }

  public Set<String> getQueryDatasources(final String queryId)
  {
    return queryDatasources.get(queryId);
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

  /**
   * Acquire semaphore from total capacity and lane capacity (if query is assigned a lane that exists)
   */
  @VisibleForTesting
  void scheduleQuery(Query<?> query)
  {
    final String lane;
    try {
      if (!totalQueryLimit.isPresent() || totalQueryLimit.get().tryAcquire(0, TimeUnit.MILLISECONDS)) {
        lane = QueryContexts.getLane(query);
        // if no lane, we are done
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
    // if we got here, the query belongs to a lane, acquire the semaphore for it
    try {
      if (laneLimits.get(lane).tryAcquire(0, TimeUnit.MILLISECONDS)) {
        runningQueries.add(query);
      } else {
        throw new QueryCapacityExceededException(lane);
      }
    }
    catch (InterruptedException e) {
      throw new QueryCapacityExceededException(lane);
    }
  }

  /**
   * Release semaphores help by query
   */
  private void completeQuery(Query<?> query)
  {
    if (runningQueries.remove(query)) {
      String lane = QueryContexts.getLane(query);
      if (laneLimits.containsKey(lane)) {
        laneLimits.get(lane).release();
      }
      totalQueryLimit.ifPresent(Semaphore::release);
    }
  }
}
