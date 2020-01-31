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

package org.apache.druid.query;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


public class QueryScheduler implements QueryWatcher
{
  // maybe instead use a fancy library?
  private final Semaphore activeQueries;
  private final Map<String, Semaphore> lanes;
  private final QuerySchedulingStrategy strategy;

  private final SetMultimap<String, ListenableFuture<?>> queries;
  private final SetMultimap<String, String> queryDatasources;
  private final Set<Query<?>> runningQueries;

  public QueryScheduler(
      int totalNumThreads,
      QuerySchedulingStrategy strategy
  )
  {
    this.strategy = strategy;
    this.activeQueries = new Semaphore(totalNumThreads);
    this.lanes = new HashMap<>();

    for (Object2IntMap.Entry<String> entry : strategy.getLaneLimits().object2IntEntrySet()) {
      lanes.put(entry.getKey(), new Semaphore(entry.getIntValue()));
    }
    this.queries = Multimaps.synchronizedSetMultimap(
        HashMultimap.create()
    );
    this.queryDatasources = Multimaps.synchronizedSetMultimap(
        HashMultimap.create()
    );
    this.runningQueries = Sets.newConcurrentHashSet();
  }

  public <T> Query<T> schedule(QueryPlus<T> query, Set<SegmentDescriptor> descriptors)
  {
    try {
      if (activeQueries.tryAcquire(0, TimeUnit.MILLISECONDS)) {
        Query<T> prioritizedAndLaned = strategy.prioritizeQuery(query, descriptors);
        String lane = prioritizedAndLaned.getContextValue("queryLane");
        if (lanes.containsKey(lane)) {
          if (lanes.get(lane).tryAcquire(0, TimeUnit.MILLISECONDS)) {
            runningQueries.add(prioritizedAndLaned);
            return prioritizedAndLaned;
          }
        } else {
          runningQueries.add(prioritizedAndLaned);
          return prioritizedAndLaned;
        }
      }
    }
    catch (InterruptedException ex) {
      throw new QueryCapacityExceededException();
    }
    throw new QueryCapacityExceededException();
  }

  public <T> Sequence<T> run(Query<?> query, Sequence<T> resultSequence)
  {
    // if really doing release on the bagage, should merge most of schedule into here to tie acquire with release
    // alternatively, if we want to release as late as possible, rework this to release in the registered queries future
    // listener, though we need a guaranteed unique queryId and slight refactor to make this work..
    return resultSequence.withBaggage(() -> {
      if (runningQueries.remove(query)) {
        String lane = query.getContextValue("queryLane");
        if (lanes.containsKey(lane)) {
          lanes.get(lane).release();
        }
        activeQueries.release();
      }
    });
  }

  public int getTotalAvailableCapacity()
  {
    return activeQueries.availablePermits();
  }

  public int getLaneAvailableCapacity(String lane)
  {
    if (lanes.containsKey(lane)) {
      return lanes.get(lane).availablePermits();
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

  @Override
  public void registerQuery(Query<?> query, final ListenableFuture<?> future)
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
}
