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

package io.druid.query.materializedview;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.ServerSelector;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import io.druid.timeline.TimelineObjectHolder;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class DatasourceOptimizer 
{
  private static final ReadWriteLock lock = new ReentrantReadWriteLock();
  private static ConcurrentHashMap<Derivative, AtomicLong> derivativesHitCount = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, AtomicLong> totalCount = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, AtomicLong> hitCount = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, AtomicLong> costTime = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, ConcurrentHashMap<Set<String>, AtomicLong>> missFields = new ConcurrentHashMap<>();
  private static TimelineServerView serverView = null;

  @Inject
  public DatasourceOptimizer(TimelineServerView serverView)
  {
    this.serverView = serverView;
  }

  /**
   * Do main work about materialized view selection: transform user query to one or more sub-queries.
   * 
   * In the sub-query, the dataSource is the derivative of dataSource in user query, and sum of all sub-queries' intervals
   * equals the interval in user query
   * 
   * Derived dataSource with smallest average size of segments have highest priority to replace the datasource in user query
   * 
   * @param query only TopNQuery/TimeseriesQuery/GroupByQuery can be optimized
   * @return a list of queries with specified derived dataSources and intervals 
   */
  public static List<Query> optimize(Query query)
  {
    long start = System.currentTimeMillis();
    // only topN/timeseries/groupby query can be optimized
    // only TableDataSource can be optimiezed
    if (!(query instanceof TopNQuery || query instanceof TimeseriesQuery || query instanceof GroupByQuery)
        || !(query.getDataSource() instanceof TableDataSource)) {
      return Lists.newArrayList(query);
    }

    String datasourceName = ((TableDataSource) query.getDataSource()).getName();
    // get all derivatives for datasource in query. The derivatives set is sorted by average size of per segment granularity.
    ImmutableSortedSet<Derivative> derivatives = DerivativesManager.getDerivatives(datasourceName);
    if (derivatives.isEmpty()) {
      return Lists.newArrayList(query);
    }

    try {
      lock.readLock().lock();
      totalCount.putIfAbsent(datasourceName, new AtomicLong(0));
      hitCount.putIfAbsent(datasourceName, new AtomicLong(0));
      costTime.putIfAbsent(datasourceName, new AtomicLong(0));
      totalCount.get(datasourceName).incrementAndGet();
      
      // get all fields which the query required
      Set<String> requiredFields = MaterializedViewUtils.getRequiredFields(query);
      
      Set<Derivative> derivativesWithRequiredFields = Sets.newHashSet();
      for (Derivative derivative : derivatives) {
        derivativesHitCount.putIfAbsent(derivative, new AtomicLong(0));
        if (derivative.getColumns().containsAll(requiredFields)) {
          derivativesWithRequiredFields.add(derivative);
        }
      }
      // if no derivatives contains all required dimensions, this materialized view selection failed.
      if (derivativesWithRequiredFields.isEmpty()) {
        missFields.putIfAbsent(datasourceName, new ConcurrentHashMap<>());
        missFields.get(datasourceName).putIfAbsent(requiredFields, new AtomicLong(0));
        missFields.get(datasourceName).get(requiredFields).incrementAndGet();
        costTime.get(datasourceName).addAndGet(System.currentTimeMillis() - start);
        return Lists.newArrayList(query);
      }

      // 
      List<Query> queries = Lists.newArrayList();
      List<Interval> remainingQueryIntervals = (List<Interval>) query.getIntervals();
      for (Derivative derivative : derivativesWithRequiredFields) {
        List<Interval> derivativeIntervals = Lists.newArrayList();
        for (Interval interval : remainingQueryIntervals) {
          serverView.getTimeline(new TableDataSource(derivative.getName()))
              .lookup(interval)
              .forEach(new Consumer<TimelineObjectHolder<String, ServerSelector>>() {
                @Override
                public void accept(TimelineObjectHolder<String, ServerSelector> stringServerSelectorTimelineObjectHolder) 
                {
                  derivativeIntervals.add(stringServerSelectorTimelineObjectHolder.getInterval());
                }
              }
            );
        }
        // if the derivative does not contain any parts of intervals in the query, the derivative will not be selected. 
        if (derivativeIntervals.isEmpty()) {
          continue;
        }
        
        remainingQueryIntervals = MaterializedViewUtils.minus(remainingQueryIntervals, derivativeIntervals);
        queries.add(
            query.withDataSource(new TableDataSource(derivative.getName()))
                .withQuerySegmentSpec(new MultipleIntervalSegmentSpec(derivativeIntervals))
        );
        derivativesHitCount.get(derivative).incrementAndGet();
        if (remainingQueryIntervals.isEmpty()) {
          break;
        }
      }

      if (queries.isEmpty()) {
        costTime.get(datasourceName).addAndGet(System.currentTimeMillis() - start);
        return Lists.newArrayList(query);
      }

      //after materialized view selection, the result of the remaining query interval will be computed based on the original datasource. 
      if (!remainingQueryIntervals.isEmpty()) {
        queries.add(query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(remainingQueryIntervals)));
      }
      hitCount.get(datasourceName).incrementAndGet();
      costTime.get(datasourceName).addAndGet(System.currentTimeMillis() - start);
      return queries;
    } 
    finally {
      lock.readLock().unlock();
    }
  }

  public List<DatasourceOptimizerStats> getAndResetStats() 
  {
    ImmutableMap<Derivative, AtomicLong> derivativesHitCountSnapshot;
    ImmutableMap<String, AtomicLong> totalCountSnapshot;
    ImmutableMap<String, AtomicLong> hitCountSnapshot;
    ImmutableMap<String, AtomicLong> costTimeSnapshot;
    ImmutableMap<String, ConcurrentHashMap<Set<String>, AtomicLong>> missFieldsSnapshot;
    try {
      lock.writeLock().lock();
      derivativesHitCountSnapshot = ImmutableMap.copyOf(derivativesHitCount);
      totalCountSnapshot = ImmutableMap.copyOf(totalCount);
      hitCountSnapshot = ImmutableMap.copyOf(hitCount);
      costTimeSnapshot = ImmutableMap.copyOf(costTime);
      missFieldsSnapshot = ImmutableMap.copyOf(missFields);
      derivativesHitCount.clear();
      totalCount.clear();
      hitCount.clear();
      costTime.clear();
      missFields.clear();
    } 
    finally {
      lock.writeLock().unlock();
    }
    List<DatasourceOptimizerStats> stats = Lists.newArrayList();
    Map<String, Set<Derivative>> baseToDerivatives = DerivativesManager.getAllDerivatives();
    for (String base : baseToDerivatives.keySet()) {
      Map<String, Long> derivativesStat = Maps.newHashMap();
      for (Derivative derivative : baseToDerivatives.get(base)) {
        derivativesStat.put(derivative.getName(), derivativesHitCountSnapshot.getOrDefault(derivative, new AtomicLong(0)).get());
      }
      stats.add(
          new DatasourceOptimizerStats(
              base,
              hitCountSnapshot.getOrDefault(base, new AtomicLong(0)).get(),
              totalCountSnapshot.getOrDefault(base, new AtomicLong(0)).get(),
              costTimeSnapshot.getOrDefault(base, new AtomicLong(0)).get(),
              missFieldsSnapshot.getOrDefault(base, new ConcurrentHashMap<>()),
              derivativesStat
          )
      );
    }
    return stats;
  }
}
