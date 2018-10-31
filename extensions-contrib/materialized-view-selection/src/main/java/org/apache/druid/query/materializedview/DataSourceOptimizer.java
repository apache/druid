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

package org.apache.druid.query.materializedview;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.inject.Inject;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DataSourceOptimizer
{
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final TimelineServerView serverView;
  private ConcurrentHashMap<String, AtomicLong> derivativesHitCount = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, AtomicLong> totalCount = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, AtomicLong> hitCount = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, AtomicLong> costTime = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, ConcurrentHashMap<Set<String>, AtomicLong>> missFields = new ConcurrentHashMap<>();
  
  @Inject
  public DataSourceOptimizer(TimelineServerView serverView) 
  {
    this.serverView = serverView;
  }

  /**
   * Do main work about materialized view selection: transform user query to one or more sub-queries.
   * 
   * In the sub-query, the dataSource is the derivative of dataSource in user query, and sum of all sub-queries' 
   * intervals equals the interval in user query
   * 
   * Derived dataSource with smallest average data size per segment granularity have highest priority to replace the
   * datasource in user query
   * 
   * @param query only TopNQuery/TimeseriesQuery/GroupByQuery can be optimized
   * @return a list of queries with specified derived dataSources and intervals 
   */
  public List<Query> optimize(Query query)
  {
    long start = System.currentTimeMillis();
    // only topN/timeseries/groupby query can be optimized
    // only TableDataSource can be optimiezed
    if (!(query instanceof TopNQuery || query instanceof TimeseriesQuery || query instanceof GroupByQuery)
        || !(query.getDataSource() instanceof TableDataSource)) {
      return Collections.singletonList(query);
    }
    String datasourceName = ((TableDataSource) query.getDataSource()).getName();
    // get all derivatives for datasource in query. The derivatives set is sorted by average size of 
    // per segment granularity.
    Set<DerivativeDataSource> derivatives = DerivativeDataSourceManager.getDerivatives(datasourceName);
    
    if (derivatives.isEmpty()) {
      return Collections.singletonList(query);
    }
    lock.readLock().lock();
    try {
      totalCount.putIfAbsent(datasourceName, new AtomicLong(0));
      hitCount.putIfAbsent(datasourceName, new AtomicLong(0));
      costTime.putIfAbsent(datasourceName, new AtomicLong(0));
      totalCount.get(datasourceName).incrementAndGet();
      
      // get all fields which the query required
      Set<String> requiredFields = MaterializedViewUtils.getRequiredFields(query);
      
      Set<DerivativeDataSource> derivativesWithRequiredFields = new HashSet<>();
      for (DerivativeDataSource derivativeDataSource : derivatives) {
        derivativesHitCount.putIfAbsent(derivativeDataSource.getName(), new AtomicLong(0));
        if (derivativeDataSource.getColumns().containsAll(requiredFields)) {
          derivativesWithRequiredFields.add(derivativeDataSource);
        }
      }
      // if no derivatives contains all required dimensions, this materialized view selection failed.
      if (derivativesWithRequiredFields.isEmpty()) {
        missFields.putIfAbsent(datasourceName, new ConcurrentHashMap<>());
        missFields.get(datasourceName).putIfAbsent(requiredFields, new AtomicLong(0));
        missFields.get(datasourceName).get(requiredFields).incrementAndGet();
        costTime.get(datasourceName).addAndGet(System.currentTimeMillis() - start);
        return Collections.singletonList(query);
      }
      
      List<Query> queries = new ArrayList<>();
      List<Interval> remainingQueryIntervals = (List<Interval>) query.getIntervals();
      
      for (DerivativeDataSource derivativeDataSource : ImmutableSortedSet.copyOf(derivativesWithRequiredFields)) {
        final List<Interval> derivativeIntervals = remainingQueryIntervals.stream()
            .flatMap(interval -> serverView
                .getTimeline((new TableDataSource(derivativeDataSource.getName())))
                .lookup(interval)
                .stream()
                .map(TimelineObjectHolder::getInterval)
            )
            .collect(Collectors.toList());
        // if the derivative does not contain any parts of intervals in the query, the derivative will
        // not be selected. 
        if (derivativeIntervals.isEmpty()) {
          continue;
        }
        
        remainingQueryIntervals = MaterializedViewUtils.minus(remainingQueryIntervals, derivativeIntervals);
        queries.add(
            query.withDataSource(new TableDataSource(derivativeDataSource.getName()))
                .withQuerySegmentSpec(new MultipleIntervalSegmentSpec(derivativeIntervals))
        );
        derivativesHitCount.get(derivativeDataSource.getName()).incrementAndGet();
        if (remainingQueryIntervals.isEmpty()) {
          break;
        }
      }

      if (queries.isEmpty()) {
        costTime.get(datasourceName).addAndGet(System.currentTimeMillis() - start);
        return Collections.singletonList(query);
      }

      //after materialized view selection, the result of the remaining query interval will be computed based on
      // the original datasource. 
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

  public List<DataSourceOptimizerStats> getAndResetStats() 
  {
    ImmutableMap<String, AtomicLong> derivativesHitCountSnapshot;
    ImmutableMap<String, AtomicLong> totalCountSnapshot;
    ImmutableMap<String, AtomicLong> hitCountSnapshot;
    ImmutableMap<String, AtomicLong> costTimeSnapshot;
    ImmutableMap<String, ConcurrentHashMap<Set<String>, AtomicLong>> missFieldsSnapshot;
    lock.writeLock().lock();
    try {
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
    List<DataSourceOptimizerStats> stats = new ArrayList<>();
    Map<String, Set<DerivativeDataSource>> baseToDerivatives = DerivativeDataSourceManager.getAllDerivatives();
    for (Map.Entry<String, Set<DerivativeDataSource>> entry : baseToDerivatives.entrySet()) {
      Map<String, Long> derivativesStat = new HashMap<>();
      for (DerivativeDataSource derivative : entry.getValue()) {
        derivativesStat.put(
            derivative.getName(),
            derivativesHitCountSnapshot.getOrDefault(derivative.getName(), new AtomicLong(0)).get()
        );
      }
      stats.add(
          new DataSourceOptimizerStats(
              entry.getKey(),
              hitCountSnapshot.getOrDefault(entry.getKey(), new AtomicLong(0)).get(),
              totalCountSnapshot.getOrDefault(entry.getKey(), new AtomicLong(0)).get(),
              costTimeSnapshot.getOrDefault(entry.getKey(), new AtomicLong(0)).get(),
              missFieldsSnapshot.getOrDefault(entry.getKey(), new ConcurrentHashMap<>()),
              derivativesStat
          )
      );
    }
    return stats;
  }
}
