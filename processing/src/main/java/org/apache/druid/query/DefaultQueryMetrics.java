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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.FastIntervalStringFormatter;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * DefaultQueryMetrics is unsafe for use from multiple threads. It fails with RuntimeException on access not from the
 * thread where it was constructed. To "transfer" DefaultQueryMetrics from one thread to another {@link #ownerThread}
 * field should be updated.
 */
public class DefaultQueryMetrics<QueryType extends Query<?>> implements QueryMetrics<QueryType>
{
  public static final String QUERY_TIME = "query/time";
  public static final String QUERY_BYTES = "query/bytes";
  public static final String QUERY_CPU_TIME = "query/cpu/time";
  public static final String QUERY_WAIT_TIME = "query/wait/time";
  public static final String QUERY_SEGMENT_TIME = "query/segment/time";
  public static final String QUERY_SEGMENT_AND_CACHE_TIME = "query/segmentAndCache/time";
  public static final String QUERY_RESULT_CACHE_HIT = "query/resultCache/hit";
  public static final String QUERY_ON_DEMAND_LOAD_BATCH_TIME = "query/load/batch/time";
  public static final String QUERY_ON_DEMAND_LOAD_TIME_AVG = "query/load/time/avg";
  public static final String QUERY_ON_DEMAND_LOAD_TIME_MAX = "query/load/time/max";
  public static final String QUERY_ON_DEMAND_WAIT_TIME_AVG = "query/load/wait/avg";
  public static final String QUERY_ON_DEMAND_WAIT_TIME_MAX = "query/load/wait/max";
  public static final String QUERY_ON_DEMAND_LOAD_COUNT = "query/load/count";
  public static final String QUERY_ON_DEMAND_LOAD_BYTES = "query/load/bytes/total";

  protected final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
  protected final Map<String, Number> metrics = new HashMap<>();

  /**
   * Non-final to give the subclasses ability to reassign it.
   */
  protected Thread ownerThread = Thread.currentThread();

  public static String getTableNamesAsString(Set<String> tableNames)
  {
    if (tableNames.size() == 1) {
      return Iterables.getOnlyElement(tableNames);
    } else {
      return tableNames.stream().sorted().collect(Collectors.toList()).toString();
    }
  }

  public static String[] getIntervalsAsStringArray(Collection<Interval> intervals)
  {
    return intervals.stream().map(FastIntervalStringFormatter::format).toArray(String[]::new);

  }

  protected void checkModifiedFromOwnerThread()
  {
    if (!Thread.currentThread().equals(ownerThread)) {
      throw new IllegalStateException(
          "DefaultQueryMetrics must not be modified from multiple threads. If it is needed to gather dimension or "
          + "metric information from multiple threads or from an async thread, this information should explicitly be "
          + "passed between threads (e. g. using Futures), or this DefaultQueryMetrics's ownerThread should be "
          + "reassigned explicitly");
    }
  }

  protected void setDimension(String dimension, Object value)
  {
    checkModifiedFromOwnerThread();
    builder.setDimension(dimension, value);
  }

  @Override
  public void query(QueryType query)
  {
    dataSource(query);
    queryType(query);
    interval(query);
    hasFilters(query);
    duration(query);
    queryId(query);
    subQueryId(query);
    sqlQueryId(query);
    context(query);
  }

  @Override
  public void dataSource(QueryType query)
  {
    setDimension(DruidMetrics.DATASOURCE, getTableNamesAsString(query.getDataSource().getTableNames()));
  }

  @Override
  public void queryType(QueryType query)
  {
    setDimension(DruidMetrics.TYPE, query.getType());
  }

  @Override
  public void interval(QueryType query)
  {
    checkModifiedFromOwnerThread();
    builder.setDimension(
        DruidMetrics.INTERVAL,
        getIntervalsAsStringArray(query.getIntervals())
    );
  }

  @Override
  public void hasFilters(QueryType query)
  {
    setDimension("hasFilters", String.valueOf(query.hasFilters()));
  }

  @Override
  public void duration(QueryType query)
  {
    setDimension("duration", query.getDuration().toString());
  }

  @Override
  public void queryId(QueryType query)
  {
    setDimension(DruidMetrics.ID, StringUtils.nullToEmptyNonDruidDataString(query.getId()));
  }

  @Override
  public void queryId(String queryId)
  {
    setDimension(DruidMetrics.ID, StringUtils.nullToEmptyNonDruidDataString(queryId));
  }

  @Override
  public void subQueryId(QueryType query)
  {
    // Emit nothing by default.
  }

  @Override
  public void sqlQueryId(QueryType query)
  {
    // Emit nothing by default.
  }

  @Override
  public void sqlQueryId(String sqlQueryId)
  {
    // Emit nothing by default.
  }

  @Override
  public void context(QueryType query)
  {
    setDimension("context", query.getContext() == null ? ImmutableMap.of() : query.getContext());
  }

  @Override
  public void server(String host)
  {
    setDimension("server", host);
  }

  @Override
  public void remoteAddress(String remoteAddress)
  {
    setDimension("remoteAddress", remoteAddress);
  }

  @Override
  public void status(String status)
  {
    setDimension(DruidMetrics.STATUS, status);
  }

  @Override
  public void success(boolean success)
  {
    setDimension("success", String.valueOf(success));
  }

  @Override
  public void statusCode(int code)
  {
    setDimension(DruidMetrics.STATUS_CODE, code);
  }

  @Override
  public void segment(String segmentIdentifier)
  {
    setDimension("segment", segmentIdentifier);
  }

  @Override
  public void projection(String projection)
  {
    setDimension("projection", projection);
  }

  @Override
  public void identity(String identity)
  {
    // Emit nothing by default.
  }

  @Override
  public void vectorized(final boolean vectorized)
  {
    setDimension("vectorized", vectorized);
  }

  @Override
  public void parallelMergeParallelism(final int parallelism)
  {
    // Emit nothing by default.
  }

  @Override
  public BitmapResultFactory<?> makeBitmapResultFactory(BitmapFactory factory)
  {
    return new DefaultBitmapResultFactory(factory);
  }

  @Override
  public QueryMetrics<QueryType> reportQueryTime(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_TIME, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportQueryBytes(long byteCount)
  {
    return reportMetric(QUERY_BYTES, byteCount);
  }

  @Override
  public QueryMetrics<QueryType> reportWaitTime(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_WAIT_TIME, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentTime(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_SEGMENT_TIME, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentAndCacheTime(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_SEGMENT_AND_CACHE_TIME, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentOnDemandLoadTime(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_ON_DEMAND_LOAD_BATCH_TIME, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentOnDemandLoadTimeAvg(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_ON_DEMAND_LOAD_TIME_AVG, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentOnDemandLoadWaitTimeMax(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_ON_DEMAND_WAIT_TIME_MAX, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentOnDemandLoadWaitTimeAvg(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_ON_DEMAND_WAIT_TIME_AVG, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentOnDemandLoadTimeMax(long timeNs)
  {
    return reportMillisTimeMetric(QUERY_ON_DEMAND_LOAD_TIME_MAX, timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentOnDemandLoadBytes(long byteCount)
  {
    return reportMetric(QUERY_ON_DEMAND_LOAD_BYTES, byteCount);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentOnDemandLoadCount(long count)
  {
    return reportMetric(QUERY_ON_DEMAND_LOAD_COUNT, count);
  }

  @Override
  public QueryMetrics<QueryType> reportResultCachePoll(boolean hit)
  {
    return reportMetric(QUERY_RESULT_CACHE_HIT, hit ? 1 : 0);
  }

  @Override
  public QueryMetrics<QueryType> reportCpuTime(long timeNs)
  {
    return reportMetric(QUERY_CPU_TIME, TimeUnit.NANOSECONDS.toMicros(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTimeToFirstByte(long timeNs)
  {
    return reportMillisTimeMetric("query/node/ttfb", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportBackPressureTime(long timeNs)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTime(long timeNs)
  {
    return reportMillisTimeMetric("query/node/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportNodeBytes(long byteCount)
  {
    return reportMetric("query/node/bytes", byteCount);
  }

  @Override
  public QueryMetrics<QueryType> reportBitmapConstructionTime(long timeNs)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentRows(long numRows)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportPreFilteredRows(long numRows)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeParallelism(int parallelism)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputSequences(long numSequences)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputRows(long numRows)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeOutputRows(long numRows)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTaskCount(long numTasks)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalCpuTime(long timeNs)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalTime(long timeNs)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeFastestPartitionTime(long timeNs)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeSlowestPartitionTime(long timeNs)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportQueriedSegmentCount(long segmentCount)
  {
    // Don't emit by default.
    return this;
  }

  @Override
  public void emit(ServiceEmitter emitter)
  {
    checkModifiedFromOwnerThread();
    for (Map.Entry<String, Number> metric : metrics.entrySet()) {
      emitter.emit(builder.setMetric(metric.getKey(), metric.getValue()));
    }
    metrics.clear();
  }

  @Override
  public Map<String, Object> generateQueryStatsMapFromMetrics()
  {
    Map<String, Object> queryStatsMap = new LinkedHashMap<>();
    appendMetricsToQueryStatsMap("query/time", queryStatsMap);
    appendMetricsToQueryStatsMap("query/bytes", queryStatsMap);

    return queryStatsMap;
  }

  protected void appendMetricsToQueryStatsMap(String key, Map<String, Object> map)
  {
    map.put(key, metrics.getOrDefault(key, 0));
  }

  protected QueryMetrics<QueryType> reportMetric(String metricName, Number value)
  {
    checkModifiedFromOwnerThread();
    metrics.put(metricName, value);
    return this;
  }

  private QueryMetrics<QueryType> reportMillisTimeMetric(String metricName, long timeNs)
  {
    return reportMetric(metricName, TimeUnit.NANOSECONDS.toMillis(timeNs));
  }
}
