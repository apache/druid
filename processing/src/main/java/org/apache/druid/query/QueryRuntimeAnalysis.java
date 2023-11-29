package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.filter.Filter;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class QueryRuntimeAnalysis<QueryType extends Query<?>, V extends QueryMetrics<QueryType> > implements QueryMetrics<QueryType>
{
  protected V delegate;
  protected Map<String, Object> debugInfo;
  protected Map<String, Number> metrics;

  public QueryRuntimeAnalysis(V delegate)
  {
    this.delegate = delegate;
    this.debugInfo = new HashMap<>();
    this.metrics = new HashMap<>();
  }

  /**
   * Non final to give subclasses ability to reassign it.
   */
  protected Thread ownerThread = Thread.currentThread();

  private static String getTableNamesAsString(DataSource dataSource)
  {
    final Set<String> names = dataSource.getTableNames();

    if (names.size() == 1) {
      return Iterables.getOnlyElement(names);
    } else {
      return names.stream().sorted().collect(Collectors.toList()).toString();
    }
  }

  protected void checkModifiedFromOwnerThread()
  {
    if (Thread.currentThread() != ownerThread) {
      throw new IllegalStateException(
          "QueryRuntimeAnalysis must not be modified from multiple threads. If it is needed to gather dimension or "
          + "metric information from multiple threads or from an async thread, this information should explicitly be "
          + "passed between threads (e. g. using Futures), or this QueryRuntimeAnalysis's ownerThread should be "
          + "reassigned explicitly");
    }
  }

  protected void setDimension(String dimension, Object value)
  {
    checkModifiedFromOwnerThread();
    debugInfo.put(dimension, value);
  }

  @Override
  public void query(QueryType query)
  {
    delegate.query(query);
    setDimension(DruidMetrics.DATASOURCE, getTableNamesAsString(query.getDataSource()));
    setDimension(DruidMetrics.TYPE, query.getType());
    setDimension(
        DruidMetrics.INTERVAL,
        query.getIntervals().stream().map(Interval::toString).toArray(String[]::new)
    );
    setDimension("hasFilters", String.valueOf(query.hasFilters()));
    setDimension("duration", query.getDuration().toString());
    setDimension(DruidMetrics.ID, StringUtils.nullToEmptyNonDruidDataString(query.getId()));
    Set<String> subqueries = (Set<String>) debugInfo.computeIfAbsent("subQueries", k -> new HashSet<>());
    subqueries.add(query.getId());
    setDimension("sqlQueryId", query.getSqlQueryId());
    setDimension("context", query.getContext() == null ? ImmutableMap.of() : query.getContext());
  }

  @Override
  public void dataSource(QueryType query)
  {
    delegate.dataSource(query);
    setDimension(DruidMetrics.DATASOURCE, getTableNamesAsString(query.getDataSource()));
  }

  @Override
  public void queryType(QueryType query)
  {
    delegate.queryType(query);
    setDimension(DruidMetrics.TYPE, query.getType());
  }

  @Override
  public void interval(QueryType query)
  {
    delegate.interval(query);
    checkModifiedFromOwnerThread();
    setDimension(
        DruidMetrics.INTERVAL,
        query.getIntervals().stream().map(Interval::toString).toArray(String[]::new)
    );
  }

  @Override
  public void hasFilters(QueryType query)
  {
    delegate.hasFilters(query);
    setDimension("hasFilters", String.valueOf(query.hasFilters()));
  }

  @Override
  public void duration(QueryType query)
  {
    delegate.duration(query);
    setDimension("duration", query.getDuration().toString());
  }

  @Override
  public void queryId(QueryType query)
  {
    delegate.queryId(query);
    setDimension(DruidMetrics.ID, StringUtils.nullToEmptyNonDruidDataString(query.getId()));
  }

  @Override
  public void queryId(String queryId)
  {
    delegate.queryId(queryId);
    setDimension(DruidMetrics.ID, StringUtils.nullToEmptyNonDruidDataString(queryId));
  }

  @Override
  public void subQueryId(QueryType query)
  {
    delegate.subQueryId(query);
    Set<String> subqueries = (Set<String>) debugInfo.computeIfAbsent("subQueries", k -> new HashSet<>());
    subqueries.add(query.getId());
  }

  @Override
  public void sqlQueryId(QueryType query)
  {
    delegate.sqlQueryId(query);
    setDimension("sqlQueryId", query.getSqlQueryId());
  }

  @Override
  public void sqlQueryId(String sqlQueryId)
  {
    delegate.sqlQueryId(sqlQueryId);
    debugInfo.put("sqlQueryId", sqlQueryId);
  }

  @Override
  public void context(QueryType query)
  {
    delegate.context(query);
    setDimension("context", query.getContext() == null ? ImmutableMap.of() : query.getContext());
  }

  @Override
  public void server(String host)
  {
    delegate.server(host);
    setDimension("server", host);
  }

  @Override
  public void remoteAddress(String remoteAddress)
  {
    delegate.remoteAddress(remoteAddress);
    setDimension("remoteAddress", remoteAddress);
  }

  @Override
  public void status(String status)
  {
    delegate.status(status);
    setDimension(DruidMetrics.STATUS, status);
  }

  @Override
  public void success(boolean success)
  {
    delegate.success(success);
    setDimension("success", String.valueOf(success));
  }

  @Override
  public void segment(String segmentIdentifier)
  {
    delegate.segment(segmentIdentifier);
    setDimension("segment", segmentIdentifier);
  }

  @Override
  public void preFilters(List<Filter> preFilters)
  {
    delegate.preFilters(preFilters);
  }

  @Override
  public void postFilters(List<Filter> postFilters)
  {
    delegate.postFilters(postFilters);
  }

  @Override
  public void identity(String identity)
  {
    delegate.identity(identity);
  }

  @Override
  public void vectorized(final boolean vectorized)
  {
    delegate.vectorized(vectorized);
    setDimension("vectorized", vectorized);
  }

  @Override
  public void parallelMergeParallelism(final int parallelism)
  {
    delegate.parallelMergeParallelism(parallelism);
    setDimension("parallelMergeParallelism", parallelism);
  }

  @Override
  public BitmapResultFactory<?> makeBitmapResultFactory(BitmapFactory factory)
  {
    return delegate.makeBitmapResultFactory(factory);
  }

  @Override
  public QueryMetrics<QueryType> reportQueryTime(long timeNs)
  {
    delegate.reportQueryTime(timeNs);
    return reportMillisTimeMetric("query/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportQueryBytes(long byteCount)
  {
    delegate.reportQueryBytes(byteCount);
    return reportMetric("query/bytes", byteCount);
  }

  @Override
  public QueryMetrics<QueryType> reportWaitTime(long timeNs)
  {
    delegate.reportWaitTime(timeNs);
    return reportMillisTimeMetric("query/wait/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentTime(long timeNs)
  {
    delegate.reportSegmentTime(timeNs);
    return reportMillisTimeMetric("query/segment/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentAndCacheTime(long timeNs)
  {
    delegate.reportSegmentAndCacheTime(timeNs);
    return reportMillisTimeMetric("query/segmentAndCache/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportCpuTime(long timeNs)
  {
    delegate.reportCpuTime(timeNs);
    return reportMetric("query/cpu/time", TimeUnit.NANOSECONDS.toMicros(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTimeToFirstByte(long timeNs)
  {
    delegate.reportNodeTimeToFirstByte(timeNs);
    return reportMillisTimeMetric("query/node/ttfb", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportBackPressureTime(long timeNs)
  {
    delegate.reportBackPressureTime(timeNs);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTime(long timeNs)
  {
    delegate.reportNodeTime(timeNs);
    return reportMillisTimeMetric("query/node/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportNodeBytes(long byteCount)
  {
    delegate.reportNodeBytes(byteCount);
    return reportMetric("query/node/bytes", byteCount);
  }

  @Override
  public QueryMetrics<QueryType> reportBitmapConstructionTime(long timeNs)
  {
    delegate.reportBitmapConstructionTime(timeNs);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentRows(long numRows)
  {
    delegate.reportSegmentRows(numRows);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportPreFilteredRows(long numRows)
  {
    delegate.reportPreFilteredRows(numRows);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeParallelism(int parallelism)
  {
    delegate.reportParallelMergeParallelism(parallelism);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputSequences(long numSequences)
  {
    delegate.reportParallelMergeInputSequences(numSequences);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputRows(long numRows)
  {
    delegate.reportParallelMergeInputRows(numRows);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeOutputRows(long numRows)
  {
    delegate.reportParallelMergeOutputRows(numRows);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTaskCount(long numTasks)
  {
    delegate.reportParallelMergeTaskCount(numTasks);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalCpuTime(long timeNs)
  {
    delegate.reportParallelMergeTotalCpuTime(timeNs);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalTime(long timeNs)
  {
    delegate.reportParallelMergeTotalTime(timeNs);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeFastestPartitionTime(long timeNs)
  {
    delegate.reportParallelMergeFastestPartitionTime(timeNs);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeSlowestPartitionTime(long timeNs)
  {
    delegate.reportParallelMergeSlowestPartitionTime(timeNs);
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportQueriedSegmentCount(long segmentCount)
  {
    delegate.reportQueriedSegmentCount(segmentCount);
    return this;
  }

  @Override
  public void emit(ServiceEmitter emitter)
  {
    checkModifiedFromOwnerThread();
    delegate.emit(emitter);
  }

  private QueryMetrics<QueryType> reportMetric(String metricName, Number value)
  {
    checkModifiedFromOwnerThread();
    metrics.put(metricName, value);
    return this;
  }

  private QueryMetrics<QueryType> reportMillisTimeMetric(String metricName, long timeNs)
  {
    return reportMetric(metricName, TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @JsonProperty
  public Map<String, Object> getDebugInfo()
  {
    return debugInfo;
  }

  @JsonProperty
  public Map<String, Number> getMetrics()
  {
    return metrics;
  }
}
