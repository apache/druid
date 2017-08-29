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

package io.druid.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.query.filter.Filter;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * DefaultQueryMetrics is unsafe for use from multiple threads. It fails with RuntimeException on access not from the
 * thread where it was constructed. To "transfer" DefaultQueryMetrics from one thread to another {@link #ownerThread}
 * field should be updated.
 */
public class DefaultQueryMetrics<QueryType extends Query<?>> implements QueryMetrics<QueryType>
{
  protected final ObjectMapper jsonMapper;
  protected final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
  protected final Map<String, Number> metrics = new HashMap<>();

  /** Non final to give subclasses ability to reassign it. */
  protected Thread ownerThread = Thread.currentThread();

  public DefaultQueryMetrics(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  protected void checkModifiedFromOwnerThread()
  {
    if (Thread.currentThread() != ownerThread) {
      throw new IllegalStateException(
          "DefaultQueryMetrics must not be modified from multiple threads. If it is needed to gather dimension or "
          + "metric information from multiple threads or from an async thread, this information should explicitly be "
          + "passed between threads (e. g. using Futures), or this DefaultQueryMetrics's ownerThread should be "
          + "reassigned explicitly");
    }
  }

  protected void setDimension(String dimension, String value)
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
  }

  @Override
  public void dataSource(QueryType query)
  {
    setDimension(DruidMetrics.DATASOURCE, DataSourceUtil.getMetricName(query.getDataSource()));
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
        query.getIntervals().stream().map(Interval::toString).toArray(String[]::new)
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
    setDimension(DruidMetrics.ID, Strings.nullToEmpty(query.getId()));
  }

  @Override
  public void context(QueryType query)
  {
    try {
      setDimension(
          "context",
          jsonMapper.writeValueAsString(query.getContext() == null ? ImmutableMap.of() : query.getContext())
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
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
  public void segment(String segmentIdentifier)
  {
    setDimension("segment", segmentIdentifier);
  }

  @Override
  public void chunkInterval(Interval interval)
  {
    setDimension("chunkInterval", interval.toString());
  }

  @Override
  public void preFilters(List<Filter> preFilters)
  {
    // Emit nothing by default.
  }

  @Override
  public void postFilters(List<Filter> postFilters)
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
    return reportMillisTimeMetric("query/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportQueryBytes(long byteCount)
  {
    return reportMetric("query/bytes", byteCount);
  }

  @Override
  public QueryMetrics<QueryType> reportWaitTime(long timeNs)
  {
    return reportMillisTimeMetric("query/wait/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentTime(long timeNs)
  {
    return reportMillisTimeMetric("query/segment/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentAndCacheTime(long timeNs)
  {
    return reportMillisTimeMetric("query/segmentAndCache/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportIntervalChunkTime(long timeNs)
  {
    return reportMillisTimeMetric("query/intervalChunk/time", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportCpuTime(long timeNs)
  {
    return reportMetric("query/cpu/time", TimeUnit.NANOSECONDS.toMicros(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTimeToFirstByte(long timeNs)
  {
    return reportMillisTimeMetric("query/node/ttfb", timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTime(long timeNs)
  {
    return reportMillisTimeMetric("query/node/time", timeNs);
  }

  private QueryMetrics<QueryType> reportMillisTimeMetric(String metricName, long timeNs)
  {
    return reportMetric(metricName, TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  protected QueryMetrics<QueryType> reportMetric(String metricName, Number value)
  {
    checkModifiedFromOwnerThread();
    metrics.put(metricName, value);
    return this;
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
  public void emit(ServiceEmitter emitter)
  {
    checkModifiedFromOwnerThread();
    for (Map.Entry<String, Number> metric : metrics.entrySet()) {
      emitter.emit(builder.build(metric.getKey(), metric.getValue()));
    }
    metrics.clear();
  }
}
