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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.filter.Filter;
import org.joda.time.Interval;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A basic thread-safe implementation of the {@link QueryMetrics} interface.
 *
 * <p><b>Note for inheritence:</b> Subclass should override {@link #makeCopy()} method to return
 * an instance of that subclass. See {@link org.apache.druid.query.groupby.DefaultGroupByQueryMetrics}
 * for an example.
 */
public class DefaultQueryMetrics<QueryType extends Query<?>> implements QueryMetrics<QueryType>
{
  protected final ObjectMapper jsonMapper;

  protected final Object lock = new Object();
  @GuardedBy("lock") private final Map<String, String> singleValueDims = new HashMap<>();
  @GuardedBy("lock") private final Map<String, String[]> multiValueDims = new HashMap<>();
  @GuardedBy("lock") private final Map<String, Number> metrics = new HashMap<>();

  public DefaultQueryMetrics(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  // copy constructor, used by makeCopy()
  public DefaultQueryMetrics(DefaultQueryMetrics that)
  {
    this.jsonMapper = that.jsonMapper;
    this.singleValueDims.putAll(that.singleValueDims);
    this.multiValueDims.putAll(that.multiValueDims);
    this.metrics.putAll(that.metrics);
  }

  protected void setDimension(String dimension, String value)
  {
    synchronized (lock) {
      singleValueDims.put(dimension, value);
    }
  }

  protected void setDimensions(String dimension, String[] values)
  {
    synchronized (lock) {
      multiValueDims.put(dimension, values);
    }
  }

  protected QueryMetrics<QueryType> reportMetric(String metricName, Number value)
  {
    synchronized (lock) {
      metrics.put(metricName, value);
      return this;
    }
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
    setDimensions(
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
    setDimension(DruidMetrics.ID, StringUtils.nullToEmptyNonDruidDataString(query.getId()));
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
  public void identity(String identity)
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
  public void emit(final ServiceEmitter emitter)
  {
    synchronized (lock) {
      ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
      singleValueDims.forEach(builder::setDimension);
      multiValueDims.forEach(builder::setDimension);
      metrics.forEach((metric, value) -> emitter.emit(builder.build(metric, value)));
      metrics.clear();
    }
  }

  /**
   * Subclasses should override this method and return an instance of that class.
   * See {@link org.apache.druid.query.groupby.GroupByQueryMetrics} for an example.
   */
  @Override
  public QueryMetrics<QueryType> makeCopy()
  {
    synchronized (lock) {
      return new DefaultQueryMetrics<>(this);
    }
  }
}
