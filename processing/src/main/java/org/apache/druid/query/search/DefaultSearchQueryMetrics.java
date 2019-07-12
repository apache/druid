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

package org.apache.druid.query.search;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.joda.time.Interval;

import java.util.List;

/**
 * This class is implemented with delegation to another QueryMetrics for compatibility, see "Making subinterfaces of
 * QueryMetrics for emitting custom dimensions and/or metrics for specific query types" section in {@link QueryMetrics}
 * javadoc.
 */
public class DefaultSearchQueryMetrics implements SearchQueryMetrics
{
  private QueryMetrics<Query<?>> delegateQueryMetrics;


  // queryMetrics.query(query) must already be called on the provided queryMetrics.
  public DefaultSearchQueryMetrics(QueryMetrics<Query<?>> queryMetrics)
  {
    this.delegateQueryMetrics = queryMetrics;
  }

  @Override
  public void query(SearchQuery query)
  {
    //delegateQueryMetrics.query(query) must already be called on the provided queryMetrics.
  }

  @Override
  public void dataSource(SearchQuery query)
  {
    throw new ISE("Unsupported method in default query metrics implementation.");
  }

  @Override
  public void queryType(SearchQuery query)
  {
    throw new ISE("Unsupported method in default query metrics implementation.");
  }

  @Override
  public void interval(SearchQuery query)
  {
    throw new ISE("Unsupported method in default query metrics implementation.");
  }

  @Override
  public void hasFilters(SearchQuery query)
  {
    throw new ISE("Unsupported method in default query metrics implementation.");
  }

  @Override
  public void duration(SearchQuery query)
  {
    throw new ISE("Unsupported method in default query metrics implementation.");
  }

  @Override
  public void queryId(SearchQuery query)
  {
    throw new ISE("Unsupported method in default query metrics implementation.");
  }

  @Override
  public void sqlQueryId(SearchQuery query)
  {
    throw new ISE("Unsupported method in default query metrics implementation.");
  }

  @Override
  public void granularity(SearchQuery query)
  {
    // Don't emit by default
  }

  @Override
  public void context(SearchQuery query)
  {
    delegateQueryMetrics.context(query);
  }

  @Override
  public void server(String host)
  {
    delegateQueryMetrics.server(host);
  }

  @Override
  public void remoteAddress(String remoteAddress)
  {
    delegateQueryMetrics.remoteAddress(remoteAddress);
  }

  @Override
  public void status(String status)
  {
    delegateQueryMetrics.status(status);
  }

  @Override
  public void success(boolean success)
  {
    delegateQueryMetrics.success(success);
  }

  @Override
  public void segment(String segmentIdentifier)
  {
    delegateQueryMetrics.segment(segmentIdentifier);
  }

  @Override
  public void chunkInterval(Interval interval)
  {
    delegateQueryMetrics.chunkInterval(interval);
  }

  @Override
  public void preFilters(List<Filter> preFilters)
  {
    delegateQueryMetrics.preFilters(preFilters);
  }

  @Override
  public void postFilters(List<Filter> postFilters)
  {
    delegateQueryMetrics.postFilters(postFilters);
  }

  @Override
  public void identity(String identity)
  {
    delegateQueryMetrics.identity(identity);
  }

  @Override
  public void vectorized(final boolean vectorized)
  {
    delegateQueryMetrics.vectorized(vectorized);
  }

  @Override
  public BitmapResultFactory<?> makeBitmapResultFactory(BitmapFactory factory)
  {
    return delegateQueryMetrics.makeBitmapResultFactory(factory);
  }

  @Override
  public QueryMetrics reportQueryTime(long timeNs)
  {
    return delegateQueryMetrics.reportQueryTime(timeNs);
  }

  @Override
  public QueryMetrics reportQueryBytes(long byteCount)
  {
    return delegateQueryMetrics.reportQueryBytes(byteCount);
  }

  @Override
  public QueryMetrics reportWaitTime(long timeNs)
  {
    return delegateQueryMetrics.reportWaitTime(timeNs);
  }

  @Override
  public QueryMetrics reportSegmentTime(long timeNs)
  {
    return delegateQueryMetrics.reportSegmentTime(timeNs);
  }

  @Override
  public QueryMetrics reportSegmentAndCacheTime(long timeNs)
  {
    return delegateQueryMetrics.reportSegmentAndCacheTime(timeNs);
  }

  @Override
  public QueryMetrics reportIntervalChunkTime(long timeNs)
  {
    return delegateQueryMetrics.reportIntervalChunkTime(timeNs);
  }

  @Override
  public QueryMetrics reportCpuTime(long timeNs)
  {
    return delegateQueryMetrics.reportCpuTime(timeNs);
  }

  @Override
  public QueryMetrics reportNodeTimeToFirstByte(long timeNs)
  {
    return delegateQueryMetrics.reportNodeTimeToFirstByte(timeNs);
  }

  @Override
  public QueryMetrics reportBackPressureTime(long timeNs)
  {
    return delegateQueryMetrics.reportBackPressureTime(timeNs);
  }

  @Override
  public QueryMetrics reportNodeTime(long timeNs)
  {
    return delegateQueryMetrics.reportNodeTime(timeNs);
  }

  @Override
  public QueryMetrics reportNodeBytes(long byteCount)
  {
    return delegateQueryMetrics.reportNodeBytes(byteCount);
  }

  @Override
  public QueryMetrics reportBitmapConstructionTime(long timeNs)
  {
    return delegateQueryMetrics.reportBitmapConstructionTime(timeNs);
  }

  @Override
  public QueryMetrics reportSegmentRows(long numRows)
  {
    return delegateQueryMetrics.reportSegmentRows(numRows);
  }

  @Override
  public QueryMetrics reportPreFilteredRows(long numRows)
  {
    return delegateQueryMetrics.reportPreFilteredRows(numRows);
  }

  @Override
  public void emit(ServiceEmitter emitter)
  {
    delegateQueryMetrics.emit(emitter);
  }
}
