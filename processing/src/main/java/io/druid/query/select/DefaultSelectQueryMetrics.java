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

package io.druid.query.select;

import com.metamx.emitter.service.ServiceEmitter;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.query.BitmapResultFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.filter.Filter;
import org.joda.time.Interval;

import java.util.List;

public class DefaultSelectQueryMetrics implements SelectQueryMetrics
{
  private QueryMetrics<Query<?>> queryMetrics;

  public DefaultSelectQueryMetrics(QueryMetrics<Query<?>> queryMetrics)
  {
    this.queryMetrics = queryMetrics;
  }

  @Override
  public void query(SelectQuery query)
  {
    dataSource(query);
    queryType(query);
    interval(query);
    hasFilters(query);
    duration(query);
    queryId(query);
    granularity(query);
  }

  @Override
  public void dataSource(SelectQuery query)
  {
    queryMetrics.dataSource(query);
  }

  @Override
  public void queryType(SelectQuery query)
  {
    queryMetrics.queryType(query);
  }

  @Override
  public void interval(SelectQuery query)
  {
    queryMetrics.interval(query);
  }

  @Override
  public void hasFilters(SelectQuery query)
  {
    queryMetrics.hasFilters(query);
  }

  @Override
  public void duration(SelectQuery query)
  {
    queryMetrics.duration(query);
  }

  @Override
  public void queryId(SelectQuery query)
  {
    queryMetrics.queryId(query);
  }

  @Override
  public void context(SelectQuery query)
  {
    queryMetrics.context(query);
  }

  @Override
  public void server(String host)
  {
    queryMetrics.server(host);
  }

  @Override
  public void remoteAddress(String remoteAddress)
  {
    queryMetrics.remoteAddress(remoteAddress);
  }

  @Override
  public void status(String status)
  {
    queryMetrics.status(status);
  }

  @Override
  public void success(boolean success)
  {
    queryMetrics.success(success);
  }

  @Override
  public void segment(String segmentIdentifier)
  {
    queryMetrics.segment(segmentIdentifier);
  }

  @Override
  public void chunkInterval(Interval interval)
  {
    queryMetrics.chunkInterval(interval);
  }

  @Override
  public void preFilters(List<Filter> preFilters)
  {
    queryMetrics.preFilters(preFilters);
  }

  @Override
  public void postFilters(List<Filter> postFilters)
  {
    queryMetrics.postFilters(postFilters);
  }

  @Override
  public BitmapResultFactory<?> makeBitmapResultFactory(BitmapFactory factory)
  {
    return queryMetrics.makeBitmapResultFactory(factory);
  }

  @Override
  public QueryMetrics reportQueryTime(long timeNs)
  {
    return queryMetrics.reportQueryTime(timeNs);
  }

  @Override
  public QueryMetrics reportQueryBytes(long byteCount)
  {
    return queryMetrics.reportQueryBytes(byteCount);
  }

  @Override
  public QueryMetrics reportWaitTime(long timeNs)
  {
    return queryMetrics.reportWaitTime(timeNs);
  }

  @Override
  public QueryMetrics reportSegmentTime(long timeNs)
  {
    return queryMetrics.reportSegmentTime(timeNs);
  }

  @Override
  public QueryMetrics reportSegmentAndCacheTime(long timeNs)
  {
    return queryMetrics.reportSegmentAndCacheTime(timeNs);
  }

  @Override
  public QueryMetrics reportIntervalChunkTime(long timeNs)
  {
    return queryMetrics.reportIntervalChunkTime(timeNs);
  }

  @Override
  public QueryMetrics reportCpuTime(long timeNs)
  {
    return queryMetrics.reportCpuTime(timeNs);
  }

  @Override
  public QueryMetrics reportNodeTimeToFirstByte(long timeNs)
  {
    return queryMetrics.reportNodeTimeToFirstByte(timeNs);
  }

  @Override
  public QueryMetrics reportNodeTime(long timeNs)
  {
    return queryMetrics.reportNodeTime(timeNs);
  }

  @Override
  public QueryMetrics reportNodeBytes(long byteCount)
  {
    return queryMetrics.reportNodeBytes(byteCount);
  }

  @Override
  public QueryMetrics reportBitmapConstructionTime(long timeNs)
  {
    return queryMetrics.reportBitmapConstructionTime(timeNs);
  }

  @Override
  public QueryMetrics reportSegmentRows(long numRows)
  {
    return queryMetrics.reportSegmentRows(numRows);
  }

  @Override
  public QueryMetrics reportPreFilteredRows(long numRows)
  {
    return queryMetrics.reportPreFilteredRows(numRows);
  }

  @Override
  public void emit(ServiceEmitter emitter)
  {
    queryMetrics.emit(emitter);
  }

  @Override
  public void granularity(SelectQuery query)
  {
    // Don't emit by default
  }

}
