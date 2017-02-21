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
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.joda.time.Interval;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DefaultQueryMetrics<QueryType extends Query<?>> implements QueryMetrics<QueryType>
{
  protected final ObjectMapper jsonMapper;
  protected final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();

  public DefaultQueryMetrics(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
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
    builder.setDimension(DruidMetrics.DATASOURCE, DataSourceUtil.getMetricName(query.getDataSource()));
  }

  @Override
  public void queryType(QueryType query)
  {
    builder.setDimension(DruidMetrics.TYPE, query.getType());
  }

  @Override
  public void interval(QueryType query)
  {
    builder.setDimension(
        DruidMetrics.INTERVAL,
        Lists.transform(
            query.getIntervals(),
            new Function<Interval, String>()
            {
              @Override
              public String apply(Interval input)
              {
                return input.toString();
              }
            }
        ).toArray(new String[query.getIntervals().size()])
    );
  }

  @Override
  public void hasFilters(QueryType query)
  {
    builder.setDimension("hasFilters", String.valueOf(query.hasFilters()));
  }

  @Override
  public void duration(QueryType query)
  {
    builder.setDimension("duration", query.getDuration().toString());
  }

  @Override
  public void queryId(QueryType query)
  {
    builder.setDimension(DruidMetrics.ID, Strings.nullToEmpty(query.getId()));
  }

  @Override
  public void context(QueryType query)
  {
    try {
      builder.setDimension(
          "context",
          jsonMapper.writeValueAsString(
              query.getContext() == null
              ? ImmutableMap.of()
              : query.getContext()
          )
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void server(String host)
  {
    builder.setDimension("server", host);
  }

  @Override
  public void remoteAddress(String removeAddress)
  {
    builder.setDimension("removeAddress", removeAddress);
  }

  @Override
  public void userDimensions(Map<String, String> userDimensions)
  {
    for (Map.Entry<String, String> userDimension : userDimensions.entrySet()) {
      builder.setDimension(userDimension.getKey(), userDimension.getValue());
    }
  }

  @Override
  public void status(String status)
  {
    builder.setDimension(DruidMetrics.STATUS, status);
  }

  @Override
  public void success(boolean success)
  {
    builder.setDimension("success", String.valueOf(success));
  }

  @Override
  public void queryTime(ServiceEmitter emitter, long timeNs)
  {
    defaultTimeMetric(emitter, "query/time", timeNs);
  }

  @Override
  public void queryBytes(ServiceEmitter emitter, long byteCount)
  {
    emitter.emit(builder.build("query/bytes", byteCount));
  }

  @Override
  public void waitTime(ServiceEmitter emitter, long timeNs)
  {
    defaultTimeMetric(emitter, "query/wait/time", timeNs);
  }

  @Override
  public void segmentTime(ServiceEmitter emitter, long timeNs)
  {
    defaultTimeMetric(emitter, "query/segment/time", timeNs);
  }

  @Override
  public void segmentAndCacheTime(ServiceEmitter emitter, long timeNs)
  {
    defaultTimeMetric(emitter, "query/segmentAndCache/time", timeNs);
  }

  @Override
  public void intervalChunkTime(ServiceEmitter emitter, long timeNs)
  {
    defaultTimeMetric(emitter, "query/intervalChunk/time", timeNs);
  }

  @Override
  public void cpuTime(ServiceEmitter emitter, long timeNs)
  {
    emitter.emit(builder.build("query/cpu/time", TimeUnit.NANOSECONDS.toMicros(timeNs)));
  }

  @Override
  public void nodeTimeToFirstByte(ServiceEmitter emitter, long timeNs)
  {
    defaultTimeMetric(emitter, "query/node/ttfb", timeNs);
  }

  @Override
  public void nodeTime(ServiceEmitter emitter, long timeNs)
  {
    defaultTimeMetric(emitter, "query/node/time", timeNs);
  }

  private void defaultTimeMetric(ServiceEmitter emitter, String metricName, long timeNs)
  {
    emitter.emit(builder.build(metricName, TimeUnit.NANOSECONDS.toMillis(timeNs)));
  }

  @Override
  public void nodeBytes(ServiceEmitter emitter, long byteCount)
  {
    emitter.emit(builder.build("query/node/bytes", byteCount));
  }
}
