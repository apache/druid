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

package io.druid.java.util.emitter.service;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.guice.annotations.PublicApi;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.emitter.core.Event;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Map;

/**
 */
@PublicApi
public class ServiceMetricEvent implements Event
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final DateTime createdTime;
  private final ImmutableMap<String, String> serviceDims;
  private final Map<String, Object> userDims;
  private final String feed;
  private final String metric;
  private final Number value;

  private ServiceMetricEvent(
      DateTime createdTime,
      ImmutableMap<String, String> serviceDims,
      Map<String, Object> userDims,
      String feed,
      String metric,
      Number value
  )
  {
    this.createdTime = createdTime != null ? createdTime : DateTimes.nowUtc();
    this.serviceDims = serviceDims;
    this.userDims = userDims;
    this.feed = feed;
    this.metric = metric;
    this.value = value;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @Override
  public String getFeed()
  {
    return feed;
  }

  public String getService()
  {
    return serviceDims.get("service");
  }

  public String getHost()
  {
    return serviceDims.get("host");
  }

  public Map<String, Object> getUserDims()
  {
    return ImmutableMap.copyOf(userDims);
  }

  public String getMetric()
  {
    return metric;
  }

  public Number getValue()
  {
    return value;
  }

  @Override
  @JsonValue
  public Map<String, Object> toMap()
  {
    return ImmutableMap.<String, Object>builder()
                       .put("feed", getFeed())
                       .put("timestamp", createdTime.toString())
                       .putAll(serviceDims)
                       .put("metric", metric)
                       .put("value", value)
                       .putAll(
                           Maps.filterEntries(
                               userDims,
                               new Predicate<Map.Entry<String, Object>>()
                               {
                                 @Override
                                 public boolean apply(Map.Entry<String, Object> input)
                                 {
                                   return input.getKey() != null;
                                 }
                               }
                           )
                       )
                       .build();
  }

  public static class Builder
  {
    private final Map<String, Object> userDims = Maps.newTreeMap();
    private String feed = "metrics";

    public Builder setFeed(String feed)
    {
      this.feed = feed;
      return this;
    }

    public Builder setDimension(String dim, String[] values)
    {
      userDims.put(dim, Arrays.asList(values));
      return this;
    }

    public Builder setDimension(String dim, String value)
    {
      userDims.put(dim, value);
      return this;
    }

    public Object getDimension(String dim)
    {
      return userDims.get(dim);
    }

    public ServiceEventBuilder<ServiceMetricEvent> build(
        final String metric,
        final Number value
    )
    {
      return build(null, metric, value);
    }

    public ServiceEventBuilder<ServiceMetricEvent> build(
        final DateTime createdTime,
        final String metric,
        final Number value
    )
    {
      if (Double.isNaN(value.doubleValue())) {
        throw new ISE("Value of NaN is not allowed!");
      }
      if (Double.isInfinite(value.doubleValue())) {
        throw new ISE("Value of Infinite is not allowed!");
      }

      return new ServiceEventBuilder<ServiceMetricEvent>()
      {
        @Override
        public ServiceMetricEvent build(ImmutableMap<String, String> serviceDimensions)
        {
          return new ServiceMetricEvent(
              createdTime,
              serviceDimensions,
              userDims,
              feed,
              metric,
              value
          );
        }
      };
    }
  }
}
