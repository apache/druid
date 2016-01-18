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
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.aggregation.AggregatorFactory;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class DruidMetrics
{
  public final static String DATASOURCE = "dataSource";
  public final static String TYPE = "type";
  public final static String INTERVAL = "interval";
  public final static String ID = "id";
  public final static String TASK_ID = "taskId";
  public final static String STATUS = "status";
  public final static String VERSION = "version";

  // task metrics
  public final static String TASK_TYPE = "taskType";
  public final static String TASK_STATUS = "taskStatus";

  public final static String SERVER = "server";

  public static int findNumComplexAggs(List<AggregatorFactory> aggs)
  {
    int retVal = 0;
    for (AggregatorFactory agg : aggs) {
      // This needs to change when we have support column types better
      if (!agg.getTypeName().equals("float") && !agg.getTypeName().equals("long")) {
        retVal++;
      }
    }
    return retVal;
  }

  public static <T> ServiceMetricEvent.Builder makePartialQueryTimeMetric(Query<T> query)
  {
    return new ServiceMetricEvent.Builder()
        .setDimension(VERSION, Strings.nullToEmpty(DruidMetrics.class.getPackage().getImplementationVersion()))
        .setDimension(DATASOURCE, DataSourceUtil.getMetricName(query.getDataSource()))
        .setDimension(TYPE, query.getType())
        .setDimension(
            INTERVAL,
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
        )
        .setDimension("hasFilters", String.valueOf(query.hasFilters()))
        .setDimension("duration", query.getDuration().toString());
  }

  public static <T> ServiceMetricEvent.Builder makeQueryTimeMetric(
      final ObjectMapper jsonMapper, final Query<T> query, final String remoteAddr
  ) throws JsonProcessingException
  {
    return makePartialQueryTimeMetric(query)
        .setDimension(
            "context",
            jsonMapper.writeValueAsString(
                query.getContext() == null
                ? ImmutableMap.of()
                : query.getContext()
            )
        )
        .setDimension("remoteAddress", remoteAddr)
        .setDimension(ID, query.getId());
  }
}
