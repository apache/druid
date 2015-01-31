/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.joda.time.Interval;

/**
 */
public class QueryMetricUtil
{
  public static <T> ServiceMetricEvent.Builder makeQueryTimeMetric(Query<T> query)
  {
    return new ServiceMetricEvent.Builder()
        .setUser2(DataSourceUtil.getMetricName(query.getDataSource()))
        .setUser4(query.getType())
        .setUser5(
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
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser9(query.getDuration().toPeriod().toStandardMinutes().toString());
  }

  public static <T> ServiceMetricEvent.Builder makeRequestTimeMetric(
      final ObjectMapper jsonMapper, final Query<T> query, final String remoteAddr
  ) throws JsonProcessingException
  {
    return makeQueryTimeMetric(query)
        .setUser3(
            jsonMapper.writeValueAsString(
                query.getContext() == null
                ? ImmutableMap.of()
                : query.getContext()
            )
        )
        .setUser7(remoteAddr)
        .setUser8(query.getId());
  }
}
