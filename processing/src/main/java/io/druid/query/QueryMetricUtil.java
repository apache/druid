/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
