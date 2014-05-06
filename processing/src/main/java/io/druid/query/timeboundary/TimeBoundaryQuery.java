/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.timeboundary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class TimeBoundaryQuery extends BaseQuery<Result<TimeBoundaryResultValue>>
{
  public static final Interval MY_Y2K_INTERVAL = new Interval(
      new DateTime("0000-01-01"),
      new DateTime("3000-01-01")
  );
  public static final String MAX_TIME = "maxTime";
  public static final String MIN_TIME = "minTime";
  private static final byte CACHE_TYPE_ID = 0x0;

  @JsonCreator
  public TimeBoundaryQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource,
        (querySegmentSpec == null) ? new MultipleIntervalSegmentSpec(Arrays.asList(MY_Y2K_INTERVAL))
                                   : querySegmentSpec,
        context
    );
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public String getType()
  {
    return Query.TIME_BOUNDARY;
  }

  @Override
  public TimeBoundaryQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new TimeBoundaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        computeOverridenContext(contextOverrides)
    );
  }

  @Override
  public TimeBoundaryQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new TimeBoundaryQuery(
        getDataSource(),
        spec,
        getContext()
    );
  }

  public byte[] getCacheKey()
  {
    return ByteBuffer.allocate(1)
                     .put(CACHE_TYPE_ID)
                     .array();
  }

  @Override
  public String toString()
  {
    return "TimeBoundaryQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", duration=" + getDuration() +
           '}';
  }

  public Iterable<Result<TimeBoundaryResultValue>> buildResult(DateTime timestamp, DateTime min, DateTime max)
  {
    List<Result<TimeBoundaryResultValue>> results = Lists.newArrayList();
    Map<String, Object> result = Maps.newHashMap();

    if (min != null) {
      result.put(TimeBoundaryQuery.MIN_TIME, min);
    }
    if (max != null) {
      result.put(TimeBoundaryQuery.MAX_TIME, max);
    }
    if (!result.isEmpty()) {
      results.add(new Result<TimeBoundaryResultValue>(timestamp, new TimeBoundaryResultValue(result)));
    }

    return results;
  }

  public Iterable<Result<TimeBoundaryResultValue>> mergeResults(List<Result<TimeBoundaryResultValue>> results)
  {
    if (results == null || results.isEmpty()) {
      return Lists.newArrayList();
    }

    DateTime min = new DateTime(Long.MAX_VALUE);
    DateTime max = new DateTime(Long.MIN_VALUE);
    for (Result<TimeBoundaryResultValue> result : results) {
      TimeBoundaryResultValue val = result.getValue();

      DateTime currMinTime = val.getMinTime();
      if (currMinTime.isBefore(min)) {
        min = currMinTime;
      }
      DateTime currMaxTime = val.getMaxTime();
      if (currMaxTime.isAfter(max)) {
        max = currMaxTime;
      }
    }

    return Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            min,
            new TimeBoundaryResultValue(
                ImmutableMap.<String, Object>of(
                    TimeBoundaryQuery.MIN_TIME, min,
                    TimeBoundaryQuery.MAX_TIME, max
                )
            )
        )
    );
  }
}
