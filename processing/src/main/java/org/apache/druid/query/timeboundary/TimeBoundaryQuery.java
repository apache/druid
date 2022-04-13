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

package org.apache.druid.query.timeboundary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class TimeBoundaryQuery extends BaseQuery<Result<TimeBoundaryResultValue>>
{
  private static final QuerySegmentSpec DEFAULT_SEGMENT_SPEC = new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY);
  public static final String MAX_TIME = "maxTime";
  public static final String MIN_TIME = "minTime";

  private static final byte CACHE_TYPE_ID = 0x0;

  private final DimFilter dimFilter;
  private final String bound;

  @JsonCreator
  public TimeBoundaryQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("bound") String bound,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec == null ? DEFAULT_SEGMENT_SPEC : querySegmentSpec, false, context);

    this.dimFilter = dimFilter;
    this.bound = bound == null ? "" : bound;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @JsonProperty("filter")
  @Override
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return Query.TIME_BOUNDARY;
  }

  @JsonProperty
  public String getBound()
  {
    return bound;
  }

  @Override
  public TimeBoundaryQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    Map<String, Object> newContext = computeOverriddenContext(getContext(), contextOverrides);
    return Druids.TimeBoundaryQueryBuilder.copy(this).context(newContext).build();
  }

  @Override
  public TimeBoundaryQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return Druids.TimeBoundaryQueryBuilder.copy(this).intervals(spec).build();
  }

  @Override
  public Query<Result<TimeBoundaryResultValue>> withDataSource(DataSource dataSource)
  {
    return Druids.TimeBoundaryQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  public byte[] getCacheKey()
  {
    final byte[] filterBytes = dimFilter == null ? ByteArrays.EMPTY_ARRAY : dimFilter.getCacheKey();
    final byte[] boundBytes = StringUtils.toUtf8(bound);
    final byte delimiter = (byte) 0xff;
    return ByteBuffer.allocate(2 + boundBytes.length + filterBytes.length)
        .put(CACHE_TYPE_ID)
        .put(boundBytes)
        .put(delimiter)
        .put(filterBytes)
        .array();
  }

  public Iterable<Result<TimeBoundaryResultValue>> buildResult(DateTime timestamp, DateTime min, DateTime max)
  {
    List<Result<TimeBoundaryResultValue>> results = new ArrayList<>();
    Map<String, Object> result = new HashMap<>();

    if (min != null) {
      result.put(MIN_TIME, min);
    }
    if (max != null) {
      result.put(MAX_TIME, max);
    }
    if (!result.isEmpty()) {
      results.add(new Result<>(timestamp, new TimeBoundaryResultValue(result)));
    }

    return results;
  }

  public Iterable<Result<TimeBoundaryResultValue>> mergeResults(List<Result<TimeBoundaryResultValue>> results)
  {
    if (results == null || results.isEmpty()) {
      return new ArrayList<>();
    }

    DateTime min = DateTimes.MAX;
    DateTime max = DateTimes.MIN;
    for (Result<TimeBoundaryResultValue> result : results) {
      TimeBoundaryResultValue val = result.getValue();

      DateTime currMinTime = val.getMinTime();
      if (currMinTime != null && currMinTime.isBefore(min)) {
        min = currMinTime;
      }
      DateTime currMaxTime = val.getMaxTime();
      if (currMaxTime != null && currMaxTime.isAfter(max)) {
        max = currMaxTime;
      }
    }

    final DateTime ts;
    final DateTime minTime;
    final DateTime maxTime;

    if (isMinTime()) {
      ts = min;
      minTime = min;
      maxTime = null;
    } else if (isMaxTime()) {
      ts = max;
      minTime = null;
      maxTime = max;
    } else {
      ts = min;
      minTime = min;
      maxTime = max;
    }

    return buildResult(ts, minTime, maxTime);
  }

  boolean isMinTime()
  {
    return bound.equalsIgnoreCase(MIN_TIME);
  }

  boolean isMaxTime()
  {
    return bound.equalsIgnoreCase(MAX_TIME);
  }

  @Override
  public String toString()
  {
    return "TimeBoundaryQuery{" +
        "dataSource='" + getDataSource() + '\'' +
        ", querySegmentSpec=" + getQuerySegmentSpec() +
        ", duration=" + getDuration() +
        ", bound=" + bound +
        ", dimFilter=" + dimFilter +
        '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TimeBoundaryQuery that = (TimeBoundaryQuery) o;

    if (!bound.equals(that.bound)) {
      return false;
    }

    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + bound.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    return result;
  }
}
