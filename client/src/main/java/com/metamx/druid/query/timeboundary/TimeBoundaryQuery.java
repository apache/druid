package com.metamx.druid.query.timeboundary;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.druid.BaseQuery;
import com.metamx.druid.Query;
import com.metamx.druid.query.segment.MultipleIntervalSegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeBoundaryResultValue;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
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
  public static final Interval MY_Y2K_INTERVAL = new Interval(new DateTime(0), new DateTime("3000-01-01"));
  public static final String MAX_TIME = "maxTime";
  public static final String MIN_TIME = "minTime";
  private static final byte CACHE_TYPE_ID = 0x0;

  @JsonCreator
  public TimeBoundaryQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, String> context
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
  public TimeBoundaryQuery withOverriddenContext(Map<String, String> contextOverrides)
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
