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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.timeline.LogicalSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class TimeBoundaryQueryQueryToolChestTest
{

  private static final TimeBoundaryQuery TIME_BOUNDARY_QUERY = new TimeBoundaryQuery(
      new TableDataSource("test"),
      null,
      null,
      null,
      null
  );

  private static final TimeBoundaryQuery MAXTIME_BOUNDARY_QUERY = new TimeBoundaryQuery(
      new TableDataSource("test"),
      null,
      TimeBoundaryQuery.MAX_TIME,
      null,
      null
  );

  private static final TimeBoundaryQuery MINTIME_BOUNDARY_QUERY = new TimeBoundaryQuery(
      new TableDataSource("test"),
      null,
      TimeBoundaryQuery.MIN_TIME,
      null,
      null
  );

  private static final TimeBoundaryQuery FILTERED_BOUNDARY_QUERY = Druids.newTimeBoundaryQueryBuilder()
                                                                         .dataSource("testing")
                                                                         .filters("foo", "bar")
                                                                         .build();

  private static LogicalSegment createLogicalSegment(final Interval interval)
  {
    return createLogicalSegment(interval, interval);
  }

  private static LogicalSegment createLogicalSegment(final Interval interval, final Interval trueInterval)
  {
    return new LogicalSegment()
    {
      @Override
      public Interval getInterval()
      {
        return interval;
      }

      @Override
      public Interval getTrueInterval()
      {
        return trueInterval;
      }
    };
  }

  @Test
  public void testFilterSegments()
  {
    List<LogicalSegment> segments = new TimeBoundaryQueryQueryToolChest().filterSegments(
        TIME_BOUNDARY_QUERY,
        Arrays.asList(
            createLogicalSegment(Intervals.of("2013-01-01/P1D")),
            createLogicalSegment(Intervals.of("2013-01-01T01/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-01T02/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-02/P1D")),
            createLogicalSegment(Intervals.of("2013-01-03T01/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-03T02/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-03/P1D"))
        )
    );

    Assert.assertEquals(6, segments.size());

    List<LogicalSegment> expected = Arrays.asList(
        createLogicalSegment(Intervals.of("2013-01-01/P1D")),
        createLogicalSegment(Intervals.of("2013-01-01T01/PT1H")),
        createLogicalSegment(Intervals.of("2013-01-01T02/PT1H")),
        createLogicalSegment(Intervals.of("2013-01-03T01/PT1H")),
        createLogicalSegment(Intervals.of("2013-01-03T02/PT1H")),
        createLogicalSegment(Intervals.of("2013-01-03/P1D"))
    );

    for (int i = 0; i < segments.size(); i++) {
      Assert.assertEquals(segments.get(i).getInterval(), expected.get(i).getInterval());
    }
  }

  @Test
  public void testFilterOverlapingSegments()
  {
    final List<LogicalSegment> actual = new TimeBoundaryQueryQueryToolChest().filterSegments(
        TIME_BOUNDARY_QUERY,
        Arrays.asList(
            createLogicalSegment(Intervals.of("2015/2016-08-01")),
            createLogicalSegment(Intervals.of("2016-08-01/2017")),
            createLogicalSegment(Intervals.of("2017/2017-08-01"), Intervals.of("2017/2018")),
            createLogicalSegment(Intervals.of("2017-08-01/2017-08-02")),
            createLogicalSegment(Intervals.of("2017-08-02/2018"), Intervals.of("2017/2018"))
        )
    );

    final List<LogicalSegment> expected = Arrays.asList(
        createLogicalSegment(Intervals.of("2015/2016-08-01")),
        createLogicalSegment(Intervals.of("2017/2017-08-01"), Intervals.of("2017/2018")),
        createLogicalSegment(Intervals.of("2017-08-01/2017-08-02")),
        createLogicalSegment(Intervals.of("2017-08-02/2018"), Intervals.of("2017/2018"))
    );

    Assert.assertEquals(expected.size(), actual.size());

    for (int i = 0; i < actual.size(); i++) {
      Assert.assertEquals(expected.get(i).getInterval(), actual.get(i).getInterval());
      Assert.assertEquals(expected.get(i).getTrueInterval(), actual.get(i).getTrueInterval());
    }
  }

  @Test
  public void testMaxTimeFilterSegments()
  {
    List<LogicalSegment> segments = new TimeBoundaryQueryQueryToolChest().filterSegments(
        MAXTIME_BOUNDARY_QUERY,
        Arrays.asList(
            createLogicalSegment(Intervals.of("2013-01-01/P1D")),
            createLogicalSegment(Intervals.of("2013-01-01T01/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-01T02/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-02/P1D")),
            createLogicalSegment(Intervals.of("2013-01-03T01/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-03T02/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-03/P1D"))
        )
    );

    Assert.assertEquals(3, segments.size());

    List<LogicalSegment> expected = Arrays.asList(
        createLogicalSegment(Intervals.of("2013-01-03T01/PT1H")),
        createLogicalSegment(Intervals.of("2013-01-03T02/PT1H")),
        createLogicalSegment(Intervals.of("2013-01-03/P1D"))
    );

    for (int i = 0; i < segments.size(); i++) {
      Assert.assertEquals(segments.get(i).getInterval(), expected.get(i).getInterval());
    }
  }

  @Test
  public void testMaxTimeFilterOverlapingSegments()
  {
    final List<LogicalSegment> actual = new TimeBoundaryQueryQueryToolChest().filterSegments(
        MAXTIME_BOUNDARY_QUERY,
        Arrays.asList(
            createLogicalSegment(Intervals.of("2015/2016-08-01")),
            createLogicalSegment(Intervals.of("2016-08-01/2017")),
            createLogicalSegment(Intervals.of("2017/2017-08-01"), Intervals.of("2017/2018")),
            createLogicalSegment(Intervals.of("2017-08-01/2017-08-02")),
            createLogicalSegment(Intervals.of("2017-08-02/2018"), Intervals.of("2017/2018"))
        )
    );

    final List<LogicalSegment> expected = Arrays.asList(
        createLogicalSegment(Intervals.of("2017/2017-08-01"), Intervals.of("2017/2018")),
        createLogicalSegment(Intervals.of("2017-08-01/2017-08-02")),
        createLogicalSegment(Intervals.of("2017-08-02/2018"), Intervals.of("2017/2018"))
    );

    Assert.assertEquals(expected.size(), actual.size());

    for (int i = 0; i < actual.size(); i++) {
      Assert.assertEquals(expected.get(i).getInterval(), actual.get(i).getInterval());
      Assert.assertEquals(expected.get(i).getTrueInterval(), actual.get(i).getTrueInterval());
    }
  }

  @Test
  public void testMinTimeFilterOverlapingSegments()
  {
    final List<LogicalSegment> actual = new TimeBoundaryQueryQueryToolChest().filterSegments(
        MINTIME_BOUNDARY_QUERY,
        Arrays.asList(
            createLogicalSegment(Intervals.of("2017/2017-08-01"), Intervals.of("2017/2018")),
            createLogicalSegment(Intervals.of("2017-08-01/2017-08-02")),
            createLogicalSegment(Intervals.of("2017-08-02/2018"), Intervals.of("2017/2018")),
            createLogicalSegment(Intervals.of("2018/2018-08-01")),
            createLogicalSegment(Intervals.of("2018-08-01/2019"))
        )
    );

    final List<LogicalSegment> expected = Arrays.asList(
        createLogicalSegment(Intervals.of("2017/2017-08-01"), Intervals.of("2017/2018")),
        createLogicalSegment(Intervals.of("2017-08-01/2017-08-02")),
        createLogicalSegment(Intervals.of("2017-08-02/2018"), Intervals.of("2017/2018"))
    );

    Assert.assertEquals(expected.size(), actual.size());

    for (int i = 0; i < actual.size(); i++) {
      Assert.assertEquals(expected.get(i).getInterval(), actual.get(i).getInterval());
      Assert.assertEquals(expected.get(i).getTrueInterval(), actual.get(i).getTrueInterval());
    }
  }

  @Test
  public void testMinTimeFilterSegments()
  {
    List<LogicalSegment> segments = new TimeBoundaryQueryQueryToolChest().filterSegments(
        MINTIME_BOUNDARY_QUERY,
        Arrays.asList(
            createLogicalSegment(Intervals.of("2013-01-01/P1D")),
            createLogicalSegment(Intervals.of("2013-01-01T01/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-01T02/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-02/P1D")),
            createLogicalSegment(Intervals.of("2013-01-03T01/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-03T02/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-03/P1D"))
        )
    );

    Assert.assertEquals(3, segments.size());

    List<LogicalSegment> expected = Arrays.asList(
        createLogicalSegment(Intervals.of("2013-01-01/P1D")),
        createLogicalSegment(Intervals.of("2013-01-01T01/PT1H")),
        createLogicalSegment(Intervals.of("2013-01-01T02/PT1H"))
    );

    for (int i = 0; i < segments.size(); i++) {
      Assert.assertEquals(segments.get(i).getInterval(), expected.get(i).getInterval());
    }
  }

  @Test
  public void testFilteredFilterSegments()
  {
    List<LogicalSegment> segments = new TimeBoundaryQueryQueryToolChest().filterSegments(
        FILTERED_BOUNDARY_QUERY,
        Arrays.asList(
            createLogicalSegment(Intervals.of("2013-01-01/P1D")),
            createLogicalSegment(Intervals.of("2013-01-01T01/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-01T02/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-02/P1D")),
            createLogicalSegment(Intervals.of("2013-01-03T01/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-03T02/PT1H")),
            createLogicalSegment(Intervals.of("2013-01-03/P1D"))
        )
    );

    Assert.assertEquals(7, segments.size());
  }

  @Test
  public void testCacheStrategy() throws Exception
  {
    CacheStrategy<Result<TimeBoundaryResultValue>, Object, TimeBoundaryQuery> strategy =
        new TimeBoundaryQueryQueryToolChest().getCacheStrategy(
            new TimeBoundaryQuery(
                new TableDataSource("dummy"),
                new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                null,
                null,
                null
            )
        );

    final Result<TimeBoundaryResultValue> result = new Result<>(
        DateTimes.utc(123L), new TimeBoundaryResultValue(
        ImmutableMap.of(
            TimeBoundaryQuery.MIN_TIME, DateTimes.EPOCH.toString(),
            TimeBoundaryQuery.MAX_TIME, DateTimes.of("2015-01-01").toString()
        )
    )
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(
        result
    );

    ObjectMapper objectMapper = new DefaultObjectMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TimeBoundaryResultValue> fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }
}
