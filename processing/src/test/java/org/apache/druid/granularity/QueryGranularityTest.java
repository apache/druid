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

package org.apache.druid.granularity;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Period;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

/**
 *
 */
public class QueryGranularityTest extends InitializedNullHandlingTest
{
  @Test
  public void testIterableNone()
  {
    final Iterator<Interval> iterator = Granularities.NONE.getIterable(Intervals.utc(0, 1000)).iterator();
    int count = 0;
    while (iterator.hasNext()) {
      Assertions.assertEquals(count, iterator.next().getStartMillis());
      count++;
    }
  }

  @Test
  public void testIterableMinuteSimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T09:38:00.000Z"),
            DateTimes.of("2011-01-01T09:39:00.000Z"),
            DateTimes.of("2011-01-01T09:40:00.000Z")
        ),
        Granularities.MINUTE.getIterable(new Interval(baseTime, baseTime.plus(Minutes.THREE)))
    );
  }

  @Test
  public void testIterableMinuteComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T09:38:00.000Z"),
            DateTimes.of("2011-01-01T09:39:00.000Z"),
            DateTimes.of("2011-01-01T09:40:00.000Z"),
            DateTimes.of("2011-01-01T09:41:00.000Z")
        ),
        Granularities.MINUTE.getIterable(new Interval(baseTime, baseTime.plus(Minutes.THREE)))
    );
  }

  @Test
  public void testIterable15MinuteSimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:30:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T09:30:00.000Z"),
            DateTimes.of("2011-01-01T09:45:00.000Z"),
            DateTimes.of("2011-01-01T10:00:00.000Z")
        ),
        Granularities.FIFTEEN_MINUTE.getIterable(new Interval(baseTime, baseTime.plus(Minutes.minutes(45))))
    );
  }

  @Test
  public void testIterable15MinuteComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T09:30:00.000Z"),
            DateTimes.of("2011-01-01T09:45:00.000Z"),
            DateTimes.of("2011-01-01T10:00:00.000Z"),
            DateTimes.of("2011-01-01T10:15:00.000Z")
        ),
        Granularities.FIFTEEN_MINUTE.getIterable(new Interval(baseTime, baseTime.plus(Minutes.minutes(45))))
    );
  }

  @Test
  public void testIterableHourSimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T09:00:00.000Z"),
            DateTimes.of("2011-01-01T10:00:00.000Z"),
            DateTimes.of("2011-01-01T11:00:00.000Z")
        ), Granularities.HOUR.getIterable(new Interval(baseTime, baseTime.plus(Hours.hours(3))))
    );
  }

  @Test
  public void testIterableHourComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T09:00:00.000Z"),
            DateTimes.of("2011-01-01T10:00:00.000Z"),
            DateTimes.of("2011-01-01T11:00:00.000Z"),
            DateTimes.of("2011-01-01T12:00:00.000Z")
        ), Granularities.HOUR.getIterable(new Interval(baseTime, baseTime.plus(Hours.hours(3))))
    );
  }

  @Test
  public void testIterableDaySimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            DateTimes.of("2011-01-02T00:00:00.000Z"),
            DateTimes.of("2011-01-03T00:00:00.000Z")
        ),
        Granularities.DAY.getIterable(new Interval(baseTime, baseTime.plus(Days.days(3))))
    );
  }

  @Test
  public void testIterableDayComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            DateTimes.of("2011-01-02T00:00:00.000Z"),
            DateTimes.of("2011-01-03T00:00:00.000Z"),
            DateTimes.of("2011-01-04T00:00:00.000Z")
        ),
        Granularities.DAY.getIterable(new Interval(baseTime, baseTime.plus(Days.days(3))))
    );
  }

  @Test
  public void testIterableWeekSimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-03T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-03T00:00:00.000Z"),
            DateTimes.of("2011-01-10T00:00:00.000Z"),
            DateTimes.of("2011-01-17T00:00:00.000Z")
        ),
        Granularities.WEEK.getIterable(new Interval(baseTime, baseTime.plus(Weeks.THREE)))
    );
  }

  @Test
  public void testIterableWeekComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2010-12-27T00:00:00.000Z"),
            DateTimes.of("2011-01-03T00:00:00.000Z"),
            DateTimes.of("2011-01-10T00:00:00.000Z"),
            DateTimes.of("2011-01-17T00:00:00.000Z")
        ),
        Granularities.WEEK.getIterable(new Interval(baseTime, baseTime.plus(Weeks.THREE)))
    );
  }

  @Test
  public void testIterableMonthSimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            DateTimes.of("2011-02-01T00:00:00.000Z"),
            DateTimes.of("2011-03-01T00:00:00.000Z")
        ),
        Granularities.MONTH.getIterable(new Interval(baseTime, baseTime.plus(Months.THREE)))
    );
  }

  @Test
  public void testIterableMonthComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            DateTimes.of("2011-02-01T00:00:00.000Z"),
            DateTimes.of("2011-03-01T00:00:00.000Z"),
            DateTimes.of("2011-04-01T00:00:00.000Z")
        ),
        Granularities.MONTH.getIterable(new Interval(baseTime, baseTime.plus(Months.THREE)))
    );
  }

  @Test
  public void testIterableQuarterSimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            DateTimes.of("2011-07-01T00:00:00.000Z")
        ),
        Granularities.QUARTER.getIterable(new Interval(baseTime, baseTime.plus(Months.NINE)))
    );
  }

  @Test
  public void testIterableQuarterComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            DateTimes.of("2011-07-01T00:00:00.000Z"),
            DateTimes.of("2011-10-01T00:00:00.000Z")
        ),
        Granularities.QUARTER.getIterable(new Interval(baseTime, baseTime.plus(Months.NINE)))
    );
  }

  @Test
  public void testIterableYearSimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            DateTimes.of("2012-01-01T00:00:00.000Z"),
            DateTimes.of("2013-01-01T00:00:00.000Z")
        ),
        Granularities.YEAR.getIterable(new Interval(baseTime, baseTime.plus(Years.THREE)))
    );
  }

  @Test
  public void testIterableYearComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            DateTimes.of("2012-01-01T00:00:00.000Z"),
            DateTimes.of("2013-01-01T00:00:00.000Z"),
            DateTimes.of("2014-01-01T00:00:00.000Z")
        ),
        Granularities.YEAR.getIterable(new Interval(baseTime, baseTime.plus(Years.THREE)))
    );
  }

  @Test
  public void testPeriodDaylightSaving()
  {
    final DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-04T00:00:00", tz);
    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00", tz),
            new DateTime("2012-11-05T00:00:00.000-08:00", tz),
            new DateTime("2012-11-06T00:00:00.000-08:00", tz)
        ),
        new PeriodGranularity(new Period("P1D"), null, tz)
            .getIterable(new Interval(baseTime, baseTime.plus(Days.days(3))))
    );

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00", tz),
            new DateTime("2012-11-04T01:00:00.000-07:00", tz),
            new DateTime("2012-11-04T01:00:00.000-08:00", tz),
            new DateTime("2012-11-04T02:00:00.000-08:00", tz),
            new DateTime("2012-11-04T03:00:00.000-08:00", tz)
        ),
        new PeriodGranularity(new Period("PT1H"), null, tz)
            .getIterable(new Interval(baseTime, baseTime.plus(Hours.hours(5))))
    );

    final PeriodGranularity hour = new PeriodGranularity(new Period("PT1H"), null, tz);
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00", tz),
            new DateTime("2012-11-04T01:00:00.000-07:00", tz),
            new DateTime("2012-11-04T01:00:00.000-08:00", tz),
            new DateTime("2012-11-04T02:00:00.000-08:00", tz),
            new DateTime("2012-11-04T03:00:00.000-08:00", tz)
        ),
        Lists.newArrayList(
            hour.bucketStart(DateTimes.of("2012-11-04T00:30:00-07:00")),
            hour.bucketStart(DateTimes.of("2012-11-04T01:30:00-07:00")),
            hour.bucketStart(DateTimes.of("2012-11-04T01:30:00-08:00")),
            hour.bucketStart(DateTimes.of("2012-11-04T02:30:00-08:00")),
            hour.bucketStart(DateTimes.of("2012-11-04T03:30:00-08:00"))
        )
    );

    final PeriodGranularity p7days = new PeriodGranularity(
        new Period("P7D"),
        DateTimes.of("2022-03-24T02:35:00.000-07:00"),
        tz
    );
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2022-03-03T02:35:00.000-08:00", tz),
            new DateTime("2022-03-10T02:35:00.000-08:00", tz),
            new DateTime("2022-03-24T02:35:00.000-07:00", tz),
            new DateTime("2022-03-31T02:35:00.000-07:00", tz)

        ),
        Lists.newArrayList(
            p7days.bucketStart(DateTimes.of("2022-03-04T02:35:00.000-08:00")),
            p7days.bucketStart(DateTimes.of("2022-03-16T02:35:00.000-07:00")),
            p7days.bucketStart(DateTimes.of("2022-03-26T02:35:00.000-07:00")),
            p7days.bucketStart(DateTimes.of("2022-03-31T03:35:00.000-07:00"))
        )
    );

    final PeriodGranularity week = new PeriodGranularity(
        new Period("P1W"),
        DateTimes.of("2022-03-24T02:35:00.000-07:00"),
        tz
    );

    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2022-03-03T02:35:00.000-08:00", tz),
            new DateTime("2022-03-10T02:35:00.000-08:00", tz),
            new DateTime("2022-03-24T02:35:00.000-07:00", tz),
            new DateTime("2022-03-31T02:35:00.000-07:00", tz)

        ),
        Lists.newArrayList(
            week.bucketStart(DateTimes.of("2022-03-04T02:35:00.000-08:00")),
            week.bucketStart(DateTimes.of("2022-03-16T02:35:00.000-07:00")),
            week.bucketStart(DateTimes.of("2022-03-26T02:35:00.000-07:00")),
            week.bucketStart(DateTimes.of("2022-03-31T03:35:00.000-07:00"))
        )
    );

    final PeriodGranularity month = new PeriodGranularity(
        new Period("P1M"),
        DateTimes.of("2022-03-24T02:35:00.000-07:00"),
        tz
    );

    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2022-02-24T02:35:00.000-08:00", tz),
            new DateTime("2022-02-24T02:35:00.000-08:00", tz),
            new DateTime("2022-03-24T02:35:00.000-07:00", tz),
            new DateTime("2022-03-24T02:35:00.000-07:00", tz)

        ),
        Lists.newArrayList(
            month.bucketStart(DateTimes.of("2022-03-04T02:35:00.000-08:00")),
            month.bucketStart(DateTimes.of("2022-03-16T02:35:00.000-07:00")),
            month.bucketStart(DateTimes.of("2022-03-26T02:35:00.000-07:00")),
            month.bucketStart(DateTimes.of("2022-03-31T03:35:00.000-07:00"))
        )
    );

    final PeriodGranularity year = new PeriodGranularity(
        new Period("P1Y"),
        DateTimes.of("2022-03-24T02:35:00.000-07:00"),
        tz
    );

    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2021-03-24T02:35:00.000-07:00", tz),
            new DateTime("2021-03-24T02:35:00.000-07:00", tz),
            new DateTime("2022-03-24T02:35:00.000-07:00", tz),
            new DateTime("2022-03-24T02:35:00.000-07:00", tz)

        ),
        Lists.newArrayList(
            year.bucketStart(DateTimes.of("2022-03-04T02:35:00.000-08:00")),
            year.bucketStart(DateTimes.of("2022-03-16T02:35:00.000-07:00")),
            year.bucketStart(DateTimes.of("2022-03-26T02:35:00.000-07:00")),
            year.bucketStart(DateTimes.of("2022-03-31T03:35:00.000-07:00"))
        )
    );
  }

  @Test
  public void testIterableMonth()
  {
    final DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-03T10:00:00", tz);
    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-11-01T00:00:00.000-07:00", tz),
            new DateTime("2012-12-01T00:00:00.000-08:00", tz),
            new DateTime("2013-01-01T00:00:00.000-08:00", tz),
            new DateTime("2013-02-01T00:00:00.000-08:00", tz)
        ),
        new PeriodGranularity(new Period("P1M"), null, tz)
            .getIterable(new Interval(baseTime, baseTime.plus(Months.months(3))))
    );
  }

  @Test
  public void testIterableWeek()
  {
    final DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-03T10:00:00", tz);
    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-10-29T00:00:00.000-07:00", tz),
            new DateTime("2012-11-05T00:00:00.000-08:00", tz),
            new DateTime("2012-11-12T00:00:00.000-08:00", tz),
            new DateTime("2012-11-19T00:00:00.000-08:00", tz)
        ),
        new PeriodGranularity(new Period("P1W"), null, tz)
            .getIterable(new Interval(baseTime, baseTime.plus(Weeks.weeks(3))))
    );

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-11-03T10:00:00.000-07:00", tz),
            new DateTime("2012-11-10T10:00:00.000-08:00", tz),
            new DateTime("2012-11-17T10:00:00.000-08:00", tz)
        ),
        new PeriodGranularity(new Period("P1W"), baseTime, tz)
            .getIterable(new Interval(baseTime, baseTime.plus(Weeks.weeks(3))))
    );
  }

  @Test
  public void testPeriodTruncateDays()
  {
    final DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");
    final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
    PeriodGranularity periodOrigin = new PeriodGranularity(
        new Period("P2D"),
        origin,
        tz
    );
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2011-12-31T05:00:00.000-08:00", tz),
            new DateTime("2012-01-02T05:00:00.000-08:00", tz),
            new DateTime("2012-01-04T05:00:00.000-08:00", tz)
        ),
        Lists.newArrayList(
            periodOrigin.bucketStart(DateTimes.of("2012-01-01T05:00:04.123-08:00")),
            periodOrigin.bucketStart(DateTimes.of("2012-01-02T07:00:04.123-08:00")),
            periodOrigin.bucketStart(DateTimes.of("2012-01-04T07:20:04.123-08:00"))

        )
    );

    PeriodGranularity periodNoOrigin = new PeriodGranularity(
        new Period("P2D"),
        null,
        tz
    );
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2012-01-01T00:00:00.000-08:00", tz),
            new DateTime("2012-01-01T00:00:00.000-08:00", tz),
            new DateTime("2012-01-03T00:00:00.000-08:00", tz)
        ),
        Lists.newArrayList(
            periodNoOrigin.bucketStart(DateTimes.of("2012-01-01T05:00:04.123-08:00")),
            periodNoOrigin.bucketStart(DateTimes.of("2012-01-02T07:00:04.123-08:00")),
            periodNoOrigin.bucketStart(DateTimes.of("2012-01-04T07:20:04.123-08:00"))

        )
    );
  }

  @Test
  public void testPeriodTruncateMinutes()
  {
    final DateTime origin = DateTimes.of("2012-01-02T00:05:00.000Z");
    PeriodGranularity periodOrigin = new PeriodGranularity(new Period("PT15M"), origin, null);
    assertSameDateTime(
        Lists.newArrayList(
            DateTimes.of("2012-01-01T04:50:00.000Z"),
            DateTimes.of("2012-01-02T07:05:00.000Z"),
            DateTimes.of("2012-01-04T00:20:00.000Z")
        ),
        Lists.newArrayList(
            periodOrigin.bucketStart(DateTimes.of("2012-01-01T05:00:04.123Z")),
            periodOrigin.bucketStart(DateTimes.of("2012-01-02T07:08:04.123Z")),
            periodOrigin.bucketStart(DateTimes.of("2012-01-04T00:20:04.123Z"))

        )
    );

    PeriodGranularity periodNoOrigin = new PeriodGranularity(new Period("PT15M"), null, null);
    assertSameDateTime(
        Lists.newArrayList(
            DateTimes.of("2012-01-01T05:00:00.000Z"),
            DateTimes.of("2012-01-02T07:00:00.000Z"),
            DateTimes.of("2012-01-04T00:15:00.000Z")
        ),
        Lists.newArrayList(
            periodNoOrigin.bucketStart(DateTimes.of("2012-01-01T05:00:04.123Z")),
            periodNoOrigin.bucketStart(DateTimes.of("2012-01-02T07:00:04.123Z")),
            periodNoOrigin.bucketStart(DateTimes.of("2012-01-04T00:20:04.123Z"))

        )
    );
  }

  @Test
  public void testCompoundPeriodTruncate()
  {
    {
      final DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");
      final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
      PeriodGranularity periodOrigin = new PeriodGranularity(
          new Period("P1M2D"),
          origin,
          tz
      );
      assertSameDateTime(
          Lists.newArrayList(
              new DateTime("2011-11-30T05:00:00.000-08:00", tz),
              new DateTime("2012-01-02T05:00:00.000-08:00", tz),
              new DateTime("2012-02-04T05:00:00.000-08:00", tz),
              new DateTime("2012-02-04T05:00:00.000-08:00", tz)
          ),
          Lists.newArrayList(
              periodOrigin.bucketStart(DateTimes.of("2012-01-01T05:00:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-01-02T07:00:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-03-01T07:20:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-02-04T05:00:00.000-08:00"))
          )
      );

      PeriodGranularity periodNoOrigin = new PeriodGranularity(
          new Period("P1M2D"),
          null,
          tz
      );
      assertSameDateTime(
          Lists.newArrayList(
              new DateTime("1970-01-01T00:00:00.000-08:00", tz),
              new DateTime("2011-12-12T00:00:00.000-08:00", tz),
              new DateTime("2012-01-14T00:00:00.000-08:00", tz),
              new DateTime("2012-02-16T00:00:00.000-08:00", tz)
          ),
          Lists.newArrayList(
              periodNoOrigin.bucketStart(DateTimes.of("1970-01-01T05:02:04.123-08:00")),
              periodNoOrigin.bucketStart(DateTimes.of("2012-01-01T05:02:04.123-08:00")),
              periodNoOrigin.bucketStart(DateTimes.of("2012-01-15T07:01:04.123-08:00")),
              periodNoOrigin.bucketStart(DateTimes.of("2012-02-16T00:00:00.000-08:00"))

          )
      );
    }

    {
      final DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");
      final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
      PeriodGranularity periodOrigin = new PeriodGranularity(
          new Period("PT12H5M"),
          origin,
          tz
      );
      assertSameDateTime(
          Lists.newArrayList(
              new DateTime("2012-01-01T04:50:00.000-08:00", tz),
              new DateTime("2012-01-02T05:00:00.000-08:00", tz),
              new DateTime("2012-01-02T17:05:00.000-08:00", tz),
              new DateTime("2012-02-03T22:25:00.000-08:00", tz)
          ),
          Lists.newArrayList(
              periodOrigin.bucketStart(DateTimes.of("2012-01-01T05:00:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-01-02T07:00:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-01-03T00:20:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-02-03T22:25:00.000-08:00"))
          )
      );
    }
  }

  @Test
  public void testCompoundPeriodMillisTruncate()
  {
    {
      final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
      PeriodGranularity periodOrigin = new PeriodGranularity(
          new Period("PT12H5M"),
          origin,
          DateTimeZone.UTC
      );
      assertSameDateTime(
          Lists.newArrayList(
              DateTimes.of("2012-01-01T04:50:00.000-08:00"),
              DateTimes.of("2012-01-02T05:00:00.000-08:00"),
              DateTimes.of("2012-01-02T17:05:00.000-08:00"),
              DateTimes.of("2012-02-03T22:25:00.000-08:00")
          ),
          Lists.newArrayList(
              periodOrigin.bucketStart(DateTimes.of("2012-01-01T05:00:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-01-02T07:00:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-01-03T00:20:04.123-08:00")),
              periodOrigin.bucketStart(DateTimes.of("2012-02-03T22:25:00.000-08:00"))
          )
      );
    }
  }

  @Test
  public void testDurationBucketStart()
  {
    {
      final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
      Granularity gran = new DurationGranularity(
          new Period("PT12H5M").toStandardDuration().getMillis(),
          origin
      );

      assertSameDateTime(
          Lists.newArrayList(
              DateTimes.of("2012-01-01T04:50:00.000-08:00"),
              DateTimes.of("2012-01-02T05:00:00.000-08:00"),
              DateTimes.of("2012-01-02T17:05:00.000-08:00"),
              DateTimes.of("2012-02-03T22:25:00.000-08:00")
          ),
          Lists.newArrayList(
              gran.bucketStart(DateTimes.of("2012-01-01T05:00:04.123-08:00")),
              gran.bucketStart(DateTimes.of("2012-01-02T07:00:04.123-08:00")),
              gran.bucketStart(DateTimes.of("2012-01-03T00:20:04.123-08:00")),
              gran.bucketStart(DateTimes.of("2012-02-03T22:25:00.000-08:00"))
          )
      );

      // Same as above, but using the millis form of the method.
      Assertions.assertEquals(
          DateTimes.of("2012-01-01T04:50:00.000-08:00").getMillis(),
          gran.bucketStart(DateTimes.of("2012-01-01T05:00:04.123-08:00").getMillis())
      );
      Assertions.assertEquals(
          DateTimes.of("2012-01-02T05:00:00.000-08:00").getMillis(),
          gran.bucketStart(DateTimes.of("2012-01-02T07:00:04.123-08:00").getMillis())
      );
      Assertions.assertEquals(
          DateTimes.of("2012-01-02T17:05:00.000-08:00").getMillis(),
          gran.bucketStart(DateTimes.of("2012-01-03T00:20:04.123-08:00").getMillis())
      );
      Assertions.assertEquals(
          DateTimes.of("2012-02-03T22:25:00.000-08:00").getMillis(),
          gran.bucketStart(DateTimes.of("2012-02-03T22:25:00.000-08:00").getMillis())
      );
    }
  }

  @Test
  public void testDurationIncrement()
  {
    {
      final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
      Granularity gran = new DurationGranularity(
          new Period("PT12H5M").toStandardDuration().getMillis(),
          origin
      );

      Assertions.assertEquals(
          DateTimes.of("2012-01-01T17:05:04.123-08:00"),
          gran.increment(DateTimes.of("2012-01-01T05:00:04.123-08:00"))
      );
      Assertions.assertEquals(
          DateTimes.of("2012-01-02T19:05:04.123-08:00"),
          gran.increment(DateTimes.of("2012-01-02T07:00:04.123-08:00"))
      );
      Assertions.assertEquals(
          DateTimes.of("2012-01-03T12:25:04.123-08:00"),
          gran.increment(DateTimes.of("2012-01-03T00:20:04.123-08:00"))
      );
      Assertions.assertEquals(
          DateTimes.of("2012-02-04T10:30:00.000-08:00"),
          gran.increment(DateTimes.of("2012-02-03T22:25:00.000-08:00"))
      );
    }
  }

  @Test
  public void testDurationIncrementOnMillis()
  {
    {
      final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
      Granularity gran = new DurationGranularity(
          new Period("PT12H5M").toStandardDuration().getMillis(),
          origin
      );

      Assertions.assertEquals(
          DateTimes.of("2012-01-01T17:05:04.123-08:00").getMillis(),
          gran.increment(DateTimes.of("2012-01-01T05:00:04.123-08:00").getMillis())
      );
      Assertions.assertEquals(
          DateTimes.of("2012-01-02T19:05:04.123-08:00").getMillis(),
          gran.increment(DateTimes.of("2012-01-02T07:00:04.123-08:00")).getMillis()
      );
      Assertions.assertEquals(
          DateTimes.of("2012-01-03T12:25:04.123-08:00").getMillis(),
          gran.increment(DateTimes.of("2012-01-03T00:20:04.123-08:00")).getMillis()
      );
      Assertions.assertEquals(
          DateTimes.of("2012-02-04T10:30:00.000-08:00").getMillis(),
          gran.increment(DateTimes.of("2012-02-03T22:25:00.000-08:00")).getMillis()
      );
    }
  }

  @Test
  public void testDurationToDateTime()
  {
    final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
    Granularity gran = new DurationGranularity(
        new Period("PT12H5M").toStandardDuration().getMillis(),
        origin
    );

    Assertions.assertEquals(
        DateTimes.of("2012-01-01T05:00:04.123-08:00"),
        gran.toDateTime(DateTimes.of("2012-01-01T05:00:04.123-08:00").getMillis())
    );
  }


  @Test
  public void testIterableAllSimple()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Collections.singletonList(baseTime),
        Granularities.ALL.getIterable(new Interval(baseTime, baseTime.plus(Days.days(3))))
    );
  }

  @Test
  public void testIterableAllComplex()
  {
    final DateTime baseTime = DateTimes.of("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Collections.singletonList(baseTime),
        Granularities.ALL.getIterable(new Interval(baseTime, baseTime.plus(Days.days(3))))
    );
  }

  @Test
  public void testSerializePeriod() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();

    String json = "{ \"type\": \"period\", \"period\": \"P1D\" }";
    Granularity gran = mapper.readValue(json, Granularity.class);
    Assertions.assertEquals(new PeriodGranularity(new Period("P1D"), null, null), gran);

    // Nonstandard period
    json = "{ \"type\": \"period\", \"period\": \"P2D\" }";
    gran = mapper.readValue(json, Granularity.class);
    Assertions.assertEquals(new PeriodGranularity(new Period("P2D"), null, null), gran);

    // Set timeZone, origin
    json = "{ \"type\": \"period\", \"period\": \"P1D\","
           + "\"timeZone\": \"America/Los_Angeles\", \"origin\": \"1970-01-01T00:00:00Z\"}";
    gran = mapper.readValue(json, Granularity.class);
    Assertions.assertEquals(new PeriodGranularity(
        new Period("P1D"),
        DateTimes.EPOCH,
        DateTimes.inferTzFromString("America/Los_Angeles")
    ), gran);

    PeriodGranularity expected = new PeriodGranularity(
        new Period("P1D"),
        DateTimes.of("2012-01-01"),
        DateTimes.inferTzFromString("America/Los_Angeles")
    );

    String jsonOut = mapper.writeValueAsString(expected);
    Assertions.assertEquals(expected, mapper.readValue(jsonOut, Granularity.class));

    String illegalJson = "{ \"type\": \"period\", \"period\": \"P0D\" }";
    try {
      mapper.readValue(illegalJson, Granularity.class);
      Assertions.fail();
    }
    catch (JsonMappingException e) {
    }
  }

  @Test
  public void testSerializeDuration() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    String json = "{ \"type\": \"duration\", \"duration\": \"3600000\" }";
    Granularity gran = mapper.readValue(json, Granularity.class);
    Assertions.assertEquals(new DurationGranularity(3600000, null), gran);

    json = "{ \"type\": \"duration\", \"duration\": \"5\", \"origin\": \"2012-09-01T00:00:00.002Z\" }";
    gran = mapper.readValue(json, Granularity.class);
    Assertions.assertEquals(new DurationGranularity(5, 2), gran);

    DurationGranularity expected = new DurationGranularity(5, 2);
    Assertions.assertEquals(expected, mapper.readValue(mapper.writeValueAsString(expected), Granularity.class));

    String illegalJson = "{ \"type\": \"duration\", \"duration\": \"0\" }";
    try {
      mapper.readValue(illegalJson, Granularity.class);
      Assertions.fail();
    }
    catch (JsonMappingException e) {
    }
  }

  @Test
  public void testStandardGranularitiesSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    for (GranularityType granularityType : GranularityType.values()) {
      final Granularity granularity = granularityType.getDefaultGranularity();

      Assertions.assertEquals(
          granularity,
          mapper.readValue("\"" + StringUtils.toUpperCase(granularityType.name()) + "\"", Granularity.class)
      );

      Assertions.assertEquals(
          granularity,
          mapper.readValue("\"" + StringUtils.toLowerCase(granularityType.name()) + "\"", Granularity.class)
      );

      Assertions.assertEquals(
          granularity,
          mapper.readValue(mapper.writeValueAsString(granularity), Granularity.class)
      );

      if (granularityType == GranularityType.ALL || granularityType == GranularityType.NONE) {
        Assertions.assertEquals(
            "{\"type\":\"" + StringUtils.toLowerCase(granularityType.name()) + "\"}",
            mapper.writeValueAsString(granularity)
        );
      } else {
        Assertions.assertEquals(
            "\"" + StringUtils.toUpperCase(granularityType.name()) + "\"",
            mapper.writeValueAsString(granularity)
        );
      }
    }
  }

  @Test
  public void testMerge()
  {
    Assertions.assertNull(Granularity.mergeGranularities(null));
    Assertions.assertNull(Granularity.mergeGranularities(ImmutableList.of()));
    Assertions.assertNull(Granularity.mergeGranularities(Lists.newArrayList(null, Granularities.DAY)));
    Assertions.assertNull(Granularity.mergeGranularities(Lists.newArrayList(Granularities.DAY, null)));
    Assertions.assertNull(
        Granularity.mergeGranularities(
            Lists.newArrayList(
                Granularities.DAY,
                null,
                Granularities.DAY
            )
        )
    );
    Assertions.assertNull(
        Granularity.mergeGranularities(ImmutableList.of(Granularities.ALL, Granularities.DAY))
    );

    Assertions.assertEquals(
        Granularities.ALL,
        Granularity.mergeGranularities(ImmutableList.of(Granularities.ALL, Granularities.ALL))
    );
  }

  private void assertSameDateTime(List<DateTime> expected, Iterable<DateTime> actual)
  {
    Assertions.assertEquals(expected.size(), Iterables.size(actual));
    Iterator<DateTime> actualIter = actual.iterator();
    Iterator<DateTime> expectedIter = expected.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      Assertions.assertEquals(expectedIter.next(), actualIter.next());
    }
    Assertions.assertFalse(actualIter.hasNext(), "actualIter not exhausted!?");
    Assertions.assertFalse(expectedIter.hasNext(), "expectedIter not exhausted!?");
  }

  private void assertSameInterval(List<DateTime> expected, Iterable<Interval> actual)
  {
    Assertions.assertEquals(expected.size(), Iterables.size(actual));
    Iterator<Interval> actualIter = actual.iterator();
    Iterator<DateTime> expectedIter = expected.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      Assertions.assertEquals(expectedIter.next(), actualIter.next().getStart());
    }
    Assertions.assertFalse(actualIter.hasNext(), "actualIter not exhausted!?");
    Assertions.assertFalse(expectedIter.hasNext(), "expectedIter not exhausted!?");
  }

  @Test
  public void testHourlyExplicitOriginAlignment()
  {
    for (final String zone : new String[]{
        "UTC", "Asia/Kuala_Lumpur", "Asia/Singapore", "Asia/Kathmandu", "Australia/Lord_Howe", "America/Los_Angeles"
    }) {
      for (final String originString : new String[]{
          "1969-12-31T16:30:00Z", "1970-01-01T00:00:00Z", "1970-01-01T00:30:00Z"
      }) {
        final DateTime origin = DateTimes.of(originString);
        for (final int hours : new int[]{1, 2, 3}) {
          final PeriodGranularity granularity = new PeriodGranularity(
              Period.hours(hours),
              origin,
              DateTimes.inferTzFromString(zone)
          );
          final long duration = Hours.hours(hours).toStandardDuration().getMillis();
          for (final long timestamp : new long[]{
              origin.getMillis() - duration - 1,
              origin.getMillis() - 1,
              origin.getMillis(),
              origin.getMillis() + 1,
              DateTimes.of("2026-04-04T15:00:00Z").getMillis(),
              DateTimes.of("2026-10-03T15:30:00.001Z").getMillis(),
              DateTimes.of("2026-11-19T16:00:00Z").getMillis(),
              DateTimes.of("2026-11-19T16:10:00Z").getMillis(),
              DateTimes.of("2026-11-19T16:29:59.999Z").getMillis(),
              DateTimes.of("2026-11-19T16:30:00Z").getMillis(),
              DateTimes.of("2026-11-19T16:30:00.001Z").getMillis()
          }) {
            final long expectedStart = origin.getMillis()
                                       + Math.floorDiv(timestamp - origin.getMillis(), duration) * duration;
            final long actualStart = granularity.bucketStart(timestamp);
            final long actualEnd = granularity.bucketEnd(DateTimes.utc(timestamp)).getMillis();
            final String message = StringUtils.format(
                "Granularity [%s], timestamp [%s]",
                granularity,
                DateTimes.utc(timestamp)
            );
            Assertions.assertEquals(expectedStart, actualStart, message);
            Assertions.assertEquals(actualStart, granularity.bucketStart(actualStart), message);
            Assertions.assertTrue(actualStart <= timestamp && timestamp < actualEnd, message);
          }
        }
      }
    }
  }

  @Test
  public void testHourlyExplicitNegativeOriginSegmentInterval()
  {
    final PeriodGranularity granularity = new PeriodGranularity(
        Period.hours(1),
        DateTimes.of("1969-12-31T16:30:00Z"),
        DateTimes.inferTzFromString("Asia/Kuala_Lumpur")
    );
    final DateTime minTime = DateTimes.of("2026-11-19T16:05:00Z");
    final DateTime maxTime = DateTimes.of("2026-11-19T16:20:00Z");
    // Cursor construction uses the same bounds. The bucket end must not precede the segment's rows.
    final Interval interval = new Interval(minTime, granularity.bucketEnd(maxTime));
    Assertions.assertEquals(DateTimes.of("2026-11-19T16:30:00Z"), interval.getEnd());
    Assertions.assertTrue(interval.contains(minTime));
    Assertions.assertTrue(interval.contains(maxTime));
  }

  @Test
  public void testHourlyImplicitOriginAlignment()
  {
    for (final String zone : new String[]{
        "UTC", "Asia/Kuala_Lumpur", "Asia/Singapore", "Asia/Kathmandu", "Asia/Dhaka",
        "Australia/Lord_Howe", "America/Los_Angeles"
    }) {
      final DateTimeZone timeZone = DateTimes.inferTzFromString(zone);
      final DateTime defaultOrigin = new DateTime(1970, 1, 1, 0, 0, timeZone);
      for (final int hours : new int[]{2, 3, 6, 12, 24, 25}) {
        final PeriodGranularity granularity = new PeriodGranularity(Period.hours(hours), null, timeZone);
        final long duration = Hours.hours(hours).toStandardDuration().getMillis();
        for (final DateTime timestamp : new DateTime[]{
            defaultOrigin.minus(1),
            defaultOrigin,
            defaultOrigin.plus(1),
            DateTimes.of("2011-03-15T20:10:00+05:45"),
            DateTimes.of("2011-03-15T20:15:00+05:45"),
            DateTimes.of("2011-03-15T20:15:00.001+05:45"),
            DateTimes.of("2026-04-04T14:59:59.999Z"),
            DateTimes.of("2026-04-04T15:00:00Z"),
            DateTimes.of("2026-10-03T15:29:59.999Z"),
            DateTimes.of("2026-10-03T15:30:00Z"),
            DateTimes.of("2026-10-03T15:30:00.001Z"),
            DateTimes.of("2026-11-19T20:10:00+05:45")
        }) {
          final long input = timestamp.getMillis();
          final long start = granularity.bucketStart(input);
          final long end = granularity.bucketEnd(timestamp).getMillis();
          final String message = StringUtils.format("Granularity [%s], timestamp [%s]", granularity, timestamp);
          Assertions.assertEquals(start, granularity.bucketStart(start), message);
          Assertions.assertTrue(start <= input && input < end, message);
          Assertions.assertTrue(granularity.bucketStart(input + 1) >= start, message);
          Assertions.assertEquals(start, granularity.bucketStart(end - 1), message);
          Assertions.assertEquals(end, granularity.bucketStart(end), message);
          Assertions.assertEquals(end, granularity.increment(granularity.bucketStart(timestamp)).getMillis(), message);
          // Iteration must use the same boundaries as direct bucketing, including at offset transitions.
          final Iterator<Interval> buckets = granularity.getIterable(Intervals.utc(start, end + duration)).iterator();
          Assertions.assertEquals(new Interval(start, end, timeZone), buckets.next(), message);
          Assertions.assertEquals(end, buckets.next().getStartMillis(), message);
          if (input != start) {
            // Incrementing an arbitrary timestamp must retain the existing period-addition behavior.
            Assertions.assertEquals(timestamp.plusHours(hours).getMillis(), granularity.increment(input), message);
          }
        }
      }
    }
  }

  @Test
  public void testHourlyImplicitOriginKathmanduBoundaries()
  {
    final DateTimeZone timeZone = DateTimes.inferTzFromString("Asia/Kathmandu");
    final PeriodGranularity implicit = new PeriodGranularity(Period.hours(2), null, timeZone);
    final PeriodGranularity explicit = new PeriodGranularity(
        Period.hours(2),
        new DateTime(1970, 1, 1, 0, 0, timeZone),
        timeZone
    );
    final DateTime start = new DateTime("2011-03-15T20:00:00+05:45", timeZone);
    for (final DateTime timestamp : new DateTime[]{
        start,
        start.plusMinutes(10),
        start.plusMinutes(15),
        start.plusMinutes(15).plus(1),
        start.plusHours(2).minus(1)
    }) {
      Assertions.assertEquals(start, implicit.bucketStart(timestamp));
      Assertions.assertEquals(start.plusHours(2), implicit.bucketEnd(timestamp));
    }
    Assertions.assertEquals(start.plusHours(2), implicit.bucketStart(start.plusHours(2)));
    // Explicit origins intentionally keep their :15 alignment; only the implicit origin uses local clock hours.
    Assertions.assertEquals(start.plusMinutes(15), explicit.bucketStart(start.plusMinutes(30)));
  }

  @Test
  public void testHourlyImplicitOriginOffsetTransitions()
  {
    final DateTimeZone timeZone = DateTimes.inferTzFromString("Australia/Lord_Howe");
    final PeriodGranularity granularity = new PeriodGranularity(Period.hours(2), null, timeZone);
    // The rounded boundaries straddle a 30-minute offset change, so these buckets have 90 and 150 minutes.
    for (final String[] sample : new String[][]{
        {"2026-04-04T15:00:00Z", "2026-04-04T14:00:00Z", "2026-04-04T15:30:00Z"},
        {"2026-10-03T15:30:00.001Z", "2026-10-03T13:30:00Z", "2026-10-03T16:00:00Z"}
    }) {
      final DateTime timestamp = DateTimes.of(sample[0]);
      final long start = DateTimes.of(sample[1]).getMillis();
      final long end = DateTimes.of(sample[2]).getMillis();
      Assertions.assertEquals(start, granularity.bucketStart(timestamp).getMillis());
      Assertions.assertEquals(end, granularity.bucketEnd(timestamp).getMillis());
      Assertions.assertEquals(start, granularity.bucketStart(start));
      Assertions.assertEquals(start, granularity.bucketStart(end - 1));
      Assertions.assertEquals(end, granularity.bucketStart(end));
      Assertions.assertEquals(end, granularity.increment(start));
    }
  }

  @Test
  public void testTruncateKathmandu()
  {
    final DateTimeZone tz = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Kathmandu"));
    final DateTime date = new DateTime("2011-03-15T21:42:23.898+05:45", tz);
    final PeriodGranularity year = new PeriodGranularity(new Period("P1Y"), null, tz);
    final PeriodGranularity hour = new PeriodGranularity(new Period("PT1H"), null, tz);
    final PeriodGranularity twoHour = new PeriodGranularity(new Period("PT2H"), null, tz);

    assertBucketStart(
        year,
        date,
        new DateTime("2011-01-01T00:00:00.000+05:45", tz)
    );

    assertBucketStart(
        hour,
        date,
        new DateTime("2011-03-15T21:00:00.000+05:45", tz)
    );

    assertBucketStart(
        twoHour,
        date,
        new DateTime("2011-03-15T20:00:00.000+05:45", tz)
    );
  }

  @Test
  public void testTruncateDhaka()
  {
    final DateTimeZone tz = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Dhaka"));
    final DateTime date = new DateTime("2011-03-15T21:42:23.898+06:00", tz);
    final PeriodGranularity year = new PeriodGranularity(new Period("P1Y"), null, tz);
    final PeriodGranularity hour = new PeriodGranularity(new Period("PT1H"), null, tz);
    final PeriodGranularity twoHour = new PeriodGranularity(new Period("PT2H"), null, tz);

    assertBucketStart(
        year,
        date,
        new DateTime("2011-01-01T00:00:00.000+06:00", tz)
    );

    assertBucketStart(
        hour,
        date,
        new DateTime("2011-03-15T21:00:00.000+06:00", tz)
    );

    assertBucketStart(
        twoHour,
        date,
        new DateTime("2011-03-15T20:00:00.000+06:00", tz)
    );
  }

  @Test
  public void testToVirtualColumn()
  {
    final Granularity hour = new PeriodGranularity(new Period("PT1H"), null, null);
    final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
    final DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");
    final Granularity hourWithOrigin = new PeriodGranularity(new Period("PT1H"), origin, tz);
    final PeriodGranularity hourWithTz = new PeriodGranularity(new Period("PT1H"), null, tz);
    final Granularity duration = new DurationGranularity(
        new Period("PT12H5M").toStandardDuration().getMillis(),
        origin
    );

    ExpressionVirtualColumn column = Granularities.toVirtualColumn(hour, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertEquals("timestamp_floor(__time,'PT1H',null,'UTC')", column.getExpression());
    column = Granularities.toVirtualColumn(hourWithOrigin, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertEquals(
        "timestamp_floor(__time,'PT1H','2012-01-02T13:00:00.000Z','America/Los_Angeles')",
        column.getExpression()
    );
    column = Granularities.toVirtualColumn(hourWithTz, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertEquals("timestamp_floor(__time,'PT1H',null,'America/Los_Angeles')", column.getExpression());
    column = Granularities.toVirtualColumn(duration, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertEquals(ColumnHolder.TIME_COLUMN_NAME, column.getExpression());
    column = Granularities.toVirtualColumn(Granularities.NONE, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertEquals(ColumnHolder.TIME_COLUMN_NAME, column.getExpression());
    column = Granularities.toVirtualColumn(Granularities.ALL, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertNull(column);
    column = Granularities.toVirtualColumn(Granularities.HOUR, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertEquals("timestamp_floor(__time,'PT1H',null,'UTC')", column.getExpression());
    column = Granularities.toVirtualColumn(Granularities.MINUTE, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertEquals("timestamp_floor(__time,'PT1M',null,'UTC')", column.getExpression());
    column = Granularities.toVirtualColumn(Granularities.FIFTEEN_MINUTE, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertEquals("timestamp_floor(__time,'PT15M',null,'UTC')", column.getExpression());
  }

  @Test
  public void testFromVirtualColumn()
  {
    ExpressionVirtualColumn hourly = Granularities.toVirtualColumn(Granularities.HOUR, "v0");
    ExpressionVirtualColumn day = Granularities.toVirtualColumn(Granularities.DAY, "v0");
    ExpressionVirtualColumn hourlyNonstandardTime = new ExpressionVirtualColumn(
        "v0",
        "timestamp_floor(__gran, 'PT1H')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn hourlyPacificTime = new ExpressionVirtualColumn(
        "v0",
        "timestamp_floor(__gran, 'PT1H', null, 'America/Los_Angeles')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn hourlyIndianTime = new ExpressionVirtualColumn(
        "v0",
        "timestamp_floor(__gran, 'PT1H', null, 'Asia/Kolkata')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn ceilHour = new ExpressionVirtualColumn(
        "v0",
        "timestamp_ceil(__time, 'PT1M')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn floorWithExpression = new ExpressionVirtualColumn(
        "v0",
        "timestamp_floor(timestamp_parse(__time,null,'UTC'), 'PT1M')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn floorWithTimezone = new ExpressionVirtualColumn(
        "v0",
        "timestamp_floor(__time, 'PT1M', null,'America/Los_Angeles')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn floorWithOriginTimezone = new ExpressionVirtualColumn(
        "v0",
        "timestamp_floor(__time, 'PT1M', '2012-01-02T05:00:00.000-08:00','America/Los_Angeles')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    Assertions.assertEquals(Granularities.HOUR, Granularities.fromVirtualColumn(hourly));
    Assertions.assertEquals(Granularities.DAY, Granularities.fromVirtualColumn(day));
    Assertions.assertEquals(Granularities.HOUR, Granularities.fromVirtualColumn(hourlyNonstandardTime));
    Assertions.assertEquals(
        new PeriodGranularity(new Period("PT1H"), null, DateTimes.inferTzFromString("America/Los_Angeles")),
        Granularities.fromVirtualColumn(hourlyPacificTime)
    );
    Assertions.assertEquals(
        new PeriodGranularity(new Period("PT1H"), null, DateTimes.inferTzFromString("Asia/Kolkata")),
        Granularities.fromVirtualColumn(hourlyIndianTime)
    );
    Assertions.assertNull(Granularities.fromVirtualColumn(ceilHour));
    Assertions.assertEquals(Granularities.MINUTE, Granularities.fromVirtualColumn(floorWithExpression));
    final DateTime origin = DateTimes.of("2012-01-02T05:00:00.000-08:00");
    final DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");
    final Granularity minuteWithTz = new PeriodGranularity(new Period("PT1M"), null, tz);
    final Granularity minuteWithOrigin = new PeriodGranularity(new Period("PT1M"), origin, tz);
    Assertions.assertEquals(minuteWithTz, Granularities.fromVirtualColumn(floorWithTimezone));
    Assertions.assertEquals(minuteWithOrigin, Granularities.fromVirtualColumn(floorWithOriginTimezone));
  }

  @Test
  public void testFromVirtualColumnExtra()
  {
    ExpressionVirtualColumn literalField = new ExpressionVirtualColumn(
        "v0",
        "a",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    Assertions.assertEquals(Granularities.ALL, Granularities.fromVirtualColumn(literalField));
  }

  private void assertBucketStart(final Granularity granularity, final DateTime in, final DateTime expectedInProperTz)
  {
    Assertions.assertEquals(
        expectedInProperTz,
        granularity.toDateTime(granularity.bucketStart(in).getMillis()),
        StringUtils.format("Granularity [%s] toDateTime(bucketStart(DateTime))", granularity)
    );

    Assertions.assertEquals(
        expectedInProperTz.withZone(in.getZone()),
        granularity.bucketStart(in),
        StringUtils.format("Granularity [%s] bucketStart(DateTime)", granularity)
    );

    Assertions.assertEquals(
        expectedInProperTz.getMillis(),
        granularity.bucketStart(in.getMillis()),
        StringUtils.format("Granularity [%s] bucketStart(long)", granularity)
    );
  }
}
