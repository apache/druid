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
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

/**
 */
public class QueryGranularityTest
{
  @Test
  public void testIterableNone()
  {
    final Iterator<Interval> iterator = Granularities.NONE.getIterable(Intervals.utc(0, 1000)).iterator();
    int count = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(count, iterator.next().getStartMillis());
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
  public void testDurationTruncate()
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

    Assert.assertEquals(
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
    Assert.assertEquals(new PeriodGranularity(new Period("P1D"), null, null), gran);

    // Nonstandard period
    json = "{ \"type\": \"period\", \"period\": \"P2D\" }";
    gran = mapper.readValue(json, Granularity.class);
    Assert.assertEquals(new PeriodGranularity(new Period("P2D"), null, null), gran);

    // Set timeZone, origin
    json = "{ \"type\": \"period\", \"period\": \"P1D\","
           + "\"timeZone\": \"America/Los_Angeles\", \"origin\": \"1970-01-01T00:00:00Z\"}";
    gran = mapper.readValue(json, Granularity.class);
    Assert.assertEquals(new PeriodGranularity(
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
    Assert.assertEquals(expected, mapper.readValue(jsonOut, Granularity.class));

    String illegalJson = "{ \"type\": \"period\", \"period\": \"P0D\" }";
    try {
      mapper.readValue(illegalJson, Granularity.class);
      Assert.fail();
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
    Assert.assertEquals(new DurationGranularity(3600000, null), gran);

    json = "{ \"type\": \"duration\", \"duration\": \"5\", \"origin\": \"2012-09-01T00:00:00.002Z\" }";
    gran = mapper.readValue(json, Granularity.class);
    Assert.assertEquals(new DurationGranularity(5, 2), gran);

    DurationGranularity expected = new DurationGranularity(5, 2);
    Assert.assertEquals(expected, mapper.readValue(mapper.writeValueAsString(expected), Granularity.class));

    String illegalJson = "{ \"type\": \"duration\", \"duration\": \"0\" }";
    try {
      mapper.readValue(illegalJson, Granularity.class);
      Assert.fail();
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

      Assert.assertEquals(
          granularity,
          mapper.readValue("\"" + StringUtils.toUpperCase(granularityType.name()) + "\"", Granularity.class)
      );

      Assert.assertEquals(
          granularity,
          mapper.readValue("\"" + StringUtils.toLowerCase(granularityType.name()) + "\"", Granularity.class)
      );

      Assert.assertEquals(
          granularity,
          mapper.readValue(mapper.writeValueAsString(granularity), Granularity.class)
      );

      if (granularityType == GranularityType.ALL || granularityType == GranularityType.NONE) {
        Assert.assertEquals(
            "{\"type\":\"" + StringUtils.toLowerCase(granularityType.name()) + "\"}",
            mapper.writeValueAsString(granularity)
        );
      } else {
        Assert.assertEquals(
            "\"" + StringUtils.toUpperCase(granularityType.name()) + "\"",
            mapper.writeValueAsString(granularity)
        );
      }
    }
  }

  @Test
  public void testMerge()
  {
    Assert.assertNull(Granularity.mergeGranularities(null));
    Assert.assertNull(Granularity.mergeGranularities(ImmutableList.of()));
    Assert.assertNull(Granularity.mergeGranularities(Lists.newArrayList(null, Granularities.DAY)));
    Assert.assertNull(Granularity.mergeGranularities(Lists.newArrayList(Granularities.DAY, null)));
    Assert.assertNull(
        Granularity.mergeGranularities(
            Lists.newArrayList(
                Granularities.DAY,
                null,
                Granularities.DAY
            )
        )
    );
    Assert.assertNull(
        Granularity.mergeGranularities(ImmutableList.of(Granularities.ALL, Granularities.DAY))
    );

    Assert.assertEquals(
        Granularities.ALL,
        Granularity.mergeGranularities(ImmutableList.of(Granularities.ALL, Granularities.ALL))
    );
  }

  private void assertSameDateTime(List<DateTime> expected, Iterable<DateTime> actual)
  {
    Assert.assertEquals(expected.size(), Iterables.size(actual));
    Iterator<DateTime> actualIter = actual.iterator();
    Iterator<DateTime> expectedIter = expected.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      Assert.assertEquals(expectedIter.next(), actualIter.next());
    }
    Assert.assertFalse("actualIter not exhausted!?", actualIter.hasNext());
    Assert.assertFalse("expectedIter not exhausted!?", expectedIter.hasNext());
  }

  private void assertSameInterval(List<DateTime> expected, Iterable<Interval> actual)
  {
    Assert.assertEquals(expected.size(), Iterables.size(actual));
    Iterator<Interval> actualIter = actual.iterator();
    Iterator<DateTime> expectedIter = expected.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      Assert.assertEquals(expectedIter.next(), actualIter.next().getStart());
    }
    Assert.assertFalse("actualIter not exhausted!?", actualIter.hasNext());
    Assert.assertFalse("expectedIter not exhausted!?", expectedIter.hasNext());
  }
  
  @Test
  public void testTruncateKathmandu()
  {
    final DateTimeZone tz = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Kathmandu"));
    final DateTime date = new DateTime("2011-03-15T21:42:23.898+05:45", tz);
    final PeriodGranularity year = new PeriodGranularity(new Period("P1Y"), null, tz);
    final PeriodGranularity hour = new PeriodGranularity(new Period("PT1H"), null, tz);
    final PeriodGranularity twoHour = new PeriodGranularity(new Period("PT2H"), null, tz);

    Assert.assertEquals(
        new DateTime("2011-01-01T00:00:00.000+05:45", tz),
        year.toDateTime(year.bucketStart(date).getMillis())
    );
    Assert.assertEquals(
        new DateTime("2011-03-15T21:00:00.000+05:45", tz),
        hour.toDateTime(hour.bucketStart(date).getMillis())
    );

    Assert.assertEquals(
        new DateTime("2011-03-15T20:00:00.000+05:45", tz),
        twoHour.toDateTime(twoHour.bucketStart(date).getMillis())
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

    Assert.assertEquals(
        new DateTime("2011-01-01T00:00:00.000+06:00", tz),
        year.toDateTime(year.bucketStart(date).getMillis())
    );
    Assert.assertEquals(
        new DateTime("2011-03-15T21:00:00.000+06:00", tz),
        hour.toDateTime(hour.bucketStart(date).getMillis())
    );

    Assert.assertEquals(
        new DateTime("2011-03-15T20:00:00.000+06:00", tz),
        twoHour.toDateTime(twoHour.bucketStart(date).getMillis())
    );
  }
  

}
