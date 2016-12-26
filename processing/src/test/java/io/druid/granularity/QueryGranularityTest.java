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

package io.druid.granularity;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.DurationGranularity;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.granularity.PeriodGranularity;
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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.List;

/**
 */
public class QueryGranularityTest
{
  @Test
  public void testIterableNone() throws Exception
  {
    final Iterator<Interval> iterator = Granularity.NONE.getIterable(new Interval(0, 1000)).iterator();
    int count = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(count, iterator.next().getStartMillis());
      count++;
    }
  }

  @Test
  public void testIterableMinuteSimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:38:00.000Z"),
            new DateTime("2011-01-01T09:39:00.000Z"),
            new DateTime("2011-01-01T09:40:00.000Z")
        ),
        Granularity.MINUTE.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Minutes.THREE).getMillis()))
    );
  }

  @Test
  public void testIterableMinuteComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:38:00.000Z"),
            new DateTime("2011-01-01T09:39:00.000Z"),
            new DateTime("2011-01-01T09:40:00.000Z"),
            new DateTime("2011-01-01T09:41:00.000Z")
        ),
        Granularity.MINUTE.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Minutes.THREE).getMillis()))
    );
  }

  @Test
  public void testIterable15MinuteSimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:30:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:30:00.000Z"),
            new DateTime("2011-01-01T09:45:00.000Z"),
            new DateTime("2011-01-01T10:00:00.000Z")
        ),
            Granularity.FIFTEEN_MINUTE.getIterable(
            new Interval(
                baseTime.getMillis(), baseTime.plus(Minutes.minutes(45)).getMillis()
            ))
    );
  }

  @Test
  public void testIterable15MinuteComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:30:00.000Z"),
            new DateTime("2011-01-01T09:45:00.000Z"),
            new DateTime("2011-01-01T10:00:00.000Z"),
            new DateTime("2011-01-01T10:15:00.000Z")
        ),
        Granularity.FIFTEEN_MINUTE.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Minutes.minutes(45)).getMillis()))
    );
  }

  @Test
  public void testIterableHourSimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:00:00.000Z"),
            new DateTime("2011-01-01T10:00:00.000Z"),
            new DateTime("2011-01-01T11:00:00.000Z")
        ), Granularity.HOUR.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Hours.hours(3)).getMillis()))
    );
  }

  @Test
  public void testIterableHourComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:00:00.000Z"),
            new DateTime("2011-01-01T10:00:00.000Z"),
            new DateTime("2011-01-01T11:00:00.000Z"),
            new DateTime("2011-01-01T12:00:00.000Z")
        ), Granularity.HOUR.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Hours.hours(3)).getMillis()))
    );
  }

  @Test
  public void testIterableDaySimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2011-01-02T00:00:00.000Z"),
            new DateTime("2011-01-03T00:00:00.000Z")
        ),
        Granularity.DAY.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis()))
    );
  }

  @Test
  public void testIterableDayComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2011-01-02T00:00:00.000Z"),
            new DateTime("2011-01-03T00:00:00.000Z"),
            new DateTime("2011-01-04T00:00:00.000Z")
        ),
        Granularity.DAY.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis()))
    );
  }

  @Test
  public void testIterableWeekSimple()
  {
    final DateTime baseTime = new DateTime("2011-01-03T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-03T00:00:00.000Z"),
            new DateTime("2011-01-10T00:00:00.000Z"),
            new DateTime("2011-01-17T00:00:00.000Z")
        ),
        Granularity.WEEK.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Weeks.THREE).getMillis()))
    );
  }

  @Test
  public void testIterableWeekComplex()
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2010-12-27T00:00:00.000Z"),
            new DateTime("2011-01-03T00:00:00.000Z"),
            new DateTime("2011-01-10T00:00:00.000Z"),
            new DateTime("2011-01-17T00:00:00.000Z")
        ),
        Granularity.WEEK.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Weeks.THREE).getMillis()))
    );
  }

  @Test
  public void testIterableMonthSimple()
  {
    final DateTime baseTime = new DateTime("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2011-02-01T00:00:00.000Z"),
            new DateTime("2011-03-01T00:00:00.000Z")
        ),
        Granularity.MONTH.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Months.THREE).getMillis()))
    );
  }

  @Test
  public void testIterableMonthComplex()
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2011-02-01T00:00:00.000Z"),
            new DateTime("2011-03-01T00:00:00.000Z"),
            new DateTime("2011-04-01T00:00:00.000Z")
        ),
        Granularity.MONTH.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Months.THREE).getMillis()))
    );
  }

  @Test
  public void testIterableQuarterSimple()
  {
    final DateTime baseTime = new DateTime("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2011-04-01T00:00:00.000Z"),
            new DateTime("2011-07-01T00:00:00.000Z")
        ),
        Granularity.QUARTER.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Months.NINE).getMillis()))
    );
  }

  @Test
  public void testIterableQuarterComplex()
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2011-04-01T00:00:00.000Z"),
            new DateTime("2011-07-01T00:00:00.000Z"),
            new DateTime("2011-10-01T00:00:00.000Z")
        ),
        Granularity.QUARTER.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Months.NINE).getMillis()))
    );
  }

  @Test
  public void testIterableYearSimple()
  {
    final DateTime baseTime = new DateTime("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2012-01-01T00:00:00.000Z"),
            new DateTime("2013-01-01T00:00:00.000Z")
        ),
        Granularity.YEAR.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Years.THREE).getMillis()))
    );
  }

  @Test
  public void testIterableYearComplex()
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:00.000Z");

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2012-01-01T00:00:00.000Z"),
            new DateTime("2013-01-01T00:00:00.000Z"),
            new DateTime("2014-01-01T00:00:00.000Z")
        ),
        Granularity.YEAR.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Years.THREE).getMillis()))
    );
  }

  @Test
  public void testPeriodDaylightSaving() throws Exception
  {
    final DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-04T00:00:00", tz);
    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00"),
            new DateTime("2012-11-05T00:00:00.000-08:00"),
            new DateTime("2012-11-06T00:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("P1D"), null, tz)
            .getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis()))
    );

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00"),
            new DateTime("2012-11-04T01:00:00.000-07:00"),
            new DateTime("2012-11-04T01:00:00.000-08:00"),
            new DateTime("2012-11-04T02:00:00.000-08:00"),
            new DateTime("2012-11-04T03:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("PT1H"), null, tz)
            .getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Hours.hours(5)).getMillis()))
    );

    final PeriodGranularity hour = new PeriodGranularity(new Period("PT1H"), null, tz);
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00"),
            new DateTime("2012-11-04T01:00:00.000-07:00"),
            new DateTime("2012-11-04T01:00:00.000-08:00"),
            new DateTime("2012-11-04T02:00:00.000-08:00"),
            new DateTime("2012-11-04T03:00:00.000-08:00")
        ),
        Lists.newArrayList(
          hour.truncate(new DateTime("2012-11-04T00:30:00-07:00")),
          hour.truncate(new DateTime("2012-11-04T01:30:00-07:00")),
          hour.truncate(new DateTime("2012-11-04T01:30:00-08:00")),
          hour.truncate(new DateTime("2012-11-04T02:30:00-08:00")),
          hour.truncate(new DateTime("2012-11-04T03:30:00-08:00"))
        )
    );
  }

  @Test
  public void testIterableMonth() throws Exception
  {
    final DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-03T10:00:00", tz);
    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-11-01T00:00:00.000-07:00"),
            new DateTime("2012-12-01T00:00:00.000-08:00"),
            new DateTime("2013-01-01T00:00:00.000-08:00"),
            new DateTime("2013-02-01T00:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("P1M"), null, tz)
            .getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Months.months(3)).getMillis()))
    );
  }

  @Test
  public void testIterableWeek() throws Exception
  {
    final DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-03T10:00:00", tz);
    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-10-29T00:00:00.000-07:00"),
            new DateTime("2012-11-05T00:00:00.000-08:00"),
            new DateTime("2012-11-12T00:00:00.000-08:00"),
            new DateTime("2012-11-19T00:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("P1W"), null, tz)
            .getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Weeks.weeks(3)).getMillis()))
    );

    assertSameInterval(
        Lists.newArrayList(
            new DateTime("2012-11-03T10:00:00.000-07:00"),
            new DateTime("2012-11-10T10:00:00.000-08:00"),
            new DateTime("2012-11-17T10:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("P1W"), baseTime, tz)
            .getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Weeks.weeks(3)).getMillis()))
    );
  }

  @Test
  public void testPeriodTruncateDays() throws Exception
  {
    final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
    PeriodGranularity periodOrigin = new PeriodGranularity(new Period("P2D"),
                                                          origin,
                                                          DateTimeZone.forID("America/Los_Angeles"));
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2011-12-31T05:00:00.000-08:00"),
            new DateTime("2012-01-02T05:00:00.000-08:00"),
            new DateTime("2012-01-04T05:00:00.000-08:00")
        ),
        Lists.newArrayList(
            periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00")),
            periodOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00")),
            periodOrigin.truncate(new DateTime("2012-01-04T07:20:04.123-08:00"))

        )
    );

    PeriodGranularity periodNoOrigin = new PeriodGranularity(new Period("P2D"),
                                                            null,
                                                            DateTimeZone.forID("America/Los_Angeles"));
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2012-01-01T00:00:00.000-08:00"),
            new DateTime("2012-01-01T00:00:00.000-08:00"),
            new DateTime("2012-01-03T00:00:00.000-08:00")
        ),
        Lists.newArrayList(
            periodNoOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00")),
            periodNoOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00")),
            periodNoOrigin.truncate(new DateTime("2012-01-04T07:20:04.123-08:00"))

        )
    );
  }

  @Test
  public void testPeriodTruncateMinutes() throws Exception
  {
    final DateTime origin = new DateTime("2012-01-02T00:05:00.000Z");
    PeriodGranularity periodOrigin = new PeriodGranularity(new Period("PT15M"), origin, null);
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2012-01-01T04:50:00.000Z"),
            new DateTime("2012-01-02T07:05:00.000Z"),
            new DateTime("2012-01-04T00:20:00.000Z")
        ),
        Lists.newArrayList(
            periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123Z")),
            periodOrigin.truncate(new DateTime("2012-01-02T07:08:04.123Z")),
            periodOrigin.truncate(new DateTime("2012-01-04T00:20:04.123Z"))

        )
    );

    PeriodGranularity periodNoOrigin = new PeriodGranularity(new Period("PT15M"), null, null);
    assertSameDateTime(
        Lists.newArrayList(
            new DateTime("2012-01-01T05:00:00.000Z"),
            new DateTime("2012-01-02T07:00:00.000Z"),
            new DateTime("2012-01-04T00:15:00.000Z")
        ),
        Lists.newArrayList(
            periodNoOrigin.truncate(new DateTime("2012-01-01T05:00:04.123Z")),
            periodNoOrigin.truncate(new DateTime("2012-01-02T07:00:04.123Z")),
            periodNoOrigin.truncate(new DateTime("2012-01-04T00:20:04.123Z"))

        )
    );
  }

  @Test
  public void testCompoundPeriodTruncate() throws Exception
  {
    {
      final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
      PeriodGranularity periodOrigin = new PeriodGranularity(
          new Period("P1M2D"),
          origin,
          DateTimeZone.forID("America/Los_Angeles")
      );
      assertSameDateTime(
          Lists.newArrayList(
              new DateTime("2011-11-30T05:00:00.000-08:00"),
              new DateTime("2012-01-02T05:00:00.000-08:00"),
              new DateTime("2012-02-04T05:00:00.000-08:00"),
              new DateTime("2012-02-04T05:00:00.000-08:00")
          ),
          Lists.newArrayList(
              periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-03-01T07:20:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-02-04T05:00:00.000-08:00"))
          )
      );

      PeriodGranularity periodNoOrigin = new PeriodGranularity(
          new Period("P1M2D"),
          null,
          DateTimeZone.forID("America/Los_Angeles")
      );
      assertSameDateTime(
          Lists.newArrayList(
              new DateTime("1970-01-01T00:00:00.000-08:00"),
              new DateTime("2011-12-12T00:00:00.000-08:00"),
              new DateTime("2012-01-14T00:00:00.000-08:00"),
              new DateTime("2012-02-16T00:00:00.000-08:00")
          ),
          Lists.newArrayList(
              periodNoOrigin.truncate(new DateTime("1970-01-01T05:02:04.123-08:00")),
              periodNoOrigin.truncate(new DateTime("2012-01-01T05:02:04.123-08:00")),
              periodNoOrigin.truncate(new DateTime("2012-01-15T07:01:04.123-08:00")),
              periodNoOrigin.truncate(new DateTime("2012-02-16T00:00:00.000-08:00"))

          )
      );
    }

    {
      final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
      PeriodGranularity periodOrigin = new PeriodGranularity(
          new Period("PT12H5M"),
          origin,
          DateTimeZone.forID("America/Los_Angeles")
      );
      assertSameDateTime(
          Lists.newArrayList(
              new DateTime("2012-01-01T04:50:00.000-08:00"),
              new DateTime("2012-01-02T05:00:00.000-08:00"),
              new DateTime("2012-01-02T17:05:00.000-08:00"),
              new DateTime("2012-02-03T22:25:00.000-08:00")
          ),
          Lists.newArrayList(
              periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-01-03T00:20:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-02-03T22:25:00.000-08:00"))
          )
      );
    }
  }

  @Test
  public void testCompoundPeriodMillisTruncate() throws Exception
  {
    {
      final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
      PeriodGranularity periodOrigin = new PeriodGranularity(
          new Period("PT12H5M"),
          origin,
          DateTimeZone.UTC
      );
      assertSameDateTime(
          Lists.newArrayList(
              new DateTime("2012-01-01T04:50:00.000-08:00"),
              new DateTime("2012-01-02T05:00:00.000-08:00"),
              new DateTime("2012-01-02T17:05:00.000-08:00"),
              new DateTime("2012-02-03T22:25:00.000-08:00")
          ),
          Lists.newArrayList(
              periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-01-03T00:20:04.123-08:00")),
              periodOrigin.truncate(new DateTime("2012-02-03T22:25:00.000-08:00"))
          )
      );
    }
  }

  @Test
  public void testDurationTruncate() throws Exception
  {
    {
      final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
      Granularity gran = new DurationGranularity(
          new Period("PT12H5M").toStandardDuration().getMillis(),
          origin
      );
      assertSameDateTime(
          Lists.newArrayList(
              new DateTime("2012-01-01T04:50:00.000-08:00"),
              new DateTime("2012-01-02T05:00:00.000-08:00"),
              new DateTime("2012-01-02T17:05:00.000-08:00"),
              new DateTime("2012-02-03T22:25:00.000-08:00")
          ),
          Lists.newArrayList(
              gran.truncate(new DateTime("2012-01-01T05:00:04.123-08:00")),
              gran.truncate(new DateTime("2012-01-02T07:00:04.123-08:00")),
              gran.truncate(new DateTime("2012-01-03T00:20:04.123-08:00")),
              gran.truncate(new DateTime("2012-02-03T22:25:00.000-08:00"))
          )
      );
    }
  }


  @Test
  public void testIterableAllSimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T00:00:00.000Z");

    assertSameInterval(
        Lists.newArrayList(baseTime),
        Granularity.ALL.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis()))
    );
  }

  @Test
  public void testIterableAllComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSameInterval(
        Lists.newArrayList(baseTime),
        Granularity.ALL.getIterable(new Interval(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis()))
    );
  }

  @Test
  public void testSerializePeriod() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    String json = "{ \"type\": \"period\", \"period\": \"P1D\" }";
    Granularity gran = mapper.readValue(json, Granularity.class);
    Assert.assertEquals(new PeriodGranularity(new Period("P1D"), null, null), gran);

    json =   "{ \"type\": \"period\", \"period\": \"P1D\","
           + "\"timeZone\": \"America/Los_Angeles\", \"origin\": \"1970-01-01T00:00:00Z\"}";
    gran = mapper.readValue(json, Granularity.class);
    Assert.assertEquals(new PeriodGranularity(new Period("P1D"), new DateTime(0L), DateTimeZone.forID("America/Los_Angeles")), gran);

    PeriodGranularity expected = new PeriodGranularity(
        new Period("P1D"),
        new DateTime("2012-01-01"),
        DateTimeZone.forID("America/Los_Angeles")
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
  public void testSerializeSimple() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    Assert.assertEquals(
        Granularity.ALL,
        mapper.readValue(
        mapper.writeValueAsString(Granularity.ALL),
        Granularity.class
      )
    );

    Assert.assertEquals(
        Granularity.NONE,
        mapper.readValue(
        mapper.writeValueAsString(Granularity.NONE),
        Granularity.class
      )
    );
  }

  @Test
  public void testDeserializeSimple() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    Assert.assertEquals(Granularity.ALL, mapper.readValue("\"all\"", Granularity.class));
    Assert.assertEquals(Granularity.ALL, mapper.readValue("\"ALL\"", Granularity.class));
    Assert.assertEquals(Granularity.NONE, mapper.readValue("\"none\"", Granularity.class));
    Assert.assertEquals(Granularity.NONE, mapper.readValue("\"NONE\"", Granularity.class));

    Assert.assertEquals(Granularity.DAY, mapper.readValue("\"day\"", Granularity.class));
    Assert.assertEquals(Granularity.HOUR, mapper.readValue("\"hour\"", Granularity.class));
    Assert.assertEquals(Granularity.MINUTE, mapper.readValue("\"minute\"", Granularity.class));
    Assert.assertEquals(Granularity.FIFTEEN_MINUTE, mapper.readValue("\"fifteen_minute\"", Granularity.class));

    Assert.assertEquals(Granularity.WEEK, mapper.readValue("\"week\"", Granularity.class));
    Assert.assertEquals(Granularity.QUARTER, mapper.readValue("\"quarter\"", Granularity.class));
    Assert.assertEquals(Granularity.MONTH, mapper.readValue("\"month\"", Granularity.class));
    Assert.assertEquals(Granularity.YEAR, mapper.readValue("\"year\"", Granularity.class));
  }

  @Test
  public void testMerge()
  {
    Assert.assertNull(Granularity.mergeGranularities(null));
    Assert.assertNull(Granularity.mergeGranularities(ImmutableList.<Granularity>of()));
    Assert.assertNull(Granularity.mergeGranularities(Lists.newArrayList(null, Granularity.DAY)));
    Assert.assertNull(Granularity.mergeGranularities(Lists.newArrayList(Granularity.DAY, null)));
    Assert.assertNull(
            Granularity.mergeGranularities(
            Lists.newArrayList(
                Granularity.DAY,
                null,
                Granularity.DAY
            )
        )
    );
    Assert.assertNull(
            Granularity.mergeGranularities(ImmutableList.of(Granularity.ALL, Granularity.DAY))
    );

    Assert.assertEquals(
        Granularity.ALL,
        Granularity.mergeGranularities(ImmutableList.of(Granularity.ALL, Granularity.ALL))
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

  @Test(timeout = 60_000L)
  public void testDeadLock() throws Exception
  {
    final URL[] urls = ((URLClassLoader)Granularity.class.getClassLoader()).getURLs();
    final String className = Granularity.class.getCanonicalName();
    for(int i = 0; i < 1000; ++i) {
      final ClassLoader loader = new URLClassLoader(urls, null);
      Assert.assertNotNull(Class.forName(className, true, loader));
    }
  }
}
