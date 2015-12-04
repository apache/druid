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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Period;
import org.joda.time.Weeks;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class QueryGranularityTest
{
  @Test
  public void testIterableNone() throws Exception
  {
    List<Long> millis = Lists.newArrayList(QueryGranularity.NONE.iterable(0, 1000));
    int count = 0;
    Assert.assertEquals(1000, millis.size());
    for (Long milli : millis) {
      Assert.assertEquals(count, milli.longValue());
      ++count;
    }
  }

  @Test
  public void testIterableMinuteSimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:00.000Z");

    assertSame(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:38:00.000Z"),
            new DateTime("2011-01-01T09:39:00.000Z"),
            new DateTime("2011-01-01T09:40:00.000Z")
        ),
        QueryGranularity.MINUTE.iterable(baseTime.getMillis(), baseTime.plus(Minutes.THREE).getMillis())
    );
  }

  @Test
  public void testIterableMinuteComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSame(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:38:00.000Z"),
            new DateTime("2011-01-01T09:39:00.000Z"),
            new DateTime("2011-01-01T09:40:00.000Z"),
            new DateTime("2011-01-01T09:41:00.000Z")
        ),
        QueryGranularity.MINUTE.iterable(baseTime.getMillis(), baseTime.plus(Minutes.THREE).getMillis())
    );
  }

  @Test
  public void testIterable15MinuteSimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:30:00.000Z");

    assertSame(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:30:00.000Z"),
            new DateTime("2011-01-01T09:45:00.000Z"),
            new DateTime("2011-01-01T10:00:00.000Z")
        ),
        QueryGranularity.fromString("FIFTEEN_MINUTE").iterable(
            baseTime.getMillis(),
            baseTime.plus(Minutes.minutes(45)).getMillis()
        )
    );
  }

  @Test
  public void testIterable15MinuteComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSame(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:30:00.000Z"),
            new DateTime("2011-01-01T09:45:00.000Z"),
            new DateTime("2011-01-01T10:00:00.000Z"),
            new DateTime("2011-01-01T10:15:00.000Z")
        ),
        QueryGranularity.fromString("FIFTEEN_MINUTE").iterable(baseTime.getMillis(), baseTime.plus(Minutes.minutes(45)).getMillis())
    );
  }

  @Test
  public void testIterableHourSimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:00:00.000Z");

    assertSame(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:00:00.000Z"),
            new DateTime("2011-01-01T10:00:00.000Z"),
            new DateTime("2011-01-01T11:00:00.000Z")
        ),
        QueryGranularity.HOUR.iterable(baseTime.getMillis(), baseTime.plus(Hours.hours(3)).getMillis())
    );
  }

  @Test
  public void testIterableHourComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSame(
        Lists.newArrayList(
            new DateTime("2011-01-01T09:00:00.000Z"),
            new DateTime("2011-01-01T10:00:00.000Z"),
            new DateTime("2011-01-01T11:00:00.000Z"),
            new DateTime("2011-01-01T12:00:00.000Z")
        ),
        QueryGranularity.HOUR.iterable(baseTime.getMillis(), baseTime.plus(Hours.hours(3)).getMillis())
    );
  }

  @Test
  public void testIterableDaySimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T00:00:00.000Z");

    assertSame(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2011-01-02T00:00:00.000Z"),
            new DateTime("2011-01-03T00:00:00.000Z")
        ),
        QueryGranularity.DAY.iterable(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis())
    );
  }

  @Test
  public void testIterableDayComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSame(
        Lists.newArrayList(
            new DateTime("2011-01-01T00:00:00.000Z"),
            new DateTime("2011-01-02T00:00:00.000Z"),
            new DateTime("2011-01-03T00:00:00.000Z"),
            new DateTime("2011-01-04T00:00:00.000Z")
        ),
        QueryGranularity.DAY.iterable(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis())
    );
  }

  @Test
  public void testPeriodDaylightSaving() throws Exception
  {
    final DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-04T00:00:00", tz);
    assertSame(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00"),
            new DateTime("2012-11-05T00:00:00.000-08:00"),
            new DateTime("2012-11-06T00:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("P1D"), null, tz)
            .iterable(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis())
    );

    assertSame(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00"),
            new DateTime("2012-11-04T01:00:00.000-07:00"),
            new DateTime("2012-11-04T01:00:00.000-08:00"),
            new DateTime("2012-11-04T02:00:00.000-08:00"),
            new DateTime("2012-11-04T03:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("PT1H"), null, tz)
            .iterable(baseTime.getMillis(), baseTime.plus(Hours.hours(5)).getMillis())
    );

    final QueryGranularity hour = new PeriodGranularity(new Period("PT1H"), null, tz);
    assertSame(
        Lists.newArrayList(
            new DateTime("2012-11-04T00:00:00.000-07:00"),
            new DateTime("2012-11-04T01:00:00.000-07:00"),
            new DateTime("2012-11-04T01:00:00.000-08:00"),
            new DateTime("2012-11-04T02:00:00.000-08:00"),
            new DateTime("2012-11-04T03:00:00.000-08:00")
        ),
        Lists.newArrayList(
          hour.truncate(new DateTime("2012-11-04T00:30:00-07:00").getMillis()),
          hour.truncate(new DateTime("2012-11-04T01:30:00-07:00").getMillis()),
          hour.truncate(new DateTime("2012-11-04T01:30:00-08:00").getMillis()),
          hour.truncate(new DateTime("2012-11-04T02:30:00-08:00").getMillis()),
          hour.truncate(new DateTime("2012-11-04T03:30:00-08:00").getMillis())
        )
    );
  }

  @Test
  public void testIterableMonth() throws Exception
  {
    final DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-03T10:00:00", tz);
    assertSame(
        Lists.newArrayList(
            new DateTime("2012-11-01T00:00:00.000-07:00"),
            new DateTime("2012-12-01T00:00:00.000-08:00"),
            new DateTime("2013-01-01T00:00:00.000-08:00"),
            new DateTime("2013-02-01T00:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("P1M"), null, tz)
            .iterable(baseTime.getMillis(), baseTime.plus(Months.months(3)).getMillis())
    );
  }

  @Test
  public void testIterableWeek() throws Exception
  {
    final DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");
    final DateTime baseTime = new DateTime("2012-11-03T10:00:00", tz);
    assertSame(
        Lists.newArrayList(
            new DateTime("2012-10-29T00:00:00.000-07:00"),
            new DateTime("2012-11-05T00:00:00.000-08:00"),
            new DateTime("2012-11-12T00:00:00.000-08:00"),
            new DateTime("2012-11-19T00:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("P1W"), null, tz)
            .iterable(baseTime.getMillis(), baseTime.plus(Weeks.weeks(3)).getMillis())
    );

    assertSame(
        Lists.newArrayList(
            new DateTime("2012-11-03T10:00:00.000-07:00"),
            new DateTime("2012-11-10T10:00:00.000-08:00"),
            new DateTime("2012-11-17T10:00:00.000-08:00")
        ),
        new PeriodGranularity(new Period("P1W"), baseTime, tz)
            .iterable(baseTime.getMillis(), baseTime.plus(Weeks.weeks(3)).getMillis())
    );
  }

  @Test
  public void testPeriodTruncateDays() throws Exception
  {
    final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
    QueryGranularity periodOrigin = new PeriodGranularity(new Period("P2D"),
                                                          origin,
                                                          DateTimeZone.forID("America/Los_Angeles"));
    assertSame(
        Lists.newArrayList(
            new DateTime("2011-12-31T05:00:00.000-08:00"),
            new DateTime("2012-01-02T05:00:00.000-08:00"),
            new DateTime("2012-01-04T05:00:00.000-08:00")
        ),
        Lists.newArrayList(
            periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00").getMillis()),
            periodOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00").getMillis()),
            periodOrigin.truncate(new DateTime("2012-01-04T07:20:04.123-08:00").getMillis())

        )
    );

    QueryGranularity periodNoOrigin = new PeriodGranularity(new Period("P2D"),
                                                            null,
                                                            DateTimeZone.forID("America/Los_Angeles"));
    assertSame(
        Lists.newArrayList(
            new DateTime("2012-01-01T00:00:00.000-08:00"),
            new DateTime("2012-01-01T00:00:00.000-08:00"),
            new DateTime("2012-01-03T00:00:00.000-08:00")
        ),
        Lists.newArrayList(
            periodNoOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00").getMillis()),
            periodNoOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00").getMillis()),
            periodNoOrigin.truncate(new DateTime("2012-01-04T07:20:04.123-08:00").getMillis())

        )
    );
  }

  @Test
  public void testPeriodTruncateMinutes() throws Exception
  {
    final DateTime origin = new DateTime("2012-01-02T00:05:00.000Z");
    QueryGranularity periodOrigin = new PeriodGranularity(new Period("PT15M"), origin, null);
    assertSame(
        Lists.newArrayList(
            new DateTime("2012-01-01T04:50:00.000Z"),
            new DateTime("2012-01-02T07:05:00.000Z"),
            new DateTime("2012-01-04T00:20:00.000Z")
        ),
        Lists.newArrayList(
            periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123Z").getMillis()),
            periodOrigin.truncate(new DateTime("2012-01-02T07:08:04.123Z").getMillis()),
            periodOrigin.truncate(new DateTime("2012-01-04T00:20:04.123Z").getMillis())

        )
    );

    QueryGranularity periodNoOrigin = new PeriodGranularity(new Period("PT15M"), null, null);
    assertSame(
        Lists.newArrayList(
            new DateTime("2012-01-01T05:00:00.000Z"),
            new DateTime("2012-01-02T07:00:00.000Z"),
            new DateTime("2012-01-04T00:15:00.000Z")
        ),
        Lists.newArrayList(
            periodNoOrigin.truncate(new DateTime("2012-01-01T05:00:04.123Z").getMillis()),
            periodNoOrigin.truncate(new DateTime("2012-01-02T07:00:04.123Z").getMillis()),
            periodNoOrigin.truncate(new DateTime("2012-01-04T00:20:04.123Z").getMillis())

        )
    );
  }

  @Test
  public void testCompoundPeriodTruncate() throws Exception
  {
    {
      final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
      QueryGranularity periodOrigin = new PeriodGranularity(
          new Period("P1M2D"),
          origin,
          DateTimeZone.forID("America/Los_Angeles")
      );
      assertSame(
          Lists.newArrayList(
              new DateTime("2011-11-30T05:00:00.000-08:00"),
              new DateTime("2012-01-02T05:00:00.000-08:00"),
              new DateTime("2012-02-04T05:00:00.000-08:00"),
              new DateTime("2012-02-04T05:00:00.000-08:00")
          ),
          Lists.newArrayList(
              periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-03-01T07:20:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-02-04T05:00:00.000-08:00").getMillis())
          )
      );

      QueryGranularity periodNoOrigin = new PeriodGranularity(
          new Period("P1M2D"),
          null,
          DateTimeZone.forID("America/Los_Angeles")
      );
      assertSame(
          Lists.newArrayList(
              new DateTime("1970-01-01T00:00:00.000-08:00"),
              new DateTime("2011-12-12T00:00:00.000-08:00"),
              new DateTime("2012-01-14T00:00:00.000-08:00"),
              new DateTime("2012-02-16T00:00:00.000-08:00")
          ),
          Lists.newArrayList(
              periodNoOrigin.truncate(new DateTime("1970-01-01T05:02:04.123-08:00").getMillis()),
              periodNoOrigin.truncate(new DateTime("2012-01-01T05:02:04.123-08:00").getMillis()),
              periodNoOrigin.truncate(new DateTime("2012-01-15T07:01:04.123-08:00").getMillis()),
              periodNoOrigin.truncate(new DateTime("2012-02-16T00:00:00.000-08:00").getMillis())

          )
      );
    }

    {
      final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
      QueryGranularity periodOrigin = new PeriodGranularity(
          new Period("PT12H5M"),
          origin,
          DateTimeZone.forID("America/Los_Angeles")
      );
      assertSame(
          Lists.newArrayList(
              new DateTime("2012-01-01T04:50:00.000-08:00"),
              new DateTime("2012-01-02T05:00:00.000-08:00"),
              new DateTime("2012-01-02T17:05:00.000-08:00"),
              new DateTime("2012-02-03T22:25:00.000-08:00")
          ),
          Lists.newArrayList(
              periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-01-03T00:20:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-02-03T22:25:00.000-08:00").getMillis())
          )
      );
    }
  }

  @Test
  public void testCompoundPeriodMillisTruncate() throws Exception
  {
    {
      final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
      QueryGranularity periodOrigin = new PeriodGranularity(
          new Period("PT12H5M"),
          origin,
          DateTimeZone.UTC
      );
      assertSame(
          Lists.newArrayList(
              new DateTime("2012-01-01T04:50:00.000-08:00"),
              new DateTime("2012-01-02T05:00:00.000-08:00"),
              new DateTime("2012-01-02T17:05:00.000-08:00"),
              new DateTime("2012-02-03T22:25:00.000-08:00")
          ),
          Lists.newArrayList(
              periodOrigin.truncate(new DateTime("2012-01-01T05:00:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-01-02T07:00:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-01-03T00:20:04.123-08:00").getMillis()),
              periodOrigin.truncate(new DateTime("2012-02-03T22:25:00.000-08:00").getMillis())
          )
      );
    }
  }

  @Test
  public void testDurationTruncate() throws Exception
  {
    {
      final DateTime origin = new DateTime("2012-01-02T05:00:00.000-08:00");
      QueryGranularity gran = new DurationGranularity(
          new Period("PT12H5M").toStandardDuration().getMillis(),
          origin
      );
      assertSame(
          Lists.newArrayList(
              new DateTime("2012-01-01T04:50:00.000-08:00"),
              new DateTime("2012-01-02T05:00:00.000-08:00"),
              new DateTime("2012-01-02T17:05:00.000-08:00"),
              new DateTime("2012-02-03T22:25:00.000-08:00")
          ),
          Lists.newArrayList(
              gran.truncate(new DateTime("2012-01-01T05:00:04.123-08:00").getMillis()),
              gran.truncate(new DateTime("2012-01-02T07:00:04.123-08:00").getMillis()),
              gran.truncate(new DateTime("2012-01-03T00:20:04.123-08:00").getMillis()),
              gran.truncate(new DateTime("2012-02-03T22:25:00.000-08:00").getMillis())
          )
      );
    }
  }


  @Test
  public void testIterableAllSimple() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T00:00:00.000Z");

    assertSame(
        Lists.newArrayList(baseTime),
        QueryGranularity.ALL.iterable(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis())
    );
  }

  @Test
  public void testIterableAllComplex() throws Exception
  {
    final DateTime baseTime = new DateTime("2011-01-01T09:38:02.992Z");

    assertSame(
        Lists.newArrayList(baseTime),
        QueryGranularity.ALL.iterable(baseTime.getMillis(), baseTime.plus(Days.days(3)).getMillis())
    );
  }

  @Test
  public void testSerializePeriod() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    String json = "{ \"type\": \"period\", \"period\": \"P1D\" }";
    QueryGranularity gran = mapper.readValue(json, QueryGranularity.class);
    Assert.assertEquals(new PeriodGranularity(new Period("P1D"), null, null), gran);

    json =   "{ \"type\": \"period\", \"period\": \"P1D\","
           + "\"timeZone\": \"America/Los_Angeles\", \"origin\": \"1970-01-01T00:00:00Z\"}";
    gran = mapper.readValue(json, QueryGranularity.class);
    Assert.assertEquals(new PeriodGranularity(new Period("P1D"), new DateTime(0L), DateTimeZone.forID("America/Los_Angeles")), gran);

    QueryGranularity expected = new PeriodGranularity(
        new Period("P1D"),
        new DateTime("2012-01-01"),
        DateTimeZone.forID("America/Los_Angeles")
    );

    String jsonOut = mapper.writeValueAsString(expected);
    Assert.assertEquals(expected, mapper.readValue(jsonOut, QueryGranularity.class));

  }

  @Test
  public void testSerializeDuration() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    String json = "{ \"type\": \"duration\", \"duration\": \"3600000\" }";
    QueryGranularity gran = mapper.readValue(json, QueryGranularity.class);
    Assert.assertEquals(new DurationGranularity(3600000, null), gran);

    json = "{ \"type\": \"duration\", \"duration\": \"5\", \"origin\": \"2012-09-01T00:00:00.002Z\" }";
    gran = mapper.readValue(json, QueryGranularity.class);
    Assert.assertEquals(new DurationGranularity(5, 2), gran);

    DurationGranularity expected = new DurationGranularity(5, 2);
    Assert.assertEquals(expected, mapper.readValue(mapper.writeValueAsString(expected), QueryGranularity.class));
  }

  @Test
  public void testSerializeSimple() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    Assert.assertEquals(
      QueryGranularity.ALL,
      mapper.readValue(
        mapper.writeValueAsString(QueryGranularity.ALL),
        QueryGranularity.class
      )
    );

    Assert.assertEquals(
      QueryGranularity.NONE,
      mapper.readValue(
        mapper.writeValueAsString(QueryGranularity.NONE),
        QueryGranularity.class
      )
    );
  }

  @Test
  public void testDeserializeSimple() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    Assert.assertEquals(QueryGranularity.ALL, mapper.readValue("\"all\"", QueryGranularity.class));
    Assert.assertEquals(QueryGranularity.ALL, mapper.readValue("\"ALL\"", QueryGranularity.class));
    Assert.assertEquals(QueryGranularity.NONE, mapper.readValue("\"none\"", QueryGranularity.class));
    Assert.assertEquals(QueryGranularity.NONE, mapper.readValue("\"NONE\"", QueryGranularity.class));

    Assert.assertEquals(QueryGranularity.DAY, mapper.readValue("\"day\"", QueryGranularity.class));
    Assert.assertEquals(QueryGranularity.HOUR, mapper.readValue("\"hour\"", QueryGranularity.class));
    Assert.assertEquals(QueryGranularity.MINUTE, mapper.readValue("\"minute\"", QueryGranularity.class));

    QueryGranularity gran = mapper.readValue("\"thirty_minute\"", QueryGranularity.class);
    Assert.assertEquals(new DurationGranularity(30 * 60 * 1000, null), gran);

    gran = mapper.readValue("\"fifteen_minute\"", QueryGranularity.class);
    Assert.assertEquals(new DurationGranularity(15 * 60 * 1000, null), gran);
  }

  private void assertSame(List<DateTime> expected, Iterable<Long> actual)
  {
    Assert.assertEquals(expected.size(), Iterables.size(actual));
    Iterator<Long> actualIter = actual.iterator();
    Iterator<DateTime> expectedIter = expected.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      long a = actualIter.next().longValue();
      Assert.assertEquals(expectedIter.next(), new DateTime(a));
    }
    Assert.assertFalse("actualIter not exhausted!?", actualIter.hasNext());
    Assert.assertFalse("expectedIter not exhausted!?", expectedIter.hasNext());
  }

  @Test
  public void testComparing() throws Exception
  {
    Assert.assertTrue(QueryGranularity.ALL.compare(QueryGranularity.ALL) == 0);
    Assert.assertTrue(QueryGranularity.NONE.compare(QueryGranularity.NONE) == 0);
    Assert.assertTrue(QueryGranularity.ALL.compare(QueryGranularity.NONE) > 0);
    Assert.assertTrue(QueryGranularity.NONE.compare(QueryGranularity.ALL) < 0);

    DurationGranularity d1 = new DurationGranularity(10 * 60 * 1000, null);
    DurationGranularity d2 = new DurationGranularity(15 * 60 * 1000, null);
    DurationGranularity d3 = new DurationGranularity(20 * 60 * 1000, null);
    DurationGranularity d4 = new DurationGranularity(30 * 60 * 1000, new DateTime("2010-01-01T01:00:00").getMillis());
    DurationGranularity d5 = new DurationGranularity(30 * 60 * 1000, new DateTime("2010-01-01T01:00:01").getMillis());

    for (DurationGranularity d : Arrays.asList(d1, d2, d3, d4, d5)) {
      Assert.assertTrue(QueryGranularity.ALL.compare(d) > 0);
      Assert.assertTrue(QueryGranularity.NONE.compare(d) < 0);
      Assert.assertTrue(d.compare(QueryGranularity.ALL) < 0);
      Assert.assertTrue(d.compare(QueryGranularity.NONE) > 0);
    }

    Assert.assertTrue(d1.compare(d3) < 0);
    Assert.assertTrue(d3.compare(d1) > 0);
    Assert.assertTrue(d1.compare(d4) < 0);
    Assert.assertTrue(d4.compare(d1) > 0);
    Assert.assertTrue(d2.compare(d4) < 0);
    Assert.assertTrue(d4.compare(d2) > 0);
    Assert.assertNull(d1.compare(d2));
    Assert.assertNull(d2.compare(d1));
    Assert.assertNull(d2.compare(d3));
    Assert.assertNull(d3.compare(d2));
    Assert.assertNull(d1.compare(d5));
    Assert.assertNull(d5.compare(d1));

    PeriodGranularity p1 = new PeriodGranularity(new Period("PT10M"), null, null);
    PeriodGranularity p2 = new PeriodGranularity(new Period("PT15M"), null, null);
    PeriodGranularity p3 = new PeriodGranularity(new Period("PT30M"), null, null);
    PeriodGranularity p4 = new PeriodGranularity(new Period("PT30M"), new DateTime("2010-01-01T01:00:00"), null);
    PeriodGranularity p5 = new PeriodGranularity(new Period("PT1H"), null, null);
    PeriodGranularity p6 = new PeriodGranularity(new Period("P1D"), null, null);
    PeriodGranularity p7 = new PeriodGranularity(new Period("P1W"), null, null);
    PeriodGranularity p8 = new PeriodGranularity(new Period("P1WT30M"), null, null);
    PeriodGranularity p9 = new PeriodGranularity(new Period("P1M"), null, null);

    for (PeriodGranularity p : Arrays.asList(p1, p2, p3, p4, p5, p6, p7, p8, p9)) {
      Assert.assertTrue(QueryGranularity.ALL.compare(p) > 0);
      Assert.assertTrue(QueryGranularity.NONE.compare(p) < 0);
      Assert.assertTrue(p.compare(QueryGranularity.ALL) < 0);
      Assert.assertTrue(p.compare(QueryGranularity.NONE) > 0);
    }

    Assert.assertTrue(p1.compare(p3) < 0);
    Assert.assertTrue(p3.compare(p1) > 0);
    Assert.assertNull(p1.compare(p2));
    Assert.assertNull(p2.compare(p1));
    Assert.assertNull(p1.compare(p4));
    Assert.assertNull(p4.compare(p1));
    Assert.assertTrue(p3.compare(p5) < 0);
    Assert.assertTrue(p5.compare(p3) > 0);
    Assert.assertTrue(p6.compare(p7) < 0);
    Assert.assertTrue(p7.compare(p6) > 0);
    Assert.assertTrue(p3.compare(p8) < 0);
    Assert.assertTrue(p8.compare(p3) > 0);
    Assert.assertNull(p1.compare(p9));
    Assert.assertNull(p9.compare(p1));

    PeriodGranularity p10 = new PeriodGranularity(new Period("P12M"), null, null);
    PeriodGranularity p11 = new PeriodGranularity(new Period("P1Y"), null, null);
    PeriodGranularity p12 = new PeriodGranularity(new Period("P2Y"), null, null);

    Assert.assertNull(p10.compare(p11));
    Assert.assertNull(p11.compare(p10));
    Assert.assertTrue(p11.compare(p12) < 0);
    Assert.assertTrue(p12.compare(p11) > 0);

  }
}
