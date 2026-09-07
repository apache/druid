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

package org.apache.druid.java.util.common.parsers;

import com.google.common.base.Function;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.TimeZone;

public class TimestampParserTest
{
  @Test
  public void testStripQuotes()
  {
    Assertions.assertEquals("hello world", ParserUtils.stripQuotes("\"hello world\""));
    Assertions.assertEquals("hello world", ParserUtils.stripQuotes("    \"    hello world   \"    "));
  }

  @Test
  public void testExtractTimeZone()
  {
    Assertions.assertEquals(DateTimeZone.UTC, ParserUtils.getDateTimeZone("UTC"));
    Assertions.assertEquals(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")), ParserUtils.getDateTimeZone("PST"));
    Assertions.assertNull(ParserUtils.getDateTimeZone("Hello"));
    Assertions.assertNull(ParserUtils.getDateTimeZone("AEST"));
    Assertions.assertEquals(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Australia/Hobart")),
        ParserUtils.getDateTimeZone("Australia/Hobart"));
    Assertions.assertNull(ParserUtils.getDateTimeZone(""));
    Assertions.assertNull(ParserUtils.getDateTimeZone(null));
  }

  @Test
  public void testAuto()
  {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("auto");
    Assertions.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("1234567890000"));
    Assertions.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("2009-02-13T23:31:30Z"));
    Assertions.assertEquals(DateTimes.of("2009-02-13T23:31:30-08:00"), parser.apply("2009-02-13T23:31:30-08:00"));
    Assertions.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("2009-02-13 23:31:30Z"));
    Assertions.assertEquals(DateTimes.of("2009-02-13T23:31:30-08:00"), parser.apply("2009-02-13 23:31:30-08:00"));
    Assertions.assertEquals(DateTimes.of("2009-02-13T00:00:00Z"), parser.apply("2009-02-13"));
    Assertions.assertEquals(DateTimes.of("2009-02-13T00:00:00Z"), parser.apply("\"2009-02-13\""));
    Assertions.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("2009-02-13 23:31:30"));
    Assertions.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply(1234567890000L));
    Assertions.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("2009-02-13 23:31:30 UTC"));
    Assertions.assertEquals(new DateTime("2009-02-13T23:31:30Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"))),
        parser.apply("2009-02-13 23:31:30 PST"));
    Assertions.assertEquals(new DateTime("2009-02-13T23:31:30Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"))),
        parser.apply("\"2009-02-13 23:31:30 PST\""));
  }

  @Test
  public void testAutoNull()
  {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("auto");
    Assertions.assertThrows(NullPointerException.class, () -> parser.apply(null));
  }

  @Test
  public void testAutoInvalid()
  {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("auto");
    Assertions.assertThrows(IllegalArgumentException.class, () -> parser.apply("asdf"));
  }

  @Test
  public void testRuby()
  {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("ruby");
    Assertions.assertEquals(DateTimes.of("2013-01-16T14:41:47.435Z"), parser.apply("1358347307.435447"));
    Assertions.assertEquals(DateTimes.of("2013-01-16T14:41:47.435Z"), parser.apply(1358347307.435447D));
  }

  @Test
  public void testNano()
  {
    String timeNsStr = "1427504794977098494";
    DateTime expectedDt = DateTimes.of("2015-3-28T01:06:34.977Z");
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("nano");
    Assertions.assertEquals(expectedDt, parser.apply(timeNsStr), "Incorrect truncation of nanoseconds -> milliseconds");

    // Confirm sub-millisecond timestamps are handled correctly
    expectedDt = DateTimes.of("1970-1-1T00:00:00.000Z");
    Assertions.assertEquals(expectedDt, parser.apply("999999"));
    Assertions.assertEquals(expectedDt, parser.apply("0"));
    Assertions.assertEquals(expectedDt, parser.apply("0000"));
    Assertions.assertEquals(expectedDt, parser.apply(999999L));
  }

  @Test
  public void testTimeStampParserWithQuotes()
  {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Function<String, DateTime> parser = TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy");
    Assertions.assertEquals(d.getMillis(), parser.apply(" \" Wed Nov 9 04:00:00 PST 1994 \"  ").getMillis());
  }

  @Test
  public void testTimeStampParserWithShortTimeZone()
  {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Function<String, DateTime> parser = TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy");
    Assertions.assertEquals(d.getMillis(), parser.apply("Wed Nov 9 04:00:00 PST 1994").getMillis());
  }

  @Test
  public void testTimeStampParserWithLongTimeZone()
  {

    long millis1 = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8)).getMillis();
    long millis2 = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-6)).getMillis();

    Function<String, DateTime> parser = TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss zZ z yyyy");
    Assertions.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 GMT-0800 PST 1994").getMillis());
    Assertions.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 GMT-0600 CST 1994").getMillis());
    Assertions.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 UTC-0800 PST 1994").getMillis());
    Assertions.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 UTC-0600 CST 1994").getMillis());

    parser = TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss zZ yyyy");
    Assertions.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 GMT-0800 1994").getMillis());
    Assertions.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 GMT-0600 1994").getMillis());
    Assertions.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 UTC-0800 1994").getMillis());
    Assertions.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 UTC-0600 1994").getMillis());
  }

  @Test
  public void testTimeZoneAtExtremeLocations()
  {
    Function<String, DateTime> parser = TimestampParser.createTimestampParser("EEE MMM dd yy HH:mm:ss zZ z");
    Assertions.assertEquals(new DateTime(2005, 1, 22, 13, 0, DateTimeZone.forOffsetHours(-6)).getMillis(),
                        parser.apply("Sat Jan 22 05 13:00:00 GMT-0600 CST").getMillis());

    parser = TimestampParser.createTimestampParser("zZ z EEE MMM dd yy HH:mm:ss");
    Assertions.assertEquals(new DateTime(2005, 1, 22, 13, 0, DateTimeZone.forOffsetHours(-6)).getMillis(),
                        parser.apply("GMT-0600 CST Sat Jan 22 05 13:00:00").getMillis());
  }

  @Test
  public void testJodaSymbolInsideLiteral()
  {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Assertions.assertEquals(d.getMillis(),
                        TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy 'helloz'")
                                   .apply("Wed Nov 9 04:00:00 PST 1994 helloz")
                                   .getMillis()
    );
    Assertions.assertEquals(d.getMillis(),
                        TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss 'helloz' z yyyy 'hello'")
                                   .apply("Wed Nov 9 04:00:00 helloz PST 1994 hello")
                                   .getMillis()
    );
  }

  @Test
  public void testFormatsForNumberBasedTimestamp()
  {
    int yearMonthDate = 20200514;
    DateTime expectedDt = DateTimes.of("2020-05-14T00:00:00.000Z");
    Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("yyyyMMdd");
    Assertions.assertEquals(expectedDt, parser.apply(yearMonthDate), "Timestamp of format yyyyMMdd not parsed correctly");

    int year = 2020;
    expectedDt = DateTimes.of("2020-01-01T00:00:00.000Z");
    parser = TimestampParser.createObjectTimestampParser("yyyy");
    Assertions.assertEquals(expectedDt, parser.apply(year), "Timestamp of format yyyy not parsed correctly");

    int yearMonth = 202010;
    expectedDt = DateTimes.of("2020-10-01T00:00:00.000Z");
    parser = TimestampParser.createObjectTimestampParser("yyyyMM");
    Assertions.assertEquals(expectedDt, parser.apply(yearMonth), "Timestamp of format yyyy not parsed correctly");

    // Friday, May 15, 2020 8:20:40 PM GMT
    long millis = 1589574040000L;
    expectedDt = DateTimes.of("2020-05-15T20:20:40.000Z");

    parser = TimestampParser.createObjectTimestampParser("millis");
    Assertions.assertEquals(expectedDt, parser.apply(millis), "Timestamp of format millis not parsed correctly");
    parser = TimestampParser.createObjectTimestampParser("auto");
    Assertions.assertEquals(expectedDt, parser.apply(millis), "Timestamp of format auto not parsed correctly");

    int posix = 1589574040;
    parser = TimestampParser.createObjectTimestampParser("posix");
    Assertions.assertEquals(expectedDt, parser.apply(posix), "Timestamp of format posix not parsed correctly");

    long micro = 1589574040000000L;
    parser = TimestampParser.createObjectTimestampParser("micro");
    Assertions.assertEquals(expectedDt, parser.apply(micro), "Timestamp of format micro not parsed correctly");

  }
}
