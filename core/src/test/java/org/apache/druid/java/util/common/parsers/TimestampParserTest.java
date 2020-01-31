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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.TimeZone;

public class TimestampParserTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testStripQuotes()
  {
    Assert.assertEquals("hello world", ParserUtils.stripQuotes("\"hello world\""));
    Assert.assertEquals("hello world", ParserUtils.stripQuotes("    \"    hello world   \"    "));
  }

  @Test
  public void testExtractTimeZone()
  {
    Assert.assertEquals(DateTimeZone.UTC, ParserUtils.getDateTimeZone("UTC"));
    Assert.assertEquals(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")), ParserUtils.getDateTimeZone("PST"));
    Assert.assertNull(ParserUtils.getDateTimeZone("Hello"));
    Assert.assertNull(ParserUtils.getDateTimeZone("AEST"));
    Assert.assertEquals(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Australia/Hobart")),
        ParserUtils.getDateTimeZone("Australia/Hobart"));
    Assert.assertNull(ParserUtils.getDateTimeZone(""));
    Assert.assertNull(ParserUtils.getDateTimeZone(null));
  }

  @Test
  public void testAuto()
  {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("auto");
    Assert.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("1234567890000"));
    Assert.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("2009-02-13T23:31:30Z"));
    Assert.assertEquals(DateTimes.of("2009-02-13T23:31:30-08:00"), parser.apply("2009-02-13T23:31:30-08:00"));
    Assert.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("2009-02-13 23:31:30Z"));
    Assert.assertEquals(DateTimes.of("2009-02-13T23:31:30-08:00"), parser.apply("2009-02-13 23:31:30-08:00"));
    Assert.assertEquals(DateTimes.of("2009-02-13T00:00:00Z"), parser.apply("2009-02-13"));
    Assert.assertEquals(DateTimes.of("2009-02-13T00:00:00Z"), parser.apply("\"2009-02-13\""));
    Assert.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("2009-02-13 23:31:30"));
    Assert.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply(1234567890000L));
    Assert.assertEquals(DateTimes.of("2009-02-13T23:31:30Z"), parser.apply("2009-02-13 23:31:30 UTC"));
    Assert.assertEquals(new DateTime("2009-02-13T23:31:30Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"))),
        parser.apply("2009-02-13 23:31:30 PST"));
    Assert.assertEquals(new DateTime("2009-02-13T23:31:30Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"))),
        parser.apply("\"2009-02-13 23:31:30 PST\""));
  }

  @Test
  public void testAutoNull()
  {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("auto");

    expectedException.expect(NullPointerException.class);
    parser.apply(null);
  }

  @Test
  public void testAutoInvalid()
  {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("auto");

    expectedException.expect(IllegalArgumentException.class);
    parser.apply("asdf");
  }

  @Test
  public void testRuby()
  {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("ruby");
    Assert.assertEquals(DateTimes.of("2013-01-16T14:41:47.435Z"), parser.apply("1358347307.435447"));
    Assert.assertEquals(DateTimes.of("2013-01-16T14:41:47.435Z"), parser.apply(1358347307.435447D));
  }

  @Test
  public void testNano()
  {
    String timeNsStr = "1427504794977098494";
    DateTime expectedDt = DateTimes.of("2015-3-28T01:06:34.977Z");
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("nano");
    Assert.assertEquals("Incorrect truncation of nanoseconds -> milliseconds",
        expectedDt, parser.apply(timeNsStr));

    // Confirm sub-millisecond timestamps are handled correctly
    expectedDt = DateTimes.of("1970-1-1T00:00:00.000Z");
    Assert.assertEquals(expectedDt, parser.apply("999999"));
    Assert.assertEquals(expectedDt, parser.apply("0"));
    Assert.assertEquals(expectedDt, parser.apply("0000"));
    Assert.assertEquals(expectedDt, parser.apply(999999L));
  }

  @Test
  public void testTimeStampParserWithQuotes()
  {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Function<String, DateTime> parser = TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy");
    Assert.assertEquals(d.getMillis(), parser.apply(" \" Wed Nov 9 04:00:00 PST 1994 \"  ").getMillis());
  }

  @Test
  public void testTimeStampParserWithShortTimeZone()
  {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Function<String, DateTime> parser = TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy");
    Assert.assertEquals(d.getMillis(), parser.apply("Wed Nov 9 04:00:00 PST 1994").getMillis());
  }

  @Test
  public void testTimeStampParserWithLongTimeZone()
  {

    long millis1 = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8)).getMillis();
    long millis2 = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-6)).getMillis();

    Function<String, DateTime> parser = TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss zZ z yyyy");
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 GMT-0800 PST 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 GMT-0600 CST 1994").getMillis());
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 UTC-0800 PST 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 UTC-0600 CST 1994").getMillis());

    parser = TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss zZ yyyy");
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 GMT-0800 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 GMT-0600 1994").getMillis());
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 UTC-0800 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 UTC-0600 1994").getMillis());
  }

  @Test
  public void testTimeZoneAtExtremeLocations()
  {
    Function<String, DateTime> parser = TimestampParser.createTimestampParser("EEE MMM dd yy HH:mm:ss zZ z");
    Assert.assertEquals(new DateTime(2005, 1, 22, 13, 0, DateTimeZone.forOffsetHours(-6)).getMillis(),
                        parser.apply("Sat Jan 22 05 13:00:00 GMT-0600 CST").getMillis());

    parser = TimestampParser.createTimestampParser("zZ z EEE MMM dd yy HH:mm:ss");
    Assert.assertEquals(new DateTime(2005, 1, 22, 13, 0, DateTimeZone.forOffsetHours(-6)).getMillis(),
                        parser.apply("GMT-0600 CST Sat Jan 22 05 13:00:00").getMillis());
  }

  @Test
  public void testJodaSymbolInsideLiteral()
  {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Assert.assertEquals(d.getMillis(),
                        TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy 'helloz'")
                                   .apply("Wed Nov 9 04:00:00 PST 1994 helloz")
                                   .getMillis()
    );
    Assert.assertEquals(d.getMillis(),
                        TimestampParser.createTimestampParser("EEE MMM dd HH:mm:ss 'helloz' z yyyy 'hello'")
                                   .apply("Wed Nov 9 04:00:00 helloz PST 1994 hello")
                                   .getMillis()
    );
  }
}
