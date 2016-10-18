/*
 * Copyright 2011 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.parsers;

import com.google.common.base.Function;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class TimestampParserTest
{

  @Test
  public void testStripQuotes() throws Exception {
    Assert.assertEquals("hello world", ParserUtils.stripQuotes("\"hello world\""));
    Assert.assertEquals("hello world", ParserUtils.stripQuotes("    \"    hello world   \"    "));
  }

  @Test
  public void testAuto() throws Exception {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("auto");
    Assert.assertEquals(new DateTime("2009-02-13T23:31:30Z"), parser.apply("1234567890000"));
    Assert.assertEquals(new DateTime("2009-02-13T23:31:30Z"), parser.apply("2009-02-13T23:31:30Z"));
    Assert.assertEquals(new DateTime("2009-02-13T23:31:30Z"), parser.apply(1234567890000L));
  }

  @Test
  public void testRuby() throws Exception {
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("ruby");
    Assert.assertEquals(new DateTime("2013-01-16T15:41:47+01:00"), parser.apply("1358347307.435447"));
    Assert.assertEquals(new DateTime("2013-01-16T15:41:47+01:00"), parser.apply(1358347307.435447D));
  }

  @Test
  public void testNano() throws Exception {
    String timeNsStr = "1427504794977098494";
    DateTime expectedDt = new DateTime("2015-3-28T01:06:34.977Z");
    final Function<Object, DateTime> parser = TimestampParser.createObjectTimestampParser("nano");
    Assert.assertEquals("Incorrect truncation of nanoseconds -> milliseconds",
        expectedDt, parser.apply(timeNsStr));

    // Confirm sub-millisecond timestamps are handled correctly
    expectedDt = new DateTime("1970-1-1T00:00:00.000Z");
    Assert.assertEquals(expectedDt, parser.apply("999999"));
    Assert.assertEquals(expectedDt, parser.apply("0"));
    Assert.assertEquals(expectedDt, parser.apply("0000"));
    Assert.assertEquals(expectedDt, parser.apply(999999L));
  }

  /*Commenting out until Joda 2.1 supported
  @Test
  public void testTimeStampParserWithQuotes() throws Exception {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Function<String, DateTime> parser = ParserUtils.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy");
    Assert.assertEquals(d.getMillis(), parser.apply(" \" Wed Nov 9 04:00:00 PST 1994 \"  ").getMillis());
  }

  @Test
  public void testTimeStampParserWithShortTimeZone() throws Exception {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Function<String, DateTime> parser = ParserUtils.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy");
    Assert.assertEquals(d.getMillis(), parser.apply("Wed Nov 9 04:00:00 PST 1994").getMillis());
  }

  @Test
  public void testTimeStampParserWithLongTimeZone() throws Exception {

    long millis1 = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8)).getMillis();
    long millis2 = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-6)).getMillis();

    Function<String, DateTime> parser = ParserUtils.createTimestampParser("EEE MMM dd HH:mm:ss zZ z yyyy");
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 GMT-0800 PST 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 GMT-0600 CST 1994").getMillis());
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 UTC-0800 PST 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 UTC-0600 CST 1994").getMillis());

    parser = ParserUtils.createTimestampParser("EEE MMM dd HH:mm:ss zZ yyyy");
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 GMT-0800 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 GMT-0600 1994").getMillis());
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 UTC-0800 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 UTC-0600 1994").getMillis());

    parser = ParserUtils.createTimestampParser("EEE MMM dd HH:mm:ss zZ Q yyyy");
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 GMT-0800 (PST) 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 GMT-0600 (CST) 1994").getMillis());
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 UTC-0800 (PST) 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 UTC-0600 (CST) 1994").getMillis());

  }

  @Test
  public void testTimeZoneAtExtremeLocations() throws Exception {
    Function<String, DateTime> parser = ParserUtils.createTimestampParser("EEE MMM dd yy HH:mm:ss zZ z");
    Assert.assertEquals(new DateTime(2005, 1, 22, 13, 0, DateTimeZone.forOffsetHours(-6)).getMillis(),
                        parser.apply("Sat Jan 22 05 13:00:00 GMT-0600 CST").getMillis());

    parser = ParserUtils.createTimestampParser("zZ z EEE MMM dd yy HH:mm:ss");
    Assert.assertEquals(new DateTime(2005, 1, 22, 13, 0, DateTimeZone.forOffsetHours(-6)).getMillis(),
                        parser.apply("GMT-0600 CST Sat Jan 22 05 13:00:00").getMillis());
  }
  */

  /**
   * This test case checks a potentially fragile behavior
   * Some timestamps will come to us in the form of GMT-OFFSET (Time Zone Abbreviation)
   * The number of time zone abbreviations is long and what they mean can change
   * If the offset is explicitly provided via GMT-OFFSET, we want Joda to use this instead
   * of the time zone abbreviation
   * @throws Exception
   */
  /*@Test
  public void testOffsetPriority() throws Exception {
    long millis1 = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8)).getMillis();
    long millis2 = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-6)).getMillis();

    Function<String, DateTime> parser = ParserUtils.createTimestampParser("EEE MMM dd HH:mm:ss zZ Q yyyy");

    //Test timestamps that have an incorrect time zone abbreviation for the GMT offset.
    //Joda should use the offset and not use the time zone abbreviation
    Assert.assertEquals(millis1, parser.apply("Wed Nov 9 04:00:00 GMT-0800 (ADT) 1994").getMillis());
    Assert.assertEquals(millis2, parser.apply("Wed Nov 9 04:00:00 GMT-0600 (MDT) 1994").getMillis());
  }

  @Test
  public void testJodaSymbolInsideLiteral() throws Exception {
    DateTime d = new DateTime(1994, 11, 9, 4, 0, DateTimeZone.forOffsetHours(-8));
    Assert.assertEquals(d.getMillis(),
                        ParserUtils.createTimestampParser("EEE MMM dd HH:mm:ss z yyyy 'helloz'")
                                   .apply("Wed Nov 9 04:00:00 PST 1994 helloz")
                                   .getMillis()
    );
    Assert.assertEquals(d.getMillis(),
                        ParserUtils.createTimestampParser("EEE MMM dd HH:mm:ss 'helloz' z yyyy 'hello'")
                                   .apply("Wed Nov 9 04:00:00 helloz PST 1994 hello")
                                   .getMillis()
    );
  }*/



}
