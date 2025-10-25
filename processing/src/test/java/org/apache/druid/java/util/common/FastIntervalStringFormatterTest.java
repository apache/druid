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

package org.apache.druid.java.util.common;

import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class FastIntervalStringFormatterTest
{
  @Test
  public void testStringFormatterFormat()
  {
    final Interval[] validIntervals = new Interval[]{
        // Boundary values
        Intervals.of("2000-03-01T00:00:00.000Z/2000-03-02T00:00:00.000Z"),
        Intervals.of("2000-03-02T00:00:00.000Z/2000-03-03T00:00:00.000Z"),
        Intervals.of("2400-02-27T00:00:00.000Z/2400-02-28T00:00:00.000Z"),
        Intervals.of("2400-02-28T00:00:00.000Z/2400-02-29T00:00:00.000Z"),

        // Random values
        Intervals.of("2005-03-14T01:33:43.029Z/2005-04-17T12:34:56.789Z"),
        Intervals.of("2021-03-14T01:33:43.029Z/2021-03-15T12:34:56.789Z"),
        Intervals.of("2022-12-03T01:33:43.029Z/2022-12-04T00:00:00.000Z"),
        Intervals.of("2044-03-14T05:26:45.499Z/2096-05-17T18:11:06.330Z"),
        Intervals.of("2004-09-05T19:02:12.560Z/2023-12-15T20:46:55.109Z"),
        Intervals.of("2079-01-25T23:04:55.554Z/2090-11-25T22:15:54.088Z"),
        Intervals.of("2017-03-17T07:43:03.784Z/2082-11-18T08:34:39.805Z"),
        Intervals.of("2065-08-17T09:11:31.589Z/2096-01-03T17:21:08.502Z"),
        Intervals.of("2012-11-17T07:39:30.856Z/2070-06-25T14:35:44.060Z"),
        Intervals.of("2013-02-17T15:22:08.427Z/2073-08-31T16:56:23.603Z"),
        Intervals.of("2073-02-10T23:47:58.800Z/2099-03-11T19:08:38.458Z"),
        Intervals.of("2035-08-09T12:33:43.951Z/2076-12-08T23:59:50.690Z"),
        Intervals.of("2030-01-22T18:39:08.566Z/2092-02-15T02:40:52.702Z"),
        };

    for (Interval interval : validIntervals) {
      Assert.assertEquals(interval.toString(), FastIntervalStringFormatter.format(interval));
    }
  }

  @Test
  public void testStringFormatterFormat_Eternity()
  {
    Assert.assertEquals(Intervals.ETERNITY.toString(), FastIntervalStringFormatter.format(Intervals.ETERNITY));
  }

  @Test
  public void testStringFormatterFormat_FallbackValues()
  {
    Interval intervalMock = Mockito.mock(Interval.class);

    Mockito.when(intervalMock.getChronology())
           .thenReturn(ISOChronology.getInstance(DateTimes.inferTzFromString("Asia/Shanghai")));
    Mockito.when(intervalMock.getStartMillis()).thenReturn(951868810000L); // Valid start
    Mockito.when(intervalMock.getEndMillis()).thenReturn(951868820000L); // Valid end

    String dummyString = "DUMMY_STRING";
    Mockito.when(intervalMock.toString()).thenReturn(dummyString);

    String out = FastIntervalStringFormatter.format(intervalMock);

    Assert.assertEquals(dummyString, out);

    final Interval[] fallbackIntervals = new Interval[]{
        // Boundary values
        Intervals.of("2000-02-28T00:00:00.000Z/2000-02-29T00:00:00.000Z"),
        Intervals.of("2000-02-29T00:00:00.000Z/2000-03-01T00:00:00.000Z"),
        Intervals.of("2400-02-29T00:00:00.000Z/2400-03-01T00:00:00.000Z"),
        Intervals.of("2400-03-01T00:00:00.000Z/2400-03-02T00:00:00.000Z"),

        // Random values
        Intervals.of("1966-09-21T14:29:42.757Z/1999-09-18T16:59:47.466Z"),
        Intervals.of("1985-04-16T03:03:37.833Z/2000-01-21T04:12:01.011Z"),
        Intervals.of("1982-02-02T07:20:40.659Z/1990-06-01T09:58:13.053Z"),
        Intervals.of("1954-11-07T14:38:33.857Z/1972-09-27T04:52:42.821Z"),
        Intervals.of("1977-08-10T18:45:18.676Z/1983-05-13T07:00:59.266Z"),
        Intervals.of("2429-02-19T08:27:26.173Z/2568-02-16T06:53:16.887Z"),
        Intervals.of("2447-07-10T12:48:18.234Z/2489-08-24T01:22:00.056Z"),
        };

    for (Interval interval : fallbackIntervals) {
      Assert.assertEquals(interval.toString(), FastIntervalStringFormatter.format(interval));
    }
  }
}
