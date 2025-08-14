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

import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class PeriodGranularityTest
{
  PeriodGranularity UTC_PT1H = new PeriodGranularity(new Period("PT1H"), null, DateTimeZone.UTC);
  PeriodGranularity UTC_PT1M = new PeriodGranularity(new Period("PT1M"), null, DateTimeZone.UTC);

  DateTimeZone PACIFIC_TZ = DateTimes.inferTzFromString("America/Los_Angeles");
  DateTimeZone INDIAN_TZ = DateTimes.inferTzFromString("Asia/Kolkata");

  @Test
  public void testCanBeMappedTo_sameTimeZone()
  {
    Assertions.assertTrue(UTC_PT1M.canBeMappedTo(UTC_PT1H));

    PeriodGranularity pacificPT2H = new PeriodGranularity(new Period("PT2H"), null, PACIFIC_TZ);
    Assertions.assertFalse(pacificPT2H.canBeMappedTo(new PeriodGranularity(new Period("PT20M"), null, PACIFIC_TZ)));
    Assertions.assertTrue(pacificPT2H.canBeMappedTo(new PeriodGranularity(new Period("PT6H"), null, PACIFIC_TZ)));

    PeriodGranularity pacificP1D = new PeriodGranularity(new Period("P1D"), null, PACIFIC_TZ);
    Assertions.assertFalse(pacificP1D.canBeMappedTo(new PeriodGranularity(new Period("PT1H"), null, PACIFIC_TZ)));
    Assertions.assertTrue(pacificP1D.canBeMappedTo(new PeriodGranularity(new Period("P1M"), null, PACIFIC_TZ)));

    PeriodGranularity pacificP2D = new PeriodGranularity(new Period("P2D"), null, PACIFIC_TZ);
    Assertions.assertFalse(pacificP2D.canBeMappedTo(new PeriodGranularity(new Period("P1W"), null, PACIFIC_TZ)));
    Assertions.assertTrue(pacificP2D.canBeMappedTo(new PeriodGranularity(new Period("P2W"), null, PACIFIC_TZ)));

    // some extra tests for different month/week/day combo
    PeriodGranularity pacificPT1W2D = new PeriodGranularity(new Period("P1W").withDays(2), null, PACIFIC_TZ);
    Assertions.assertFalse(pacificPT1W2D.canBeMappedTo(new PeriodGranularity(new Period("P2M"), null, PACIFIC_TZ)));
    Assertions.assertFalse(pacificPT1W2D.canBeMappedTo(new PeriodGranularity(new Period("P2Y"), null, PACIFIC_TZ)));
    Assertions.assertTrue(pacificPT1W2D.canBeMappedTo(pacificPT1W2D));
    Assertions.assertTrue(pacificPT1W2D.canBeMappedTo(new PeriodGranularity(
        new Period("P2W").withDays(4),
        null,
        PACIFIC_TZ
    )));

    Assertions.assertTrue(pacificP1D.canBeMappedTo(pacificPT1W2D));
    Assertions.assertTrue(pacificPT2H.canBeMappedTo(new PeriodGranularity(
        new Period("P1D").withHours(2),
        null,
        PACIFIC_TZ
    )));
  }

  @Test
  public void testCanBeMappedTo_sameHourlyAlignWithUtc()
  {
    Assertions.assertTrue(UTC_PT1H.canBeMappedTo(new PeriodGranularity(new Period("PT1H"), null, PACIFIC_TZ)));
    Assertions.assertTrue(UTC_PT1H.canBeMappedTo(new PeriodGranularity(new Period("PT3H"), null, PACIFIC_TZ)));
  }

  @Test
  public void testCanBeMappedTo_sameMinuteAlignWithUtc()
  {
    Assertions.assertFalse(UTC_PT1H.canBeMappedTo(new PeriodGranularity(new Period("PT2H"), null, INDIAN_TZ)));
    Assertions.assertTrue(UTC_PT1M.canBeMappedTo(new PeriodGranularity(new Period("PT1M"), null, PACIFIC_TZ)));
  }

  @Test
  public void testCanBeMappedTo_withOrigin()
  {
    Assertions.assertFalse(new PeriodGranularity(
        new Period("PT1H"),
        DateTimes.nowUtc(),
        DateTimeZone.UTC
    ).canBeMappedTo(new PeriodGranularity(new Period("PT2H"), null, DateTimeZone.UTC)));

    Assertions.assertFalse(UTC_PT1H.canBeMappedTo(new PeriodGranularity(
        new Period("PT1H"),
        DateTimes.nowUtc(),
        DateTimeZone.UTC
    )));
  }

  @Test
  public void testCanBeMappedTo_differentTimeZone()
  {
    // In theory pacificPT1M should be able to map to UTC_PT1M, but we don't support this for simplicity.
    PeriodGranularity pacificPT1H = new PeriodGranularity(new Period("PT1H"), null, PACIFIC_TZ);
    Assertions.assertTrue(pacificPT1H.canBeMappedTo(UTC_PT1H));
    Assertions.assertTrue(pacificPT1H.canBeMappedTo(new PeriodGranularity(
        new Period("PT3H"),
        null,
        DateTimes.inferTzFromString("America/New_York")
    )));
    Assertions.assertTrue(pacificPT1H.canBeMappedTo(new PeriodGranularity(
        new Period("P1M"),
        null,
        DateTimeZone.UTC
    )));

    Assertions.assertFalse(pacificPT1H.canBeMappedTo(new PeriodGranularity(
        new Period("PT1H"),
        null,
        INDIAN_TZ
    )));
    Assertions.assertFalse(pacificPT1H.canBeMappedTo(new PeriodGranularity(
        new Period("P1D"),
        null,
        INDIAN_TZ
    )));
    Assertions.assertFalse(new PeriodGranularity(
        new Period("P1D"),
        null,
        PACIFIC_TZ
    ).canBeMappedTo(new PeriodGranularity(new Period("P1D"), null, DateTimeZone.UTC)));
  }
}
