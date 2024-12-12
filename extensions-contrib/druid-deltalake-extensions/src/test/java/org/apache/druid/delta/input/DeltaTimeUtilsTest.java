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

package org.apache.druid.delta.input;

import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

public class DeltaTimeUtilsTest
{
  @Before
  public void setUp()
  {
    System.setProperty("user.timezone", "UTC");
  }

  @Test
  public void testTimestampValue()
  {
    Assert.assertEquals(
        Instant.parse("2018-02-02T00:28:02.000Z"),
        Instant.ofEpochMilli(
            DeltaTimeUtils.getMillisFromTimestamp(
                Instant.parse("2018-02-02T00:28:02.000Z").toEpochMilli() * 1_000
            )
        )
    );

    Assert.assertEquals(
        Instant.parse("2024-01-31T00:58:03.000Z"),
        Instant.ofEpochMilli(
            DeltaTimeUtils.getMillisFromTimestamp(
                Instant.parse("2024-01-31T00:58:03.002Z").toEpochMilli() * 1_000
            )
        )
    );
  }

  @Test
  public void testDateTimeValue()
  {
    Assert.assertEquals(
        Instant.parse("2020-02-01T00:00:00.000Z"),
        Instant.ofEpochSecond(
            DeltaTimeUtils.getSecondsFromDate(
                (int) Intervals.of("1970-01-01/2020-02-01").toDuration().getStandardDays()
            )
        )
    );

    Assert.assertEquals(
        Instant.parse("2024-01-01T00:00:00.000Z"),
        Instant.ofEpochSecond(
            DeltaTimeUtils.getSecondsFromDate(
                (int) Intervals.of("1970-01-01/2024-01-01T02:23:00").toDuration().getStandardDays()
            )
        )
    );
  }
}
