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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class DeltaTimeUtils
{
  private static final ZoneId ZONE_ID = ZoneId.systemDefault();

  /**
   * {@link io.delta.kernel.types.TimestampType} data in Delta Lake tables is stored internally as the number of
   * microseconds since epoch.
   *
   * @param microSecsSinceEpochUTC microseconds since epoch
   * @return Datetime millis correpsonding to {@code microSecsSinceEpochUTC}
   */
  public static long getMillisFromTimestamp(final long microSecsSinceEpochUTC)
  {
    final LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
        microSecsSinceEpochUTC / 1_000_000 /* epochSecond */,
        (int) (1000 * microSecsSinceEpochUTC % 1_000_000) /* nanoOfSecond */,
        ZoneOffset.UTC
    );
    return dateTime.atZone(ZONE_ID).toInstant().toEpochMilli();
  }

  /**
   * {@link io.delta.kernel.types.DateType} data in Delta Lake tables is stored internally as the number of
   * days since epoch.
   *
   * @param daysSinceEpochUTC number of days since epoch
   * @return number of seconds corresponding to {@code daysSinceEpochUTC}.
   */
  public static long getSecondsFromDate(final int daysSinceEpochUTC)
  {
    return LocalDate.ofEpochDay(daysSinceEpochUTC).atStartOfDay(ZONE_ID).toEpochSecond();
  }
}
