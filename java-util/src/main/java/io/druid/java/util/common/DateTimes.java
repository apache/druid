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

package io.druid.java.util.common;

import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

public final class DateTimes
{
  public static final DateTime EPOCH = utc(0);
  public static final DateTime MAX = utc(JodaUtils.MAX_INSTANT);
  public static final DateTime MIN = utc(JodaUtils.MIN_INSTANT);

  public static DateTime utc(long instant)
  {
    return new DateTime(instant, ISOChronology.getInstanceUTC());
  }

  public static DateTime of(String instant)
  {
    return new DateTime(instant, ISOChronology.getInstanceUTC());
  }

  public static DateTime nowUtc()
  {
    return DateTime.now(ISOChronology.getInstanceUTC());
  }

  public static DateTime max(DateTime dt1, DateTime dt2)
  {
    return dt1.compareTo(dt2) >= 0 ? dt1 : dt2;
  }

  public static DateTime min(DateTime dt1, DateTime dt2)
  {
    return dt1.compareTo(dt2) < 0 ? dt1 : dt2;
  }

  private DateTimes()
  {
  }
}
