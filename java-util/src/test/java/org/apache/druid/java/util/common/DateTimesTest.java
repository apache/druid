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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

public class DateTimesTest
{

  @Test
  public void testCommonDateTimePattern()
  {
    DateTime dt1 = DateTimes.nowUtc();
    DateTime dt2 = new DateTime(System.currentTimeMillis(), DateTimes.inferTzFromString("IST"));
    DateTime dt3 = new DateTime(System.currentTimeMillis(), DateTimeZone.forOffsetHoursMinutes(1, 30));

    for (DateTime dt : new DateTime[] {dt1, dt2, dt3}) {
      Assert.assertTrue(DateTimes.COMMON_DATE_TIME_PATTERN.matcher(dt.toString()).matches());
    }
  }
}
