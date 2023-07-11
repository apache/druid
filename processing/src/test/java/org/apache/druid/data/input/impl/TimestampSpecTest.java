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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

public class TimestampSpecTest
{
  @Test
  public void testExtractTimestamp()
  {
    TimestampSpec spec = new TimestampSpec("TIMEstamp", "yyyy-MM-dd", null);
    Assert.assertEquals(
        DateTimes.of("2014-03-01"),
        spec.extractTimestamp(ImmutableMap.of("TIMEstamp", "2014-03-01"))
    );
  }

  @Test
  public void testExtractTimestampWithMissingTimestampColumn()
  {
    TimestampSpec spec = new TimestampSpec(null, null, DateTimes.EPOCH);
    Assert.assertEquals(
        DateTimes.of("1970-01-01"),
        spec.extractTimestamp(ImmutableMap.of("dim", "foo"))
    );
  }

  @Test
  public void testContextualTimestampList()
  {
    String dateFormat = "yyyy-MM-dd'T'HH:mm:ss";
    String[] dates = new String[]{
        "2000-01-01T05:00:00",
        "2000-01-01T05:00:01",
        "2000-01-01T05:00:01",
        "2000-01-01T05:00:02",
        "2000-01-01T05:00:03",
        };
    TimestampSpec spec = new TimestampSpec("TIMEstamp", dateFormat, null);

    DateTimes.UtcFormatter formatter = DateTimes.wrapFormatter(ISODateTimeFormat.dateHourMinuteSecond());

    for (String date : dates) {
      DateTime dateTime = spec.extractTimestamp(ImmutableMap.of("TIMEstamp", date));
      DateTime expectedDateTime = formatter.parse(date);
      Assert.assertEquals(expectedDateTime, dateTime);
    }
  }
}
