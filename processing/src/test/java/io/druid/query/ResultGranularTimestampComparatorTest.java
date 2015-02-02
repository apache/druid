/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
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

package io.druid.query;

import io.druid.granularity.QueryGranularity;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ResultGranularTimestampComparatorTest
{
  private final DateTime time = new DateTime("2011-11-11");

  @Test
  public void testCompareAll()
  {
    Result<Object> r1 = new Result<Object>(time, null);
    Result<Object> r2 = new Result<Object>(time.plusYears(5), null);

    Assert.assertEquals(new ResultGranularTimestampComparator<Object>(QueryGranularity.ALL).compare(r1, r2), 0);
  }

  @Test
  public void testCompareDay()
  {
    Result<Object> res = new Result<Object>(time, null);
    Result<Object> same = new Result<Object>(time.plusHours(12), null);
    Result<Object> greater = new Result<Object>(time.plusHours(25), null);
    Result<Object> less = new Result<Object>(time.minusHours(1), null);

    QueryGranularity day = QueryGranularity.DAY;
    Assert.assertEquals(new ResultGranularTimestampComparator<Object>(day).compare(res, same), 0);
    Assert.assertEquals(new ResultGranularTimestampComparator<Object>(day).compare(res, greater), -1);
    Assert.assertEquals(new ResultGranularTimestampComparator<Object>(day).compare(res, less), 1);
  }
  
  @Test
  public void testCompareHour()
  {
    Result<Object> res = new Result<Object>(time, null);
    Result<Object> same = new Result<Object>(time.plusMinutes(55), null);
    Result<Object> greater = new Result<Object>(time.plusHours(1), null);
    Result<Object> less = new Result<Object>(time.minusHours(1), null);

    QueryGranularity hour = QueryGranularity.HOUR;
    Assert.assertEquals(new ResultGranularTimestampComparator<Object>(hour).compare(res, same), 0);
    Assert.assertEquals(new ResultGranularTimestampComparator<Object>(hour).compare(res, greater), -1);
    Assert.assertEquals(new ResultGranularTimestampComparator<Object>(hour).compare(res, less), 1);
  }
}
