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

package io.druid.segment.realtime.plumber;

import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class SegmentGranularityRejectionPolicyFactoryTest
{
  private static final DateTimes.UtcFormatter FMT = DateTimes.wrapFormatter(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
  private Granularity gran = Granularities.HOUR;
  private Period period = new Period("PT10M");

  @Test
  public void testInSegmentGranularity() throws Exception
  {
    RejectionPolicy rejectionPolicy = new SegmentGranularityRejectionPolicyFactory().create(period, gran);

    DateTime current = FMT.parse("2016-01-09 03:45:00");

    Assert.assertTrue(rejectionPolicy.accept(current.getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 03:21:00").getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 03:00:00").getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 03:50:00").getMillis()));
  }

  @Test
  public void testInWindow() throws Exception
  {
    RejectionPolicy rejectionPolicy = new SegmentGranularityRejectionPolicyFactory().create(period, gran);

    DateTime current = FMT.parse("2016-01-09 03:05:00");

    Assert.assertTrue(rejectionPolicy.accept(current.getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 02:00:00").getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 02:01:00").getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 02:59:59").getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 03:01:00").getMillis()));
    Assert.assertFalse(rejectionPolicy.accept(FMT.parse("2016-01-09 01:59:59").getMillis()));
  }

  @Test
  public void testOutOfWindow() throws Exception
  {
    RejectionPolicy rejectionPolicy = new SegmentGranularityRejectionPolicyFactory().create(period, gran);

    DateTime current = FMT.parse("2016-01-09 03:15:00");

    Assert.assertTrue(rejectionPolicy.accept(current.getMillis()));
    Assert.assertFalse(rejectionPolicy.accept(FMT.parse("2016-01-09 02:01:00").getMillis()));
    Assert.assertFalse(rejectionPolicy.accept(FMT.parse("2016-01-09 02:59:59").getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 03:00:00").getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(FMT.parse("2016-01-09 03:01:00").getMillis()));
  }

  @Test
  public void testRejectAfterSegmentEnd() throws Exception
  {
    RejectionPolicy rejectionPolicy = new SegmentGranularityRejectionPolicyFactory().create(period, gran);
    DateTime now = DateTimes.nowUtc();
    // 1 hour future
    Assert.assertFalse(rejectionPolicy.accept(now.getMillis() + 3600 * 1000));

    Assert.assertTrue(rejectionPolicy.accept(now.getMillis()));
    long future1 = gran.bucketEnd(now).getMillis() - 10;
    long future2 = gran.bucketEnd(now).getMillis() + 10;

    Assert.assertTrue(rejectionPolicy.accept(future1));
    Assert.assertFalse(rejectionPolicy.accept(future2));
  }
}
