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

package org.apache.druid.indexing.common.stats;

import org.apache.druid.segment.incremental.InputRowThrownAwayReason;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class DropwizardRowIngestionMetersTest
{
  @Test
  public void testBasicIncrements()
  {
    DropwizardRowIngestionMeters meters = new DropwizardRowIngestionMeters();
    meters.incrementProcessed();
    meters.incrementProcessedBytes(100);
    meters.incrementProcessedWithError();
    meters.incrementUnparseable();
    meters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);

    Assert.assertEquals(1, meters.getProcessed());
    Assert.assertEquals(100, meters.getProcessedBytes());
    Assert.assertEquals(1, meters.getProcessedWithError());
    Assert.assertEquals(1, meters.getUnparseable());
    Assert.assertEquals(1, meters.getThrownAway());

    RowIngestionMetersTotals totals = meters.getTotals();
    Assert.assertEquals(1, totals.getProcessed());
    Assert.assertEquals(100, totals.getProcessedBytes());
    Assert.assertEquals(1, totals.getProcessedWithError());
    Assert.assertEquals(1, totals.getUnparseable());
    Assert.assertEquals(1, totals.getThrownAway());
  }

  @Test
  public void testIncrementThrownAwayWithReason()
  {
    DropwizardRowIngestionMeters meters = new DropwizardRowIngestionMeters();

    meters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    meters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    meters.incrementThrownAway(InputRowThrownAwayReason.BEFORE_MIN_MESSAGE_TIME);
    meters.incrementThrownAway(InputRowThrownAwayReason.AFTER_MAX_MESSAGE_TIME);
    meters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    meters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    meters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);

    // Total thrownAway should be sum of all reasons
    Assert.assertEquals(7, meters.getThrownAway());

    // Check per-reason counts
    Map<InputRowThrownAwayReason, Long> byReason = meters.getThrownAwayByReason();
    Assert.assertEquals(Long.valueOf(2), byReason.get(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD));
    Assert.assertEquals(Long.valueOf(1), byReason.get(InputRowThrownAwayReason.BEFORE_MIN_MESSAGE_TIME));
    Assert.assertEquals(Long.valueOf(1), byReason.get(InputRowThrownAwayReason.AFTER_MAX_MESSAGE_TIME));
    Assert.assertEquals(Long.valueOf(3), byReason.get(InputRowThrownAwayReason.FILTERED));
  }

  @Test
  public void testGetThrownAwayByReasonReturnsAllReasons()
  {
    DropwizardRowIngestionMeters meters = new DropwizardRowIngestionMeters();

    // Even with no increments, all reasons should be present with 0 counts
    Map<InputRowThrownAwayReason, Long> byReason = meters.getThrownAwayByReason();
    Assert.assertEquals(InputRowThrownAwayReason.values().length, byReason.size());
    for (InputRowThrownAwayReason reason : InputRowThrownAwayReason.values()) {
      Assert.assertEquals(Long.valueOf(0), byReason.get(reason));
    }
  }

  @Test
  public void testMovingAverages()
  {
    DropwizardRowIngestionMeters meters = new DropwizardRowIngestionMeters();

    meters.incrementProcessed();
    meters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);

    Map<String, Object> movingAverages = meters.getMovingAverages();
    Assert.assertNotNull(movingAverages);
    Assert.assertTrue(movingAverages.containsKey(DropwizardRowIngestionMeters.ONE_MINUTE_NAME));
    Assert.assertTrue(movingAverages.containsKey(DropwizardRowIngestionMeters.FIVE_MINUTE_NAME));
    Assert.assertTrue(movingAverages.containsKey(DropwizardRowIngestionMeters.FIFTEEN_MINUTE_NAME));
  }
}

