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

import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    meters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);

    Assertions.assertEquals(1, meters.getProcessed());
    Assertions.assertEquals(100, meters.getProcessedBytes());
    Assertions.assertEquals(1, meters.getProcessedWithError());
    Assertions.assertEquals(1, meters.getUnparseable());
    Assertions.assertEquals(1, meters.getThrownAway());

    RowIngestionMetersTotals totals = meters.getTotals();
    Assertions.assertEquals(1, totals.getProcessed());
    Assertions.assertEquals(100, totals.getProcessedBytes());
    Assertions.assertEquals(1, totals.getProcessedWithError());
    Assertions.assertEquals(1, totals.getUnparseable());
    Assertions.assertEquals(1, totals.getThrownAway());
  }

  @Test
  public void testIncrementThrownAwayWithReason()
  {
    DropwizardRowIngestionMeters meters = new DropwizardRowIngestionMeters();

    meters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    meters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    meters.incrementThrownAway(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME);
    meters.incrementThrownAway(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME);
    meters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    meters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    meters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);

    // Total thrownAway should be sum of all reasons
    Assertions.assertEquals(7, meters.getThrownAway());

    // Check per-reason counts
    Map<String, Long> byReason = meters.getThrownAwayByReason();
    Assertions.assertEquals(Long.valueOf(2), byReason.get(InputRowFilterResult.NULL_OR_EMPTY_RECORD.getReason()));
    Assertions.assertEquals(Long.valueOf(1), byReason.get(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.getReason()));
    Assertions.assertEquals(Long.valueOf(1), byReason.get(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.getReason()));
    Assertions.assertEquals(Long.valueOf(3), byReason.get(InputRowFilterResult.CUSTOM_FILTER.getReason()));
  }

  @Test
  public void testGetThrownAwayByReasonReturnsAllReasons()
  {
    DropwizardRowIngestionMeters meters = new DropwizardRowIngestionMeters();

    // With no increments, all reasons should be present with 0 counts
    Map<String, Long> byReason = meters.getThrownAwayByReason();
    Assertions.assertTrue(byReason.isEmpty());
  }

  @Test
  public void testMovingAverages()
  {
    DropwizardRowIngestionMeters meters = new DropwizardRowIngestionMeters();

    meters.incrementProcessed();
    meters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);

    Map<String, Object> movingAverages = meters.getMovingAverages();
    Assertions.assertNotNull(movingAverages);
    Assertions.assertTrue(movingAverages.containsKey(DropwizardRowIngestionMeters.ONE_MINUTE_NAME));
    Assertions.assertTrue(movingAverages.containsKey(DropwizardRowIngestionMeters.FIVE_MINUTE_NAME));
    Assertions.assertTrue(movingAverages.containsKey(DropwizardRowIngestionMeters.FIFTEEN_MINUTE_NAME));
  }
}
