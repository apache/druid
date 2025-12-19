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

package org.apache.druid.segment.incremental;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SimpleRowIngestionMetersTest
{
  @Test
  public void testIncrement()
  {
    SimpleRowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    rowIngestionMeters.incrementProcessed();
    rowIngestionMeters.incrementProcessedBytes(5);
    rowIngestionMeters.incrementProcessedWithError();
    rowIngestionMeters.incrementUnparseable();
    rowIngestionMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    Assert.assertEquals(rowIngestionMeters.getTotals(), new RowIngestionMetersTotals(1, 5, 1, Map.of(
        InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD, 1L), 1));
  }

  @Test
  public void testAddRowIngestionMetersTotals()
  {
    SimpleRowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    RowIngestionMetersTotals rowIngestionMetersTotals = new RowIngestionMetersTotals(10, 0, 1, Map.of(), 1);
    rowIngestionMeters.addRowIngestionMetersTotals(rowIngestionMetersTotals);
    Assert.assertEquals(rowIngestionMeters.getTotals(), rowIngestionMetersTotals);
  }

  @Test
  public void testIncrementThrownAwayWithReason()
  {
    SimpleRowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();

    rowIngestionMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    rowIngestionMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    rowIngestionMeters.incrementThrownAway(InputRowThrownAwayReason.BEFORE_MIN_MESSAGE_TIME);
    rowIngestionMeters.incrementThrownAway(InputRowThrownAwayReason.AFTER_MAX_MESSAGE_TIME);
    rowIngestionMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    rowIngestionMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    rowIngestionMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);

    Assert.assertEquals(7, rowIngestionMeters.getThrownAway());

    Map<InputRowThrownAwayReason, Long> byReason = rowIngestionMeters.getThrownAwayByReason();
    Assert.assertEquals(Long.valueOf(2), byReason.get(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD));
    Assert.assertEquals(Long.valueOf(1), byReason.get(InputRowThrownAwayReason.BEFORE_MIN_MESSAGE_TIME));
    Assert.assertEquals(Long.valueOf(1), byReason.get(InputRowThrownAwayReason.AFTER_MAX_MESSAGE_TIME));
    Assert.assertEquals(Long.valueOf(3), byReason.get(InputRowThrownAwayReason.FILTERED));
  }

  @Test
  public void testGetThrownAwayByReasonReturnsAllReasons()
  {
    SimpleRowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();

    // Even with no increments, all reasons should be present with 0 counts
    Map<InputRowThrownAwayReason, Long> byReason = rowIngestionMeters.getThrownAwayByReason();
    Assert.assertEquals(InputRowThrownAwayReason.values().length, byReason.size());
    for (InputRowThrownAwayReason reason : InputRowThrownAwayReason.values()) {
      Assert.assertEquals(Long.valueOf(0), byReason.get(reason));
    }
  }
}
