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
    rowIngestionMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    final Map<String, Long> expected = Map.of(InputRowFilterResult.NULL_OR_EMPTY_RECORD.getReason(), 1L);
    Assert.assertEquals(new RowIngestionMetersTotals(1, 5, 1, expected, 1), rowIngestionMeters.getTotals());
  }

  @Test
  public void testAddRowIngestionMetersTotals()
  {
    SimpleRowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    RowIngestionMetersTotals rowIngestionMetersTotals = new RowIngestionMetersTotals(10, 0, 1, 1, 1);
    rowIngestionMeters.addRowIngestionMetersTotals(rowIngestionMetersTotals);
    Assert.assertEquals(rowIngestionMetersTotals, rowIngestionMeters.getTotals());
  }

  @Test
  public void testIncrementThrownAwayWithReason()
  {
    SimpleRowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();

    rowIngestionMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    rowIngestionMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    rowIngestionMeters.incrementThrownAway(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME);
    rowIngestionMeters.incrementThrownAway(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME);
    rowIngestionMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    rowIngestionMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    rowIngestionMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);

    Assert.assertEquals(7, rowIngestionMeters.getThrownAway());

    Map<String, Long> byReason = rowIngestionMeters.getThrownAwayByReason();
    Assert.assertEquals(Long.valueOf(2), byReason.get(InputRowFilterResult.NULL_OR_EMPTY_RECORD.getReason()));
    Assert.assertEquals(Long.valueOf(1), byReason.get(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.getReason()));
    Assert.assertEquals(Long.valueOf(1), byReason.get(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.getReason()));
    Assert.assertEquals(Long.valueOf(3), byReason.get(InputRowFilterResult.CUSTOM_FILTER.getReason()));
  }

  @Test
  public void testGetThrownAwayByReasonReturnsNoRejectedReasons()
  {
    SimpleRowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();

    // With no increments, no rejected reasons should be present
    Map<String, Long> byReason = rowIngestionMeters.getThrownAwayByReason();
    Assert.assertTrue(byReason.isEmpty());
  }
}
