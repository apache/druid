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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.incremental.ThrownAwayReason;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RowFilterTest
{
  private static final List<String> DIMENSIONS = ImmutableList.of("dim1");

  @Test
  public void testFromPredicateAccept()
  {
    RowFilter filter = RowFilter.fromPredicate(row -> true);
    InputRow row = newRow(100);

    Assert.assertNull(filter.test(row));
  }

  @Test
  public void testFromPredicateReject()
  {
    RowFilter filter = RowFilter.fromPredicate(row -> false);
    InputRow row = newRow(100);

    Assert.assertEquals(ThrownAwayReason.FILTERED, filter.test(row));
  }

  @Test
  public void testAndBothAccept()
  {
    RowFilter filter1 = row -> null;
    RowFilter filter2 = row -> null;
    RowFilter combined = filter1.and(filter2);

    InputRow row = newRow(100);
    Assert.assertNull(combined.test(row));
  }

  @Test
  public void testAndFirstRejects()
  {
    RowFilter filter1 = row -> ThrownAwayReason.NULL;
    RowFilter filter2 = row -> null;
    RowFilter combined = filter1.and(filter2);

    InputRow row = newRow(100);
    Assert.assertEquals(ThrownAwayReason.NULL, combined.test(row));
  }

  @Test
  public void testAndSecondRejects()
  {
    RowFilter filter1 = row -> null;
    RowFilter filter2 = row -> ThrownAwayReason.BEFORE_MIN_MESSAGE_TIME;
    RowFilter combined = filter1.and(filter2);

    InputRow row = newRow(100);
    Assert.assertEquals(ThrownAwayReason.BEFORE_MIN_MESSAGE_TIME, combined.test(row));
  }

  @Test
  public void testAndBothRejectReturnsFirst()
  {
    RowFilter filter1 = row -> ThrownAwayReason.NULL;
    RowFilter filter2 = row -> ThrownAwayReason.FILTERED;
    RowFilter combined = filter1.and(filter2);

    InputRow row = newRow(100);
    // Should return reason from first filter
    Assert.assertEquals(ThrownAwayReason.NULL, combined.test(row));
  }

  @Test
  public void testChainedAnd()
  {
    RowFilter filter1 = row -> null;
    RowFilter filter2 = row -> null;
    RowFilter filter3 = row -> ThrownAwayReason.AFTER_MAX_MESSAGE_TIME;

    RowFilter combined = filter1.and(filter2).and(filter3);

    InputRow row = newRow(100);
    Assert.assertEquals(ThrownAwayReason.AFTER_MAX_MESSAGE_TIME, combined.test(row));
  }

  private static InputRow newRow(Object dim1Val)
  {
    return new MapBasedInputRow(
        DateTimes.of("2020-01-01"),
        DIMENSIONS,
        ImmutableMap.of("dim1", dim1Val)
    );
  }
}

