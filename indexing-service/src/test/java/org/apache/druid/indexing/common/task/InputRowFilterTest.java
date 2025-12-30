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
import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class InputRowFilterTest
{
  private static final List<String> DIMENSIONS = ImmutableList.of("dim1");

  @Test
  public void test_fromPredicate_whichAllowsAll()
  {
    InputRowFilter filter = InputRowFilter.fromPredicate(row -> true);
    InputRow row = newRow(100);

    Assert.assertEquals(InputRowFilterResult.ACCEPTED, filter.test(row));
  }

  @Test
  public void testFromPredicateReject()
  {
    InputRowFilter filter = InputRowFilter.fromPredicate(row -> false);
    InputRow row = newRow(100);

    Assert.assertEquals(InputRowFilterResult.CUSTOM_FILTER, filter.test(row));
  }

  @Test
  public void testAndBothAccept()
  {
    InputRowFilter filter1 = InputRowFilter.allowAll();
    InputRowFilter filter2 = InputRowFilter.allowAll();
    InputRowFilter combined = filter1.and(filter2);

    InputRow row = newRow(100);
    Assert.assertEquals(InputRowFilterResult.ACCEPTED, combined.test(row));
  }

  @Test
  public void testAndFirstRejects()
  {
    InputRowFilter filter1 = row -> InputRowFilterResult.NULL_OR_EMPTY_RECORD;
    InputRowFilter filter2 = InputRowFilter.allowAll();
    InputRowFilter combined = filter1.and(filter2);

    InputRow row = newRow(100);
    Assert.assertEquals(InputRowFilterResult.NULL_OR_EMPTY_RECORD, combined.test(row));
  }

  @Test
  public void testAndSecondRejects()
  {
    InputRowFilter filter1 = InputRowFilter.allowAll();
    InputRowFilter filter2 = row -> InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME;
    InputRowFilter combined = filter1.and(filter2);

    InputRow row = newRow(100);
    Assert.assertEquals(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME, combined.test(row));
  }

  @Test
  public void testAndBothRejectReturnsFirst()
  {
    InputRowFilter filter1 = row -> InputRowFilterResult.NULL_OR_EMPTY_RECORD;
    InputRowFilter filter2 = row -> InputRowFilterResult.CUSTOM_FILTER;
    InputRowFilter combined = filter1.and(filter2);

    InputRow row = newRow(100);
    // Should return reason from first filter
    Assert.assertEquals(InputRowFilterResult.NULL_OR_EMPTY_RECORD, combined.test(row));
  }

  @Test
  public void testChainedAnd()
  {
    InputRowFilter filter1 = InputRowFilter.allowAll();
    InputRowFilter filter2 = InputRowFilter.allowAll();
    InputRowFilter filter3 = row -> InputRowFilterResult.AFTER_MAX_MESSAGE_TIME;

    InputRowFilter combined = filter1.and(filter2).and(filter3);

    InputRow row = newRow(100);
    Assert.assertEquals(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME, combined.test(row));
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

