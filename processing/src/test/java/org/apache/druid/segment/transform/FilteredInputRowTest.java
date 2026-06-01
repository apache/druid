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

package org.apache.druid.segment.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

public class FilteredInputRowTest
{
  @Test
  public void testFilterResultAndObjectMethods() throws Exception
  {
    final FilteredInputRow row = FilteredInputRow.CUSTOM_FILTER;
    final FilteredInputRow sameReasonRow = newFilteredInputRow(InputRowFilterResult.CUSTOM_FILTER);
    final FilteredInputRow differentReasonRow = newFilteredInputRow(InputRowFilterResult.UNKNOWN);

    Assert.assertEquals(InputRowFilterResult.CUSTOM_FILTER, row.getFilterResult());
    Assert.assertEquals(row, sameReasonRow);
    Assert.assertNotEquals(row, differentReasonRow);
    Assert.assertNotEquals(row, null);
    Assert.assertNotEquals(row, new Object());
    Assert.assertEquals(row.hashCode(), sameReasonRow.hashCode());
    Assert.assertEquals(0, row.compareTo(sameReasonRow));
    Assert.assertTrue(row.compareTo(differentReasonRow) < 0);
  }

  @Test
  public void testFilteredInputRowDoesNotCarryRowData()
  {
    final FilteredInputRow row = FilteredInputRow.CUSTOM_FILTER;
    final InputRow inputRow = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", "value")
    );

    assertUnsupported(row::getDimensions);
    assertUnsupported(row::getTimestampFromEpoch);
    assertUnsupported(row::getTimestamp);
    assertUnsupported(() -> row.getDimension("dim"));
    assertUnsupported(() -> row.getRaw("dim"));
    assertUnsupported(() -> row.getMetric("metric"));
    assertUnsupported(() -> row.compareTo(inputRow));
  }

  private static FilteredInputRow newFilteredInputRow(final InputRowFilterResult filterResult) throws Exception
  {
    final Constructor<FilteredInputRow> constructor = FilteredInputRow.class.getDeclaredConstructor(
        InputRowFilterResult.class
    );
    constructor.setAccessible(true);
    return constructor.newInstance(filterResult);
  }

  private static void assertUnsupported(final ThrowingRunnable runnable)
  {
    final UnsupportedOperationException exception = Assert.assertThrows(
        UnsupportedOperationException.class,
        runnable::run
    );
    Assert.assertEquals("Filtered input rows do not carry row data", exception.getMessage());
  }

  private interface ThrowingRunnable
  {
    void run() throws Exception;
  }
}
