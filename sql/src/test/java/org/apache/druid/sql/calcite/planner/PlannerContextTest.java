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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.Intervals;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link PlannerContext}.
 */
public class PlannerContextTest
{
  @Test
  public void test_parseTimeCondition_null()
  {
    Assertions.assertNull(PlannerContext.parseTimeCondition(null));
  }

  @Test
  public void test_parseTimeCondition_singleInterval()
  {
    Assertions.assertEquals(
        List.of(Intervals.of("2000-01-01/2000-01-02")),
        PlannerContext.parseTimeCondition("2000-01-01/2000-01-02")
    );
  }

  @Test
  public void test_parseTimeCondition_commaSeparated()
  {
    // Out of order, with whitespace, and with overlapping and abutting intervals.
    Assertions.assertEquals(
        List.of(Intervals.of("2000-01-01/2000-01-04"), Intervals.of("2001-01-01/2001-01-02")),
        PlannerContext.parseTimeCondition(
            "2001-01-01/2001-01-02, 2000-01-02/2000-01-03 ,2000-01-01/2000-01-02,2000-01-02T12/2000-01-04"
        )
    );
  }

  @Test
  public void test_parseTimeCondition_timeZoneOffset()
  {
    Assertions.assertEquals(
        List.of(Intervals.of("2000-01-01T08:00:00Z/2000-01-02T08:00:00Z")),
        PlannerContext.parseTimeCondition("2000-01-01T00:00:00-08:00/2000-01-02T00:00:00-08:00")
    );
  }

  @Test
  public void test_parseTimeCondition_emptyString()
  {
    assertInvalidTimeCondition("", "");
  }

  @Test
  public void test_parseTimeCondition_onlyComma()
  {
    assertInvalidTimeCondition(",", ",");
  }

  @Test
  public void test_parseTimeCondition_trailingComma()
  {
    assertInvalidTimeCondition("2000-01-01/2000-01-02,", "2000-01-01/2000-01-02,");
  }

  @Test
  public void test_parseTimeCondition_invalidInterval()
  {
    assertInvalidTimeCondition("2000-01-01/2000-01-02,foo", "2000-01-01/2000-01-02,foo");
  }

  private static void assertInvalidTimeCondition(final String value, final String expectedValueString)
  {
    DruidExceptionMatcher.assertThat(
        Assertions.assertThrows(DruidException.class, () -> PlannerContext.parseTimeCondition(value)),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Query context parameter [sqlTimeCondition] must be a comma-separated string of ISO8601 intervals, "
            + "but got [" + expectedValueString + "]"
        )
    );
  }
}
