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

package org.apache.druid.server.compaction;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.IndexSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReindexingIndexSpecRuleTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");
  private static final Period PERIOD_21_DAYS = Period.days(21);

  private final ReindexingIndexSpecRule rule = new ReindexingIndexSpecRule(
      "test-index-spec-rule",
      "Custom index spec",
      PERIOD_21_DAYS,
      IndexSpec.getDefault()
  );

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    Interval interval = Intervals.of("2025-11-24T00:00:00Z/2025-11-25T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    Interval interval = Intervals.of("2025-11-27T12:00:00Z/2025-11-28T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    Interval interval = Intervals.of("2025-11-27T00:00:00Z/2025-11-29T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    Interval interval = Intervals.of("2025-12-15T00:00:00Z/2025-12-16T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getIndexSpec_returnsConfiguredValue()
  {
    IndexSpec indexSpec = rule.getIndexSpec();

    Assertions.assertNotNull(indexSpec);
    Assertions.assertEquals(IndexSpec.getDefault(), indexSpec);
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assertions.assertEquals("test-index-spec-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assertions.assertEquals("Custom index spec", rule.getDescription());
  }

  @Test
  public void test_getOlderThan_returnsConfiguredPeriod()
  {
    Assertions.assertEquals(PERIOD_21_DAYS, rule.getOlderThan());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingIndexSpecRule(null, "description", PERIOD_21_DAYS, IndexSpec.getDefault())
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingIndexSpecRule("test-id", "description", null, IndexSpec.getDefault())
    );
  }

  @Test
  public void test_constructor_zeroPeriod_succeeds()
  {
    Period zeroPeriod = Period.days(0);
    ReindexingIndexSpecRule rule = new ReindexingIndexSpecRule(
        "test-id",
        "description",
        zeroPeriod,
        IndexSpec.getDefault()
    );
    Assertions.assertEquals(zeroPeriod, rule.getOlderThan());
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    Period negativePeriod = Period.days(-21);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingIndexSpecRule("test-id", "description", negativePeriod, IndexSpec.getDefault())
    );
  }

  @Test
  public void test_constructor_nullIndexSpec_throwsDruidException()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> new ReindexingIndexSpecRule("test-id", "description", PERIOD_21_DAYS, null)
    );
  }

  @Test
  public void test_equals_sameObject_returnsTrue()
  {
    Assertions.assertEquals(rule, rule);
  }

  @Test
  public void test_equals_null_returnsFalse()
  {
    Assertions.assertNotEquals(null, rule);
  }

  @Test
  public void test_equals_equalObjects_returnsTrue()
  {
    ReindexingIndexSpecRule other = new ReindexingIndexSpecRule(
        "test-index-spec-rule",
        "Custom index spec",
        PERIOD_21_DAYS,
        IndexSpec.getDefault()
    );

    Assertions.assertEquals(rule, other);
    Assertions.assertEquals(rule.hashCode(), other.hashCode());
  }

  @Test
  public void test_equals_differentId_returnsFalse()
  {
    ReindexingIndexSpecRule other = new ReindexingIndexSpecRule(
        "different-id",
        "Custom index spec",
        PERIOD_21_DAYS,
        IndexSpec.getDefault()
    );

    Assertions.assertNotEquals(rule, other);
  }

  @Test
  public void test_equals_differentOlderThan_returnsFalse()
  {
    ReindexingIndexSpecRule other = new ReindexingIndexSpecRule(
        "test-index-spec-rule",
        "Custom index spec",
        Period.days(90),
        IndexSpec.getDefault()
    );

    Assertions.assertNotEquals(rule, other);
  }
}
