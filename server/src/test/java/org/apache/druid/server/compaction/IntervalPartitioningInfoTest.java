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
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntervalPartitioningInfoTest
{
  private static final Interval INTERVAL = Intervals.of("2025-01-01/2025-01-02");
  private static final ReindexingPartitioningRule SOURCE_RULE = new ReindexingPartitioningRule(
      "rule-1",
      "Test rule",
      Period.days(7),
      Granularities.DAY,
      new DynamicPartitionsSpec(5000000, null),
      null
  );

  @Test
  public void test_constructor_validInput_succeeds()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    Assertions.assertEquals(INTERVAL, info.getInterval());
    Assertions.assertSame(SOURCE_RULE, info.getSourceRule());
    Assertions.assertFalse(info.isRuleSynthetic());
  }

  @Test
  public void test_constructor_withSyntheticFlag()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE, true);

    Assertions.assertTrue(info.isRuleSynthetic());
    Assertions.assertEquals(INTERVAL, info.getInterval());
  }

  @Test
  public void test_constructor_nullSourceRule_throwsDruidException()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> new IntervalPartitioningInfo(INTERVAL, null)
    );
  }

  @Test
  public void test_getGranularity_delegatesToSourceRule()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    Assertions.assertEquals(Granularities.DAY, info.getGranularity());
  }

  @Test
  public void test_getPartitionsSpec_delegatesToSourceRule()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    Assertions.assertNotNull(info.getPartitionsSpec());
    Assertions.assertInstanceOf(DynamicPartitionsSpec.class, info.getPartitionsSpec());
  }

  @Test
  public void test_getVirtualColumns_delegatesToSourceRule()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    Assertions.assertNull(info.getVirtualColumns());
  }

  @Test
  public void test_toString_includesRuleId()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    String str = info.toString();
    Assertions.assertTrue(str.contains("rule-1"));
    Assertions.assertTrue(str.contains("IntervalPartitioningInfo"));
  }

  @Test
  public void test_equals_sameObject_returnsTrue()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    Assertions.assertEquals(info, info);
  }

  @Test
  public void test_equals_null_returnsFalse()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    Assertions.assertNotEquals(null, info);
  }

  @Test
  public void test_equals_differentClass_returnsFalse()
  {
    IntervalPartitioningInfo info = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    Assertions.assertNotEquals("not an IntervalPartitioningInfo", info);
  }

  @Test
  public void test_equals_equalObjects_returnsTrue()
  {
    IntervalPartitioningInfo info1 = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);
    IntervalPartitioningInfo info2 = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);

    Assertions.assertEquals(info1, info2);
    Assertions.assertEquals(info1.hashCode(), info2.hashCode());
  }

  @Test
  public void test_equals_differentInterval_returnsFalse()
  {
    IntervalPartitioningInfo info1 = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);
    IntervalPartitioningInfo info2 = new IntervalPartitioningInfo(
        Intervals.of("2025-02-01/2025-02-02"),
        SOURCE_RULE
    );

    Assertions.assertNotEquals(info1, info2);
  }

  @Test
  public void test_equals_differentSynthetic_returnsFalse()
  {
    IntervalPartitioningInfo info1 = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE, false);
    IntervalPartitioningInfo info2 = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE, true);

    Assertions.assertNotEquals(info1, info2);
  }

  @Test
  public void test_equals_differentSourceRule_returnsFalse()
  {
    ReindexingPartitioningRule otherRule = new ReindexingPartitioningRule(
        "rule-2",
        "Other rule",
        Period.days(14),
        Granularities.HOUR,
        new DynamicPartitionsSpec(1000000, null),
        null
    );
    IntervalPartitioningInfo info1 = new IntervalPartitioningInfo(INTERVAL, SOURCE_RULE);
    IntervalPartitioningInfo info2 = new IntervalPartitioningInfo(INTERVAL, otherRule);

    Assertions.assertNotEquals(info1, info2);
  }
}
