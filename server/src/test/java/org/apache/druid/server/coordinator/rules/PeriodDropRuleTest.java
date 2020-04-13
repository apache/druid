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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class PeriodDropRuleTest
{
  private static final DataSegment.Builder BUILDER = DataSegment
      .builder()
      .dataSource("test")
      .version(DateTimes.of("2012-12-31T01:00:00").toString())
      .shardSpec(NoneShardSpec.instance())
      .size(0);

  @Test
  public void testAppliesToAll()
  {
    DateTime now = DateTimes.of("2012-12-31T01:00:00");
    PeriodDropRule rule = new PeriodDropRule(
        new Period("P5000Y"),
        false
    );

    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(
                new Interval(
                    now.minusDays(2),
                    now.minusDays(1)
                )
            ).build(),
            now
        )
    );
    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusYears(100), now.minusDays(1)))
                       .build(),
            now
        )
    );
  }

  @Test
  public void testAppliesToPeriod()
  {
    DateTime now = DateTimes.of("2012-12-31T01:00:00");
    PeriodDropRule rule = new PeriodDropRule(
        new Period("P1M"),
        false
    );

    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusWeeks(1), now.minusDays(1)))
                       .build(),
            now
        )
    );
    Assert.assertTrue(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusDays(1), now))
                   .build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusYears(1), now.minusDays(1)))
                       .build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            BUILDER.interval(new Interval(now.minusMonths(2), now.minusDays(1)))
                       .build(),
            now
        )
    );
  }

  @Test
  public void testIncludeFuture()
  {
    DateTime now = DateTimes.of("2012-12-31T01:00:00");
    PeriodDropRule includeFutureRule = new PeriodDropRule(
        new Period("P2D"),
        true
    );
    PeriodDropRule notIncludeFutureRule = new PeriodDropRule(
        new Period("P2D"),
        false
    );

    Assert.assertTrue(
        includeFutureRule.appliesTo(
            BUILDER.interval(new Interval(now.plusDays(1), now.plusDays(2))).build(),
            now
        )
    );
    Assert.assertFalse(
        notIncludeFutureRule.appliesTo(
            BUILDER.interval(new Interval(now.plusDays(1), now.plusDays(2))).build(),
            now
        )
    );
  }
}
