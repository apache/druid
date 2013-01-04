/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.master.rules;

import com.metamx.druid.client.DataSegment;
import com.metamx.druid.shard.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class PeriodDropRuleTest
{
  private final static DataSegment.Builder builder = DataSegment.builder()
                                                          .dataSource("test")
                                                          .version(new DateTime("2012-12-31T01:00:00").toString())
                                                          .shardSpec(new NoneShardSpec());

  @Test
  public void testAppliesToAll()
  {
    DateTime now = new DateTime("2012-12-31T01:00:00");
    PeriodDropRule rule = new PeriodDropRule(
        new Period("P5000Y")
    );

    Assert.assertTrue(
        rule.appliesTo(
            builder.interval(
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
            builder.interval(new Interval(now.minusYears(100), now.minusDays(1)))
                       .build(),
            now
        )
    );
  }

  @Test
  public void testAppliesToPeriod()
  {
    DateTime now = new DateTime("2012-12-31T01:00:00");
    PeriodDropRule rule = new PeriodDropRule(
        new Period("P1M")
    );

    Assert.assertTrue(
        rule.appliesTo(
            builder.interval(new Interval(now.minusWeeks(1), now.minusDays(1)))
                       .build(),
            now
        )
    );
    Assert.assertTrue(
        rule.appliesTo(
            builder.interval(new Interval(now.minusDays(1), now))
                   .build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            builder.interval(new Interval(now.minusYears(1), now.minusDays(1)))
                       .build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            builder.interval(new Interval(now.minusMonths(2), now.minusDays(1)))
                       .build(),
            now
        )
    );
  }
}
