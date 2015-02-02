/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.server.coordinator.rules;

import com.google.common.collect.ImmutableMap;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class PeriodLoadRuleTest
{
  private final static DataSegment.Builder builder = DataSegment.builder()
                                                            .dataSource("test")
                                                            .version(new DateTime().toString())
                                                            .shardSpec(new NoneShardSpec());

  @Test
  public void testAppliesToAll()
  {
    DateTime now = new DateTime("2013-01-01");
    PeriodLoadRule rule = new PeriodLoadRule(
        new Period("P5000Y"),
        ImmutableMap.<String, Integer>of("", 0)
    );

    Assert.assertTrue(rule.appliesTo(builder.interval(new Interval("2012-01-01/2012-12-31")).build(), now));
    Assert.assertTrue(rule.appliesTo(builder.interval(new Interval("1000-01-01/2012-12-31")).build(), now));
    Assert.assertTrue(rule.appliesTo(builder.interval(new Interval("0500-01-01/2100-12-31")).build(), now));
  }

  @Test
  public void testAppliesToPeriod()
  {
    DateTime now = new DateTime("2012-12-31T01:00:00");
    PeriodLoadRule rule = new PeriodLoadRule(
        new Period("P1M"),
        ImmutableMap.<String, Integer>of("", 0)
    );

    Assert.assertTrue(rule.appliesTo(builder.interval(new Interval(now.minusWeeks(1), now)).build(), now));
    Assert.assertTrue(
        rule.appliesTo(
            builder.interval(new Interval(now.minusDays(1), now.plusDays(1)))
                   .build(),
            now
        )
    );
    Assert.assertFalse(
        rule.appliesTo(
            builder.interval(new Interval(now.plusDays(1), now.plusDays(2)))
                       .build(),
            now
        )
    );
  }
}
