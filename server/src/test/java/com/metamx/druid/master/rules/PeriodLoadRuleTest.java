package com.metamx.druid.master.rules;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class PeriodLoadRuleTest
{
  @Test
  public void testAppliesToAll()
  {
    PeriodLoadRule rule = new PeriodLoadRule(
        "all",
        0,
        ""
    );

    Assert.assertTrue(rule.appliesTo(new Interval("2012-01-01/2012-12-31")));
    Assert.assertTrue(rule.appliesTo(new Interval("1000-01-01/2012-12-31")));
    Assert.assertTrue(rule.appliesTo(new Interval("0500-01-01/2100-12-31")));
  }

  @Test
  public void testAppliesToPeriod()
  {
    DateTime now = new DateTime();
    PeriodLoadRule rule = new PeriodLoadRule(
        "P1M",
        0,
        ""
    );

    Assert.assertTrue(rule.appliesTo(new Interval(now.minusWeeks(1), now)));
    Assert.assertTrue(rule.appliesTo(new Interval(now.minusDays(1), now.plusDays(1))));
    Assert.assertFalse(rule.appliesTo(new Interval(now.plusDays(1), now.plusDays(2))));
  }
}
