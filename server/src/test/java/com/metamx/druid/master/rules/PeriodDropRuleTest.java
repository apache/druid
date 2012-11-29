package com.metamx.druid.master.rules;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class PeriodDropRuleTest
{
  @Test
  public void testAppliesToAll()
  {
    DateTime now = new DateTime();
    PeriodDropRule rule = new PeriodDropRule(
        "all"
    );

    Assert.assertTrue(rule.appliesTo(new Interval(now.minusDays(2), now.minusDays(1))));
    Assert.assertTrue(rule.appliesTo(new Interval(now.minusYears(100), now.minusDays(1))));
  }

  @Test
  public void testAppliesToPeriod()
  {
    DateTime now = new DateTime();
    PeriodDropRule rule = new PeriodDropRule(
        "P1M"
    );

    Assert.assertTrue(rule.appliesTo(new Interval(now.minusWeeks(1), now.minusDays(1))));
    Assert.assertFalse(rule.appliesTo(new Interval(now.minusYears(1), now.minusDays(1))));
    Assert.assertFalse(rule.appliesTo(new Interval(now.minusMonths(2), now.minusDays(1))));
  }
}
