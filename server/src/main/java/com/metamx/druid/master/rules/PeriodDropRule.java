package com.metamx.druid.master.rules;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

/**
 */
public class PeriodDropRule implements DropRule
{
  private final Period period;

  @JsonCreator
  public PeriodDropRule(
      @JsonProperty("period") String period
  )
  {
    this.period = (period == null || period.equalsIgnoreCase("all")) ? new Period("P1000Y") : new Period(period);
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "dropByPeriod";
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  @Override
  public boolean appliesTo(Interval interval)
  {
    final Interval currInterval = new Interval(new DateTime().minus(period), period);
    return currInterval.contains(interval);
  }
}
