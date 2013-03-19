package com.metamx.druid.realtime.plumber;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

/**
 */
public class IntervalRejectionPolicyFactory implements RejectionPolicyFactory
{
  private final Interval interval;

  public IntervalRejectionPolicyFactory(Interval interval)
  {
    this.interval = interval;
  }

  @Override
  public RejectionPolicy create(Period windowPeriod)
  {
    return new RejectionPolicy()
    {
      @Override
      public DateTime getCurrMaxTime()
      {
        return new DateTime();
      }

      @Override
      public boolean accept(long timestamp)
      {
        return interval.contains(timestamp);
      }

      @Override
      public String toString()
      {
        return String.format("interval-%s", interval);
      }
    };
  }
}
