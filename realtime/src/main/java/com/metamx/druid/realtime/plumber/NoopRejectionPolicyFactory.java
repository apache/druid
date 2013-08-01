package com.metamx.druid.realtime.plumber;

import org.joda.time.DateTime;
import org.joda.time.Period;

public class NoopRejectionPolicyFactory implements RejectionPolicyFactory
{
  @Override
  public RejectionPolicy create(Period windowPeriod)
  {
    return new RejectionPolicy()
    {
      @Override
      public DateTime getCurrMaxTime()
      {
        return new DateTime(0);
      }

      @Override
      public boolean accept(long timestamp)
      {
        return true;
      }
    };
  }
}
