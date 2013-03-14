package com.metamx.druid.realtime.plumber;

import org.joda.time.DateTime;
import org.joda.time.Period;

public class ServerTimeRejectionPolicyFactory implements RejectionPolicyFactory
{
  @Override
  public RejectionPolicy create(final Period windowPeriod)
  {
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

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
        return timestamp >= (System.currentTimeMillis() - windowMillis);
      }

      @Override
      public String toString()
      {
        return String.format("serverTime-%s", windowPeriod);
      }
    };
  }
}
