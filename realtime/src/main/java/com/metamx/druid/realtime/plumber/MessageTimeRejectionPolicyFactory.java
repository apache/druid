package com.metamx.druid.realtime.plumber;

import org.joda.time.DateTime;
import org.joda.time.Period;

public class MessageTimeRejectionPolicyFactory implements RejectionPolicyFactory
{
  @Override
  public RejectionPolicy create(final Period windowPeriod)
  {
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    return new RejectionPolicy()
    {
      private volatile long maxTimestamp = Long.MIN_VALUE;

      @Override
      public DateTime getCurrMaxTime()
      {
        return new DateTime(maxTimestamp);
      }

      @Override
      public boolean accept(long timestamp)
      {
        maxTimestamp = Math.max(maxTimestamp, timestamp);

        return timestamp >= (maxTimestamp - windowMillis);
      }

      @Override
      public String toString()
      {
        return String.format("messageTime-%s", windowPeriod);
      }
    };
  }
}

