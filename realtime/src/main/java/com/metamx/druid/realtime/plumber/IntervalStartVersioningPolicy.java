package com.metamx.druid.realtime.plumber;

import org.joda.time.Interval;

public class IntervalStartVersioningPolicy implements VersioningPolicy
{
  @Override
  public String getVersion(Interval interval)
  {
    return interval.getStart().toString();
  }
}
