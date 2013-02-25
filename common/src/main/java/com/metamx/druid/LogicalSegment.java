package com.metamx.druid;

import org.joda.time.Interval;

public interface LogicalSegment
{
  public Interval getInterval();
}
