package io.druid.segment;

import org.joda.time.Interval;

public interface LogicalSegment
{
  public Interval getInterval();
}
