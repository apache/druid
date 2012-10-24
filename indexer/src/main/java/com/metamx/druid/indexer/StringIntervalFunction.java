package com.metamx.druid.indexer;

import com.google.common.base.Function;
import org.joda.time.Interval;

/**
*/
class StringIntervalFunction implements Function<String, Interval>
{
  @Override
  public Interval apply(String input)
  {
    return new Interval(input);
  }
}
