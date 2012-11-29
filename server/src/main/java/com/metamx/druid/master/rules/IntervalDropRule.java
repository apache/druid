package com.metamx.druid.master.rules;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Interval;

/**
 */
public class IntervalDropRule implements DropRule
{
  private final Interval interval;

  @JsonCreator
  public IntervalDropRule(
      @JsonProperty("interval") Interval interval
  )
  {
    this.interval = interval;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "dropByInterval";
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public boolean appliesTo(Interval interval)
  {
    return this.interval.contains(interval);
  }
}
