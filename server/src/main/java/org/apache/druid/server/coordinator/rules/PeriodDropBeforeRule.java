package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

public class PeriodDropBeforeRule extends DropRule
{
  private final Period period;

  @JsonCreator
  public PeriodDropBeforeRule(
      @JsonProperty("period") Period period
  )
  {
    this.period = period;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "dropBeforeByPeriod";
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return appliesTo(segment.getInterval(), referenceTimestamp);
  }

  @Override
  public boolean appliesTo(Interval theInterval, DateTime referenceTimestamp)
  {
    final DateTime periodAgo = referenceTimestamp.minus(period);
    return theInterval.getEndMillis() <= periodAgo.getMillis();
  }
}
