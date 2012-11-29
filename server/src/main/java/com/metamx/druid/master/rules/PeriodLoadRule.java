package com.metamx.druid.master.rules;

import com.metamx.common.logger.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

/**
 */
public class PeriodLoadRule implements LoadRule
{
  private static final Logger log = new Logger(PeriodLoadRule.class);

  private final Period period;
  private final Integer replicationFactor;
  private final String nodeType;

  @JsonCreator
  public PeriodLoadRule(
      @JsonProperty("period") String period,
      @JsonProperty("replicationFactor") Integer replicationFactor,
      @JsonProperty("nodeType") String nodeType
  )
  {
    this.period = (period == null || period.equalsIgnoreCase("all")) ? new Period("P1000Y") : new Period(period);
    this.replicationFactor = (replicationFactor == null) ? 2 : replicationFactor; //TODO:config
    this.nodeType = nodeType;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadByPeriod";
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  @JsonProperty
  public int getReplicationFactor()
  {
    return replicationFactor;
  }

  @JsonProperty
  public String getNodeType()
  {
    return nodeType;
  }

  @Override
  public boolean appliesTo(Interval interval)
  {
    final Interval currInterval = new Interval(new DateTime().minus(period), period);
    return currInterval.overlaps(interval);
  }
}
