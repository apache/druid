package com.metamx.druid.master.rules;

import com.metamx.common.logger.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Interval;

/**
 */
public class IntervalLoadRule implements LoadRule
{
  private static final Logger log = new Logger(IntervalLoadRule.class);

  private final Interval interval;
  private final Integer replicationFactor;
  private final String nodeType;

  @JsonCreator
  public IntervalLoadRule(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("replicationFactor") Integer replicationFactor,
      @JsonProperty("nodeType") String nodeType
  )
  {
    this.interval = interval;
    this.replicationFactor = (replicationFactor == null) ? 2 : replicationFactor; //TODO:config
    this.nodeType = nodeType;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadByInterval";
  }

  @Override
  @JsonProperty
  public int getReplicationFactor()
  {
    return replicationFactor;
  }

  @Override
  @JsonProperty
  public String getNodeType()
  {
    return nodeType;
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
