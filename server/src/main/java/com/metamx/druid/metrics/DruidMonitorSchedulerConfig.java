package com.metamx.druid.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.metrics.MonitorSchedulerConfig;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 */
public class DruidMonitorSchedulerConfig extends MonitorSchedulerConfig
{
  @JsonProperty
  private Period emissionPeriod = new Period("PT1M");

  @JsonProperty
  public Period getEmissionPeriod()
  {
    return emissionPeriod;
  }

  @Override
  public Duration getEmitterPeriod()
  {
    return emissionPeriod.toStandardDuration();
  }
}
