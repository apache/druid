package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Period;

public class TaskQueueConfig
{
  @JsonProperty
  private int maxSize;

  @JsonProperty
  private Duration startDelay;

  @JsonProperty
  private Duration restartDelay;

  @JsonProperty
  private Duration storageSyncRate;

  @JsonCreator
  public TaskQueueConfig(
      @JsonProperty("maxSize") final Integer maxSize,
      @JsonProperty("startDelay") final Period startDelay,
      @JsonProperty("restartDelay") final Period restartDelay,
      @JsonProperty("storageSyncRate") final Period storageSyncRate
  )
  {
    this.maxSize = maxSize == null ? Integer.MAX_VALUE : maxSize;
    this.startDelay = defaultDuration(startDelay, "PT1M");
    this.restartDelay = defaultDuration(restartDelay, "PT30S");
    this.storageSyncRate = defaultDuration(storageSyncRate, "PT1M");
  }

  public int getMaxSize()
  {
    return maxSize;
  }

  public Duration getStartDelay()
  {
    return startDelay;
  }

  public Duration getRestartDelay()
  {
    return restartDelay;
  }

  public Duration getStorageSyncRate()
  {
    return storageSyncRate;
  }

  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }
}
