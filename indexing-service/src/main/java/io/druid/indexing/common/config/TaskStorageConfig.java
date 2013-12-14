package io.druid.indexing.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;

public class TaskStorageConfig
{
  @JsonProperty
  @NotNull
  public Duration recentlyFinishedThreshold = new Period("PT24H").toStandardDuration();

  public Duration getRecentlyFinishedThreshold()
  {
    return recentlyFinishedThreshold;
  }
}
