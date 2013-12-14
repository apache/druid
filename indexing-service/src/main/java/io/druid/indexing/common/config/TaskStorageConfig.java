package io.druid.indexing.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Period;

public class TaskStorageConfig
{
  @JsonProperty
  private final Duration recentlyFinishedThreshold;

  @JsonCreator
  public TaskStorageConfig(
      @JsonProperty("recentlyFinishedThreshold") final Period recentlyFinishedThreshold
  )
  {
    this.recentlyFinishedThreshold = recentlyFinishedThreshold == null
                                     ? new Period("PT24H").toStandardDuration()
                                     : recentlyFinishedThreshold.toStandardDuration();
  }

  public Duration getRecentlyFinishedThreshold()
  {
    return recentlyFinishedThreshold;
  }
}
