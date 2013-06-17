package com.metamx.druid.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;

/**
 */
public class ConfigManagerConfig
{
  @JsonProperty
  @NotNull
  private Period pollDuration = new Period("PT1M");

  public Period getPollDuration()
  {
    return pollDuration;
  }
}
