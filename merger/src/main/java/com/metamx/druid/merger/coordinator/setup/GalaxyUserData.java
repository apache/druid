package com.metamx.druid.merger.coordinator.setup;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class GalaxyUserData implements WorkerUserData
{
  public final String env;
  public final String ver;
  public final String type;

  @JsonCreator
  public GalaxyUserData(
      @JsonProperty("env") String env,
      @JsonProperty("ver") String ver,
      @JsonProperty("type") String type
  )
  {
    this.env = env;
    this.ver = ver;
    this.type = type;
  }

  @JsonProperty
  public String getEnv()
  {
    return env;
  }

  @JsonProperty
  public String getVer()
  {
    return ver;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }
}
