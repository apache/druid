package io.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VersionedSupervisorSpec
{
  private final SupervisorSpec spec;
  private final String version;

  @JsonCreator
  public VersionedSupervisorSpec(@JsonProperty("spec") SupervisorSpec spec, @JsonProperty("version") String version)
  {
    this.spec = spec;
    this.version = version;
  }

  @JsonProperty
  public SupervisorSpec getSpec()
  {
    return spec;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }
}
