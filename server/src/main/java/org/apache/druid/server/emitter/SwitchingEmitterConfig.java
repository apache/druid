package org.apache.druid.server.emitter;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class SwitchingEmitterConfig
{

  @JsonProperty
  @NotNull
  private Map<String, List<String>> emitters = ImmutableMap.of();

  @JsonProperty
  private List<String> defaultEmitters = ImmutableList.of();

  public Map<String, List<String>> getEmitters()
  {
    return emitters;
  }

  public List<String> getDefaultEmitter()
  {
    return defaultEmitters;
  }
}