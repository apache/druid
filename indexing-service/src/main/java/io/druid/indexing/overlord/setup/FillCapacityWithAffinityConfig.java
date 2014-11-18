package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Maps;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 */
public class FillCapacityWithAffinityConfig
{
  @JsonProperty
  @NotNull
  // key:Datasource, value:[nodeHostNames]
  private Map<String, List<String>> preferences = Maps.newHashMap();

  public Map<String, List<String>> getPreferences()
  {
    return preferences;
  }
}
