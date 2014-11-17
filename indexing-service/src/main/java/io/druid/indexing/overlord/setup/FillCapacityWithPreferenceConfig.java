package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Maps;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 */
public class FillCapacityWithPreferenceConfig
{
  @JsonProperty
  @NotNull
  // key:Datasource, value:nodeHostName
  private Map<String, String> preferences = Maps.newHashMap();

  public Map<String, String> getPreferences()
  {
    return preferences;
  }
}
