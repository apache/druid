package org.apache.druid.guice;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.ArrayList;

public class ExtensionDependencies {
  @JsonProperty("name")
  final private String name;

  @JsonProperty("dependencies")
  final private List<String> dependencies;

  @JsonCreator
  public ExtensionDependencies(
      @JsonProperty("name") final String name,
      @JsonProperty("dependencies") final List<String> dependencies
  ) {
    this.name = name;
    this.dependencies = dependencies != null ? dependencies : new ArrayList<>();
  }

  public String getName() {
    return name;
  }

  public List<String> getDependencies() {
    return dependencies;
  }
}