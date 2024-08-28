package org.apache.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.ArrayList;

public class ExtensionDependencies {

  @JsonProperty("dependsOnDruidExtensions")
  private List<String> dependsOnDruidExtensions;

  public ExtensionDependencies() {
    this.dependsOnDruidExtensions = new ArrayList<>();
  }

  public ExtensionDependencies(@Nonnull final List<String> dependsOnDruidExtensions) {
    this.dependsOnDruidExtensions = dependsOnDruidExtensions;
  }

  public List<String> getDependsOnDruidExtensions() {
    return dependsOnDruidExtensions;
  }
}