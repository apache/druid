package org.apache.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.ArrayList;

public class ExtensionDependencies {
  @JsonProperty("name")
  private String name;

  @JsonProperty("dependencies")
  private List<String> dependencies;

  // Default constructor
  public ExtensionDependencies() {
    this.dependencies = new ArrayList<>();
  }

  // Constructor with parameters
  public ExtensionDependencies(String name, List<String> dependencies) {
    this.name = name;
    this.dependencies = dependencies != null ? dependencies : new ArrayList<>();
  }

  // Getter for name
  public String getName() {
    return name;
  }

  // Setter for name
  public void setName(String name) {
    this.name = name;
  }

  // Getter for dependencies
  public List<String> getDependencies() {
    return dependencies;
  }

  // Setter for dependencies
  public void setDependencies(List<String> dependencies) {
    this.dependencies = dependencies != null ? dependencies : new ArrayList<>();
  }

  // toString method for easy printing
  @Override
  public String toString() {
    return "ExtensionDependencies{" +
        "name='" + name + '\'' +
        ", dependencies=" + dependencies +
        '}';
  }
}