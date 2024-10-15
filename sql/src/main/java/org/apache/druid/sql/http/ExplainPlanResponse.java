package org.apache.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ExplainPlanResponse
{
  @JsonProperty("PLAN")
  private final String plan;
  @JsonProperty("RESOURCES")
  private final String resources;
  // TODO: investigate why changing the type from String to ExplainAttributes doesn't work.
  @JsonProperty("ATTRIBUTES")
  private final String attributes;

  @JsonCreator
  public ExplainPlanResponse(
      @JsonProperty("PLAN") String plan,
      @JsonProperty("RESOURCES") String resources,
      @JsonProperty("ATTRIBUTES") String attributes
  ) {
    this.plan = plan;
    this.resources = resources;
    this.attributes = attributes;
  }

  public String getPlan() {
    return plan;
  }

  public String getResources() {
    return resources;
  }

  public String getAttributes() {
    return attributes;
  }
}
