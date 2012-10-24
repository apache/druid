package com.metamx.druid.realtime;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    @JsonSubTypes.Type(name = "realtime", value = RealtimePlumberSchool.class)
)
public interface PlumberSchool
{
  /**
   * Creates a Plumber
   *
   * @return returns a plumber
   */
  public Plumber findPlumber(Schema schema, FireDepartmentMetrics metrics);
}
