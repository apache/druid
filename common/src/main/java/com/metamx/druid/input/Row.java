package com.metamx.druid.input;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "version")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "v1", value = MapBasedRow.class)
})
public interface Row
{
  public long getTimestampFromEpoch();
  public List<String> getDimension(String dimension);
  public float getFloatMetric(String metric);
}
