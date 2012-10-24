package com.metamx.druid.aggregation.post;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Comparator;
import java.util.Map;

/**
 */
public class FieldAccessPostAggregator implements PostAggregator
{
  private final String name;
  private final String fieldName;

  @JsonCreator
  public FieldAccessPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return combinedAggregators.get(fieldName);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public String toString()
  {
    return "FieldAccessPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
