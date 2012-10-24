package com.metamx.druid.aggregation.post;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Comparator;
import java.util.Map;

/**
 */
public class ConstantPostAggregator implements PostAggregator
{
  private final String name;
  private final Number constantValue;

  @JsonCreator
  public ConstantPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("value") Number constantValue
  )
  {
    this.name = name;
    this.constantValue = constantValue;
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o1, Object o2)
      {
        return 0;
      }
    };
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return constantValue;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Number getConstantValue()
  {
    return constantValue;
  }

  @Override
  public String toString()
  {
    return "ConstantPostAggregator{" +
           "name='" + name + '\'' +
           ", constantValue=" + constantValue +
           '}';
  }
}
