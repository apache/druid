package com.metamx.druid.index.v1;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;

/**
 */
public class SpatialDimensionSchema
{
  private final String dimName;
  private final List<String> dims;

  @JsonCreator
  public SpatialDimensionSchema(
      @JsonProperty("dimName") String dimName,
      @JsonProperty("dims") List<String> dims
  )
  {
    this.dimName = dimName.toLowerCase();
    this.dims = Lists.transform(
        dims,
        new Function<String, String>()
        {
          @Override
          public String apply(String input)
          {
            return input.toLowerCase();
          }
        }
    );
  }

  @JsonProperty
  public String getDimName()
  {
    return dimName;
  }

  @JsonProperty
  public List<String> getDims()
  {
    return dims;
  }
}
