package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;

/**
 */
public class SpatialDimensionSchema
{
  private final String dimName;
  private final List<String> dims;

  public SpatialDimensionSchema(String dimName, List<String> dims)
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

  public String getDimName()
  {
    return dimName;
  }

  public List<String> getDims()
  {
    return dims;
  }
}
