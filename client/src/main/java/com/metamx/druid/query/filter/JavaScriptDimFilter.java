package com.metamx.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class JavaScriptDimFilter implements DimFilter
{
  private final String dimension;
  private final String function;

  @JsonCreator
  public JavaScriptDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("function") String function
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(function != null, "function must not be null");
    this.dimension = dimension;
    this.function = function;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = dimension.getBytes(Charsets.UTF_8);
    final byte[] functionBytes = function.getBytes(Charsets.UTF_8);

    return ByteBuffer.allocate(1 + dimensionBytes.length + functionBytes.length)
        .put(DimFilterCacheHelper.JAVASCRIPT_CACHE_ID)
        .put(dimensionBytes)
        .put(functionBytes)
        .array();
  }
}
