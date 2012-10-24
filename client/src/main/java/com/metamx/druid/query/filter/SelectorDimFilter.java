package com.metamx.druid.query.filter;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;

/**
 */
public class SelectorDimFilter implements DimFilter
{
  private static final byte CACHE_TYPE_ID = 0x0;

  private final String dimension;
  private final String value;

  @JsonCreator
  public SelectorDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value
  )
  {
    this.dimension = dimension.toLowerCase();
    this.value = value;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = dimension.getBytes();
    byte[] valueBytes = value.getBytes();

    return ByteBuffer.allocate(1 + dimensionBytes.length + valueBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .put(valueBytes)
                     .array();
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SelectorDimFilter that = (SelectorDimFilter) o;

    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
      return false;
    }
    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return String.format("%s = %s", dimension, value);
  }
}
