package com.metamx.druid.query.filter;

import com.metamx.druid.query.extraction.DimExtractionFn;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;

/**
 */
public class ExtractionDimFilter implements DimFilter
{
  private static final byte CACHE_TYPE_ID = 0x4;

  private final String dimension;
  private final String value;
  private final DimExtractionFn dimExtractionFn;

  @JsonCreator
  public ExtractionDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("dimExtractionFn") DimExtractionFn dimExtractionFn
  )
  {
    this.dimension = dimension.toLowerCase();
    this.value = value;
    this.dimExtractionFn = dimExtractionFn;
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

  @JsonProperty
  public DimExtractionFn getDimExtractionFn()
  {
    return dimExtractionFn;
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

  @Override
  public String toString()
  {
    return String.format("%s(%s) = %s", dimExtractionFn, dimension, value);
  }
}
