package com.metamx.druid.query.dimension;

import com.metamx.druid.query.extraction.DimExtractionFn;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;

/**
 */
public class DefaultDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x0;
  private final String dimension;
  private final String outputName;

  @JsonCreator
  public DefaultDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") String outputName
  )
  {
    this.dimension = dimension.toLowerCase();

    // Do null check for legacy backwards compatibility, callers should be setting the value.
    this.outputName = outputName == null ? dimension : outputName;
  }

  @Override
  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public DimExtractionFn getDimExtractionFn()
  {
    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = dimension.getBytes();

    return ByteBuffer.allocate(1 + dimensionBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "DefaultDimensionSpec{" +
           "dimension='" + dimension + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
