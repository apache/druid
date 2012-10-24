package com.metamx.druid.query.dimension;

import com.metamx.druid.query.extraction.DimExtractionFn;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;

/**
 */
public class ExtractionDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final String dimension;
  private final DimExtractionFn dimExtractionFn;
  private final String outputName;

  @JsonCreator
  public ExtractionDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("dimExtractionFn") DimExtractionFn dimExtractionFn
  )
  {
    this.dimension = dimension.toLowerCase();
    this.dimExtractionFn = dimExtractionFn;

    // Do null check for backwards compatibility
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
  @JsonProperty
  public DimExtractionFn getDimExtractionFn()
  {
    return dimExtractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = dimension.getBytes();
    byte[] dimExtractionFnBytes = dimExtractionFn.getCacheKey();

    return ByteBuffer.allocate(1 + dimensionBytes.length + dimExtractionFnBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .put(dimExtractionFnBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "ExtractionDimensionSpec{" +
           "dimension='" + dimension + '\'' +
           ", dimExtractionFn=" + dimExtractionFn +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
