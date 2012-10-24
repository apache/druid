package com.metamx.druid.query.dimension;

import com.metamx.druid.query.extraction.DimExtractionFn;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacyDimensionSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultDimensionSpec.class),
    @JsonSubTypes.Type(name = "extraction", value = ExtractionDimensionSpec.class)
})
public interface DimensionSpec
{
  public String getDimension();
  public String getOutputName();
  public DimExtractionFn getDimExtractionFn();
  public byte[] getCacheKey();
}
