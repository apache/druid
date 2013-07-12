package com.metamx.druid.indexer.data;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.Parser;
import com.metamx.druid.index.v1.SpatialDimensionSchema;

/**
 * @author jan.rudert
 */
public class ProtoBufDataSpec implements DataSpec{
  private final List<String> dimensions;
  private final List<SpatialDimensionSchema> spatialDimensions;
  private final String descriptorFileInClasspath;

  @JsonCreator
  public ProtoBufDataSpec(
          @JsonProperty("descriptor") String descriptorFileInClasspath,
          @JsonProperty("dimensions") List<String> dimensions,
          @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions
  )
  {
    this.descriptorFileInClasspath = descriptorFileInClasspath;
    this.dimensions = dimensions;
    this.spatialDimensions = (spatialDimensions == null)
            ? Lists.<SpatialDimensionSchema>newArrayList()
            : spatialDimensions;

  }

  @JsonProperty("descriptor")
  public String getDescriptorFileInClassPath() {
    return descriptorFileInClasspath;
  }

  @JsonProperty("dimensions")
  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty("spatialDimensions")
  @Override
  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    return spatialDimensions;
  }

  @Override
  public void verify(List<String> usedCols)
  {
  }

  @Override
  public boolean hasCustomDimensions()
  {
    return !(dimensions == null || dimensions.isEmpty());
  }

  @Override
  public Parser<String, Object> getParser()
  {
    throw new UnsupportedOperationException("No String parser for protobuf data");
  }
}
