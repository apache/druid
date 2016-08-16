/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.metamx.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class ExtractionDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final List<String> dimensions;
  private final ExtractionFn extractionFn;
  private final String outputName;

  @JsonCreator
  public ExtractionDimensionSpec(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      // for backwards compatibility
      @Deprecated @JsonProperty("dimExtractionFn") ExtractionFn dimExtractionFn
  )
  {
    Preconditions.checkNotNull(dimensions, "dimensions must not be null");
    Preconditions.checkArgument(extractionFn != null || dimExtractionFn != null, "extractionFn must not be null");
    Preconditions.checkArgument(extractionFn == null || ((dimensions.size() == extractionFn.arity()) || (extractionFn.arity() < 0)),
        "dimensions and extractionFn should have the same arity");
    Preconditions.checkArgument(dimExtractionFn == null || ((dimensions.size() == dimExtractionFn.arity()) || (dimExtractionFn.arity() < 0)),
        "dimensions and dimExtractionFn should have the same arity");

    this.dimensions = dimensions;
    this.extractionFn = extractionFn != null ? extractionFn : dimExtractionFn;

    // Do null check for backwards compatibility
    // for multi dimensional case, the first dimension name is set as a default outputName
    this.outputName = outputName == null ? dimensions.get(0) : outputName;
  }

  public ExtractionDimensionSpec(List<String> dimensions, String outputName, ExtractionFn extractionFn)
  {
    this(dimensions, outputName, extractionFn, null);
  }

  public ExtractionDimensionSpec(String dimension, String outputName, ExtractionFn extractionFn1, ExtractionFn extractionFn2)
  {
    this(ImmutableList.of(dimension), outputName, extractionFn1, extractionFn2);
  }

  public ExtractionDimensionSpec(String dimension, String outputName, ExtractionFn extractionFn)
  {
    this(ImmutableList.of(dimension), outputName, extractionFn);
  }

  @Override
  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public DimensionSelector decorate(DimensionSelector selector)
  {
    return selector;
  }

  @Override
  public byte[] getCacheKey()
  {
    int totalSize = 0;
    byte[][] dimensionBytes = new byte[dimensions.size()][];
    for (int idx = 0; idx < dimensions.size(); idx++) {
      String dimension = dimensions.get(idx);
      dimensionBytes[idx] = StringUtils.toUtf8(dimension);
      totalSize += dimensionBytes[idx].length;
    }
    byte[] dimExtractionFnBytes = extractionFn.getCacheKey();

    ByteBuffer byteBuffer = ByteBuffer.allocate(1 + totalSize + dimExtractionFnBytes.length)
                     .put(CACHE_TYPE_ID);
    for (int idx = 0; idx < dimensions.size(); idx++) {
      byteBuffer.put(dimensionBytes[idx]);
    }

    return byteBuffer.put(dimExtractionFnBytes).array();
  }

  @Override
  public boolean preservesOrdering()
  {
    return extractionFn.preservesOrdering();
  }

  @Override
  public String toString()
  {
    return "ExtractionDimensionSpec{" +
           "dimensions='" + dimensions + '\'' +
           ", extractionFn=" + extractionFn +
           ", outputName='" + outputName + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ExtractionDimensionSpec that = (ExtractionDimensionSpec) o;

    if (extractionFn != null ? !extractionFn.equals(that.extractionFn) : that.extractionFn != null)
      return false;
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) return false;
    if (outputName != null ? !outputName.equals(that.outputName) : that.outputName != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimensions != null ? dimensions.hashCode() : 0;
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    result = 31 * result + (outputName != null ? outputName.hashCode() : 0);
    return result;
  }
}
