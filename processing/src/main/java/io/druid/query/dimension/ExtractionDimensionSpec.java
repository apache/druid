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
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;

/**
 */
public class ExtractionDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final String dimension;
  private final ExtractionFn extractionFn;
  private final String outputName;

  @JsonCreator
  public ExtractionDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      // for backwards compatibility
      @Deprecated @JsonProperty("dimExtractionFn") ExtractionFn dimExtractionFn
  )
  {
    Preconditions.checkNotNull(dimension, "dimension must not be null");
    Preconditions.checkArgument(extractionFn != null || dimExtractionFn != null, "extractionFn must not be null");

    this.dimension = dimension;
    this.extractionFn = extractionFn != null ? extractionFn : dimExtractionFn;

    // Do null check for backwards compatibility
    this.outputName = outputName == null ? dimension : outputName;
  }

  public ExtractionDimensionSpec(String dimension, String outputName, ExtractionFn extractionFn)
  {
    this(dimension, outputName, extractionFn, null);
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
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    byte[] dimExtractionFnBytes = extractionFn.getCacheKey();

    return ByteBuffer.allocate(1 + dimensionBytes.length + dimExtractionFnBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .put(dimExtractionFnBytes)
                     .array();
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
           "dimension='" + dimension + '\'' +
           ", extractionFn=" + extractionFn +
           ", outputName='" + outputName + '\'' +
           '}';
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

    ExtractionDimensionSpec that = (ExtractionDimensionSpec) o;

    if (extractionFn != null ? !extractionFn.equals(that.extractionFn) : that.extractionFn != null) {
      return false;
    }
    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
      return false;
    }
    if (outputName != null ? !outputName.equals(that.outputName) : that.outputName != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    result = 31 * result + (outputName != null ? outputName.hashCode() : 0);
    return result;
  }
}
