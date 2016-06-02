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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 */
public class DefaultDimensionSpec implements DimensionSpec
{
  public static List<DimensionSpec> toSpec(String... dimensionNames)
  {
    return toSpec(Arrays.asList(dimensionNames));
  }

  public static List<DimensionSpec> toSpec(Iterable<String> dimensionNames)
  {
    return Lists.newArrayList(
        Iterables.transform(
            dimensionNames, new Function<String, DimensionSpec>()
            {
              @Override
              public DimensionSpec apply(String input)
              {
                List<String> dimensions = ImmutableList.of(input);
                return new DefaultDimensionSpec(dimensions, input);
              }
            }
        )
    );
  }

  private static final byte CACHE_TYPE_ID = 0x0;
  private final List<String> dimensions;
  private final String outputName;

  @JsonCreator
  public DefaultDimensionSpec(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("outputName") String outputName
  )
  {
    this.dimensions = dimensions;

    // Do null check for legacy backwards compatibility, callers should be setting the value.
    this.outputName = outputName == null ? dimensions.get(0) : outputName;
  }

  public DefaultDimensionSpec(
      String dimension,
      String outputName
  )
  {
    this.dimensions = ImmutableList.of(dimension);

    // Do null check for legacy backwards compatibility, callers should be setting the value.
    this.outputName = outputName == null ? dimension : outputName;
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
  public ExtractionFn getExtractionFn()
  {
    return null;
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

    ByteBuffer byteBuffer = ByteBuffer.allocate(1 + totalSize)
        .put(CACHE_TYPE_ID);
    for (int idx = 0; idx < dimensions.size(); idx++) {
      byteBuffer.put(dimensionBytes[idx]);
    }

    return byteBuffer.array();
  }

  @Override
  public boolean preservesOrdering()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "DefaultDimensionSpec{" +
           "dimensions='" + dimensions + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    // LegacyDimensionSpec can be equal to DefaultDimensionSpec
    if (!(o instanceof DefaultDimensionSpec)) return false;

    DefaultDimensionSpec that = (DefaultDimensionSpec) o;

    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) return false;
    if (outputName != null ? !outputName.equals(that.outputName) : that.outputName != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimensions != null ? dimensions.hashCode() : 0;
    result = 31 * result + (outputName != null ? outputName.hashCode() : 0);
    return result;
  }
}
