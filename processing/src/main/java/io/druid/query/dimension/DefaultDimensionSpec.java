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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.StringUtils;
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
                return new DefaultDimensionSpec(input, input);
              }
            }
        )
    );
  }

  private static final byte CACHE_TYPE_ID = 0x0;
  private final String dimension;
  private final String outputName;

  @JsonCreator
  public DefaultDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") String outputName
  )
  {
    this.dimension = dimension;

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
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);

    return ByteBuffer.allocate(1 + dimensionBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .array();
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
           "dimension='" + dimension + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    // LegacyDimensionSpec can be equal to DefaultDimensionSpec
    if (!(o instanceof DefaultDimensionSpec)) {
      return false;
    }

    DefaultDimensionSpec that = (DefaultDimensionSpec) o;

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
    result = 31 * result + (outputName != null ? outputName.hashCode() : 0);
    return result;
  }
}
