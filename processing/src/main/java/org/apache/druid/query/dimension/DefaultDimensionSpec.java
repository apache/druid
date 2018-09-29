/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 */
public class DefaultDimensionSpec implements DimensionSpec
{
  public static DefaultDimensionSpec of(String dimensionName)
  {
    return new DefaultDimensionSpec(dimensionName, dimensionName);
  }

  public static List<DimensionSpec> toSpec(String... dimensionNames)
  {
    return toSpec(Arrays.asList(dimensionNames));
  }

  public static List<DimensionSpec> toSpec(Iterable<String> dimensionNames)
  {
    return StreamSupport.stream(dimensionNames.spliterator(), false)
                        .map(input -> new DefaultDimensionSpec(input, input))
                        .collect(Collectors.toList());
  }

  private static final byte CACHE_TYPE_ID = 0x0;
  private final String dimension;
  private final String outputName;
  private final ValueType outputType;

  @JsonCreator
  public DefaultDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") @Nullable String outputName,
      @JsonProperty("outputType") @Nullable ValueType outputType
  )
  {
    this.dimension = dimension;
    this.outputType = outputType == null ? ValueType.STRING : outputType;

    // Do null check for legacy backwards compatibility, callers should be setting the value.
    this.outputName = outputName == null ? dimension : outputName;
  }

  public DefaultDimensionSpec(String dimension, String outputName)
  {
    this(dimension, outputName, ValueType.STRING);
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
  public ValueType getOutputType()
  {
    return outputType;
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
  public boolean mustDecorate()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(CACHE_TYPE_ID)
        .appendString(dimension)
        .appendString(outputType.toString())
        .build();
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
           ", outputType='" + outputType + '\'' +
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
    if (outputType != null ? !outputType.equals(that.outputType) : that.outputType != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (outputName != null ? outputName.hashCode() : 0);
    result = 31 * result + (outputType != null ? outputType.hashCode() : 0);
    return result;
  }
}
