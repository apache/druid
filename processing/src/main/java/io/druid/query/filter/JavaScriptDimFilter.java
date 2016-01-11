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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class JavaScriptDimFilter implements DimFilter
{
  private final String[] dimensions;
  private final String function;

  @JsonCreator
  public JavaScriptDimFilter(
      // for backwards compatibility
      @JsonProperty("dimension") String dimension,
      @JsonProperty("dimensions") String[] dimensions,
      @JsonProperty("function") String function
  )
  {
    Preconditions.checkArgument(dimension != null ^ dimensions != null, "dimensions(xor dimension) must not be null");
    Preconditions.checkArgument(function != null, "function must not be null");
    this.dimensions = (dimensions != null) ? dimensions : new String[]{dimension};
    this.function = function;
  }

  @JsonProperty
  public String[] getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] functionBytes = StringUtils.toUtf8(function);
    byte[][] dimensionsBytes = new byte[dimensions.length][];
    int totalDimensionsBytes = 0;

    for (int idx = 0; idx < dimensions.length; idx++) {
      dimensionsBytes[idx] = StringUtils.toUtf8(dimensions[idx]);
      totalDimensionsBytes += dimensionsBytes[idx].length;
    }

    ByteBuffer byteBuffer = ByteBuffer.allocate(2 + dimensions.length + totalDimensionsBytes + functionBytes.length)
                     .put(DimFilterCacheHelper.JAVASCRIPT_CACHE_ID);
    for (byte[] dimBytes: dimensionsBytes) {
      byteBuffer.put(dimBytes)
                .put(DimFilterCacheHelper.STRING_SEPARATOR);
    }
    return byteBuffer.put(functionBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public String toString()
  {
    String dimensionString = (dimensions.length == 1) ? "dimension='" + dimensions[0] + '\''
                                                      : "dimensions=['" + Joiner.on("', '").join(dimensions) + "']";
    return "JavaScriptDimFilter{" +
           dimensionString +
           ", function='" + function + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JavaScriptDimFilter)) {
      return false;
    }

    JavaScriptDimFilter that = (JavaScriptDimFilter) o;

    if (!function.equals(that.function)) {
      return false;
    }
    return Arrays.equals(that.dimensions, dimensions);
  }
}
