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
import com.metamx.common.Pair;
import com.metamx.common.StringUtils;
import io.druid.common.utils.SerializerUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.filter.ByRowJavaScriptFilter;
import io.druid.segment.filter.JavaScriptFilter;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class JavaScriptDimFilter implements DimFilter
{
  public static JavaScriptDimFilter of(String dimension, String function, ExtractionFn extractionFn)
  {
    return new JavaScriptDimFilter(dimension, extractionFn, function, null, null, false);
  }

  public static JavaScriptDimFilter of(String[] dimensions, String function, ExtractionFn[] extractionFns)
  {
    return new JavaScriptDimFilter(null, null, function, dimensions, extractionFns, false);
  }

  public static JavaScriptDimFilter byRow(String dimension, String function, ExtractionFn extractionFn)
  {
    return new JavaScriptDimFilter(dimension, extractionFn, function, null, null, true);
  }

  public static JavaScriptDimFilter byRow(String[] dimensions, String function, ExtractionFn[] extractionFns)
  {
    return new JavaScriptDimFilter(null, null, function, dimensions, extractionFns, true);
  }

  private final String[] dimensions;
  private final String function;
  private final ExtractionFn[] extractionFns;
  private final boolean byRow;

  @JsonCreator
  public JavaScriptDimFilter(
      // for backwards compatibility
      @JsonProperty("dimension") String dimension,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      @JsonProperty("function") String function,
      @JsonProperty("dimensions") String[] dimensions,
      @JsonProperty("extractionFns") ExtractionFn[] extractionFns,
      @JsonProperty("byRow") boolean byRow
  )
  {
    Preconditions.checkArgument(dimension != null ^ dimensions != null, "dimensions(xor dimension) must not be null");
    Preconditions.checkArgument(function != null, "function must not be null");
    this.dimensions = dimensions != null ? dimensions : new String[]{dimension};
    this.function = function;
    this.extractionFns = dimension == null ? extractionFns
                                           : extractionFn == null ? null : new ExtractionFn[]{extractionFn};
    this.byRow = byRow;
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

  @JsonProperty
  public ExtractionFn[] getExtractionFn()
  {
    return extractionFns;
  }

  public boolean isByRow()
  {
    return byRow;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] functionBytes = StringUtils.toUtf8(function);
    Pair<Integer, byte[][]> extractionFnBytes = SerializerUtils.serializeUTFs(extractionFns);
    Pair<Integer, byte[][]> dimensionsBytes = SerializerUtils.serializeUTFs(dimensions);

    ByteBuffer byteBuffer = ByteBuffer.allocate(
        2
        + dimensionsBytes.rhs.length + dimensionsBytes.lhs
        + functionBytes.length + 1
        + extractionFnBytes.rhs.length + extractionFnBytes.lhs
    );
    byteBuffer.put(DimFilterCacheHelper.JAVASCRIPT_CACHE_ID).put(byRow ? (byte) 1 : 0);

    for (byte[] dimBytes : dimensionsBytes.rhs) {
      byteBuffer.put(dimBytes).put(DimFilterCacheHelper.STRING_SEPARATOR);
    }
    byteBuffer.put(functionBytes).put(DimFilterCacheHelper.STRING_SEPARATOR);

    for (byte[] extractionFnByte : extractionFnBytes.rhs) {
      byteBuffer.put(extractionFnByte).put(DimFilterCacheHelper.STRING_SEPARATOR);
    }
    return byteBuffer.array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    if (byRow) {
      return new ByRowJavaScriptFilter(dimensions, function, extractionFns);
    } else {
      return new JavaScriptFilter(dimensions, function, extractionFns);
    }
  }

  @Override
  public String toString()
  {
    return "JavaScriptDimFilter{" +
           "dimensions=['" + Joiner.on("', '").join(dimensions) + "']" +
           ", function='" + function + '\'' +
           (extractionFns == null ? "" : ", extractionFns='" + Joiner.on("', '").join(extractionFns) + '\'') +
           ", byRow='" + byRow + '\'' +
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

    if (!Arrays.equals(dimensions, that.dimensions)) {
      return false;
    }
    if (byRow ^ that.byRow) {
      return false;
    }
    if (!function.equals(that.function)) {
      return false;
    }
    return Arrays.equals(extractionFns, that.extractionFns);
  }

  @Override
  public int hashCode()
  {
    int result = Arrays.hashCode(dimensions);
    result = 31 * result + (byRow ? 0 : 1);
    result = 31 * result + function.hashCode();
    result = 31 * result + Arrays.hashCode(extractionFns);
    return result;
  }
}
