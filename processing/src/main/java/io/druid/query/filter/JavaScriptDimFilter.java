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
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.filter.JavaScriptFilter;

import java.nio.ByteBuffer;

public class JavaScriptDimFilter implements DimFilter
{
  private final String dimension;
  private final String function;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public JavaScriptDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("function") String function,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(function != null, "function must not be null");
    this.dimension = dimension;
    this.function = function;
    this.extractionFn = extractionFn;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] functionBytes = StringUtils.toUtf8(function);
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + functionBytes.length + extractionFnBytes.length)
                     .put(DimFilterCacheHelper.JAVASCRIPT_CACHE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(functionBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(extractionFnBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new JavaScriptFilter(dimension, function, extractionFn);
  }

  @Override
  public String toString()
  {
    return "JavaScriptDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", function='" + function + '\'' +
           ", extractionFn='" + extractionFn + '\'' +
           '}';
  }
}
