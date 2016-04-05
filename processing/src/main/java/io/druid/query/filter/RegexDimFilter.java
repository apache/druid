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
import io.druid.segment.filter.RegexFilter;

import java.nio.ByteBuffer;

/**
 */
public class RegexDimFilter implements DimFilter
{
  private final String dimension;
  private final String pattern;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public RegexDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("pattern") String pattern,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(pattern != null, "pattern must not be null");
    this.dimension = dimension;
    this.pattern = pattern;
    this.extractionFn = extractionFn;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
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
    final byte[] patternBytes = StringUtils.toUtf8(pattern);
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + patternBytes.length + extractionFnBytes.length)
                     .put(DimFilterCacheHelper.REGEX_CACHE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(patternBytes)
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
    return new RegexFilter(dimension, pattern, extractionFn);
  }

  @Override
  public String toString()
  {
    return "RegexDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", pattern='" + pattern + '\'' +
           ", extractionFn='" + extractionFn + '\'' +
           '}';
  }
}
