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
import com.google.common.collect.RangeSet;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;

import java.nio.ByteBuffer;

/**
 * This class is deprecated, use SelectorDimFilter instead: {@link SelectorDimFilter}
 */
@Deprecated
public class ExtractionDimFilter implements DimFilter
{
  private final String dimension;
  private final String value;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public ExtractionDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      // for backwards compatibility
      @Deprecated @JsonProperty("dimExtractionFn") ExtractionFn dimExtractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(
        extractionFn != null || dimExtractionFn != null,
        "extraction function must not be null"
    );

    this.dimension = dimension;
    this.value = value;
    this.extractionFn = extractionFn != null ? extractionFn : dimExtractionFn;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    byte[] valueBytes = value == null ? new byte[0] : StringUtils.toUtf8(value);
    byte[] extractionFnBytes = extractionFn.getCacheKey();
    return ByteBuffer.allocate(3 + dimensionBytes.length + valueBytes.length + extractionFnBytes.length)
                     .put(DimFilterUtils.EXTRACTION_CACHE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(valueBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(extractionFnBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return new SelectorDimFilter(dimension, value, extractionFn).optimize();
  }

  @Override
  public Filter toFilter()
  {
    return new SelectorDimFilter(dimension, value, extractionFn).toFilter();
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s(%s) = %s", extractionFn, dimension, value);
  }
}
