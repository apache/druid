/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import io.druid.query.extraction.DimExtractionFn;

import java.nio.ByteBuffer;

/**
 */
public class ExtractionDimFilter implements DimFilter
{
  private final String dimension;
  private final String value;
  private final DimExtractionFn dimExtractionFn;

  @JsonCreator
  public ExtractionDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("dimExtractionFn") DimExtractionFn dimExtractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(value != null, "value must not be null");
    Preconditions.checkArgument(dimExtractionFn != null, "extraction function must not be null");

    this.dimension = dimension;
    this.value = value;
    this.dimExtractionFn = dimExtractionFn;
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
  public DimExtractionFn getDimExtractionFn()
  {
    return dimExtractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    byte[] valueBytes = StringUtils.toUtf8(value);

    return ByteBuffer.allocate(1 + dimensionBytes.length + valueBytes.length)
                     .put(DimFilterCacheHelper.EXTRACTION_CACHE_ID)
                     .put(dimensionBytes)
                     .put(valueBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return String.format("%s(%s) = %s", dimExtractionFn, dimension, value);
  }
}
