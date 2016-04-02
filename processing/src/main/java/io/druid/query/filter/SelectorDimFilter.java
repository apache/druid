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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.metamx.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.lookup.LookupExtractor;
import io.druid.segment.filter.SelectorFilter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 */
public class SelectorDimFilter implements DimFilter
{
  private final String dimension;
  private final String value;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public SelectorDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");

    this.dimension = dimension;
    this.value = value;
    this.extractionFn = extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    byte[] valueBytes = (value == null) ? new byte[]{} : StringUtils.toUtf8(value);
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + valueBytes.length + extractionFnBytes.length)
                     .put(DimFilterCacheHelper.SELECTOR_CACHE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valueBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(extractionFnBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    if (this.getExtractionFn() instanceof LookupExtractionFn
        && ((LookupExtractionFn) this.getExtractionFn()).isOptimize()) {
      LookupExtractionFn exFn = (LookupExtractionFn) this.getExtractionFn();
      LookupExtractor lookup = exFn.getLookup();

      final String convertedValue = Strings.emptyToNull(value);

      // We cannot do an unapply()-based optimization if the selector value
      // and the replaceMissingValuesWith value are the same, since we have to match on
      // all values that are not present in the lookup.
      if (!exFn.isRetainMissingValue() && Objects.equals(convertedValue, exFn.getReplaceMissingValueWith())) {
        return this;
      }

      final String mappingForValue = lookup.apply(convertedValue);
      final List<String> keys = new ArrayList<>();
      keys.addAll(lookup.unapply(convertedValue));

      // If retainMissingValues is true and the selector value is not in the lookup map,
      // there may be row values that match the selector value but are not included
      // in the lookup map. Match on the selector value as well.
      // If the selector value is overwritten in the lookup map, don't add selector value to keys.
      if (exFn.isRetainMissingValue() && mappingForValue == null) {
        keys.add(convertedValue);
      }

      if (keys.isEmpty()) {
        return this;
      } else if (keys.size() == 1) {
        return new SelectorDimFilter(dimension, keys.get(0), null);
      } else {
        return new InDimFilter(dimension, keys, null);
      }
    }
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new SelectorFilter(dimension, value, extractionFn);
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
  public String toString()
  {
    if (extractionFn != null) {
      return String.format("%s(%s) = %s", extractionFn, dimension, value);
    } else {
      return String.format("%s = %s", dimension, value);
    }
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

    SelectorDimFilter that = (SelectorDimFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;
  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }
}
