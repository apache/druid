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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.filter.DimensionPredicateFilter;
import org.apache.druid.segment.filter.SelectorFilter;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class SelectorDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  private final String dimension;

  @Nullable
  private final String value;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;

  private final DruidPredicateFactory predicateFactory;

  @JsonCreator
  public SelectorDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");

    this.dimension = dimension;
    this.value = NullHandling.emptyToNullIfNeeded(value);
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;

    // Create this just in case "toFilter" needs it. It's okay to do this here, because initialization is lazy
    // (and therefore construction is cheap).
    this.predicateFactory = new SelectorPredicateFactory(this.value);
  }

  public SelectorDimFilter(String dimension, String value, @Nullable ExtractionFn extractionFn)
  {
    this(dimension, value, extractionFn, null);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(DimFilterUtils.SELECTOR_CACHE_ID)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(dimension)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByte(value == null ? NullHandling.IS_NULL_BYTE : NullHandling.IS_NOT_NULL_BYTE)
        .appendString(value)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(extractionFn == null ? new byte[0] : extractionFn.getCacheKey())
        .build();
  }

  @Override
  public DimFilter optimize(final boolean mayIncludeUnknown)
  {
    final InDimFilter.ValuesSet valuesSet = new InDimFilter.ValuesSet();
    valuesSet.add(value);
    return new InDimFilter(dimension, valuesSet, extractionFn, filterTuning).optimize(mayIncludeUnknown);
  }

  @Override
  public Filter toFilter()
  {
    if (extractionFn == null) {
      return new SelectorFilter(dimension, value, filterTuning);
    } else {
      return new DimensionPredicateFilter(dimension, predicateFactory, extractionFn, filterTuning);
    }
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  /**
   * Value to filter against. If {@code null}, then the meaning is `is null`.
   */
  @Nullable
  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public String toString()
  {
    return new DimFilterToStringBuilder().appendDimension(dimension, extractionFn)
                                         .appendEquals(value)
                                         .appendFilterTuning(filterTuning)
                                         .build();
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
    return dimension.equals(that.dimension) &&
           Objects.equals(value, that.value) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, value, extractionFn, filterTuning);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(getDimension(), dimension) || getExtractionFn() != null) {
      return null;
    }
    RangeSet<String> retSet = TreeRangeSet.create();
    String valueEquivalent = NullHandling.nullToEmptyIfNeeded(value);
    if (valueEquivalent == null) {
      // Case when SQL compatible null handling is enabled
      // Nulls are less than empty String in segments
      retSet.add(Range.lessThan(""));
    } else {
      retSet.add(Range.singleton(valueEquivalent));
    }
    return retSet;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }
}
