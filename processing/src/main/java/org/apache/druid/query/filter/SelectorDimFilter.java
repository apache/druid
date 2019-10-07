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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.filter.DimensionPredicateFilter;
import org.apache.druid.segment.filter.SelectorFilter;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 */
public class SelectorDimFilter implements DimFilter
{
  private final String dimension;

  @Nullable
  private final String value;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;

  private final Object initLock = new Object();

  private DruidLongPredicate longPredicate;
  private DruidFloatPredicate floatPredicate;
  private DruidDoublePredicate druidDoublePredicate;

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
  public DimFilter optimize()
  {
    return new InDimFilter(dimension, Collections.singletonList(value), extractionFn, filterTuning).optimize();
  }

  @Override
  public Filter toFilter()
  {
    if (extractionFn == null) {
      return new SelectorFilter(dimension, value, filterTuning);
    } else {

      final DruidPredicateFactory predicateFactory = new DruidPredicateFactory()
      {
        @Override
        public Predicate<String> makeStringPredicate()
        {
          return Predicates.equalTo(value);
        }

        @Override
        public DruidLongPredicate makeLongPredicate()
        {
          initLongPredicate();
          return longPredicate;
        }

        @Override
        public DruidFloatPredicate makeFloatPredicate()
        {
          initFloatPredicate();
          return floatPredicate;
        }

        @Override
        public DruidDoublePredicate makeDoublePredicate()
        {
          initDoublePredicate();
          return druidDoublePredicate;
        }
      };
      return new DimensionPredicateFilter(dimension, predicateFactory, extractionFn, filterTuning);
    }
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Nullable
  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Nullable
  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
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


  private void initLongPredicate()
  {
    if (longPredicate != null) {
      return;
    }
    synchronized (initLock) {
      if (longPredicate != null) {
        return;
      }
      if (value == null) {
        longPredicate = DruidLongPredicate.MATCH_NULL_ONLY;
        return;
      }
      final Long valueAsLong = GuavaUtils.tryParseLong(value);
      if (valueAsLong == null) {
        longPredicate = DruidLongPredicate.ALWAYS_FALSE;
      } else {
        // store the primitive, so we don't unbox for every comparison
        final long unboxedLong = valueAsLong.longValue();
        longPredicate = input -> input == unboxedLong;
      }
    }
  }

  private void initFloatPredicate()
  {
    if (floatPredicate != null) {
      return;
    }
    synchronized (initLock) {
      if (floatPredicate != null) {
        return;
      }

      if (value == null) {
        floatPredicate = DruidFloatPredicate.MATCH_NULL_ONLY;
        return;
      }
      final Float valueAsFloat = Floats.tryParse(value);

      if (valueAsFloat == null) {
        floatPredicate = DruidFloatPredicate.ALWAYS_FALSE;
      } else {
        final int floatBits = Float.floatToIntBits(valueAsFloat);
        floatPredicate = input -> Float.floatToIntBits(input) == floatBits;
      }
    }
  }

  private void initDoublePredicate()
  {
    if (druidDoublePredicate != null) {
      return;
    }
    synchronized (initLock) {
      if (druidDoublePredicate != null) {
        return;
      }
      if (value == null) {
        druidDoublePredicate = DruidDoublePredicate.MATCH_NULL_ONLY;
        return;
      }
      final Double aDouble = Doubles.tryParse(value);

      if (aDouble == null) {
        druidDoublePredicate = DruidDoublePredicate.ALWAYS_FALSE;
      } else {
        final long bits = Double.doubleToLongBits(aDouble);
        druidDoublePredicate = input -> Double.doubleToLongBits(input) == bits;
      }
    }
  }
}
