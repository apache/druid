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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import com.google.common.hash.HashCode;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.filter.DimensionPredicateFilter;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 */
public class BloomDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{

  private final String dimension;
  private final BloomKFilter bloomKFilter;
  private final HashCode hash;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;

  @JsonCreator
  public BloomDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bloomKFilter") BloomKFilterHolder bloomKFilterHolder,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkNotNull(bloomKFilterHolder);
    this.dimension = dimension;
    this.bloomKFilter = bloomKFilterHolder.getFilter();
    this.hash = bloomKFilterHolder.getFilterHash();
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;
  }

  @VisibleForTesting
  public BloomDimFilter(String dimension, BloomKFilterHolder bloomKFilterHolder, @Nullable ExtractionFn extractionFn)
  {
    this(dimension, bloomKFilterHolder, extractionFn, null);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(DimFilterUtils.BLOOM_DIM_FILTER_CACHE_ID)
        .appendString(dimension)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(extractionFn == null ? new byte[0] : extractionFn.getCacheKey())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(hash.asBytes())
        .build();
  }

  @Override
  public Filter toFilter()
  {
    return new DimensionPredicateFilter(
        dimension,
        new DruidPredicateFactory()
        {
          @Override
          public DruidObjectPredicate<String> makeStringPredicate()
          {
            return str -> {
              if (str == null) {
                return DruidPredicateMatch.of(bloomKFilter.testBytes(null, 0, 0));
              }
              return DruidPredicateMatch.of(bloomKFilter.testString(str));
            };
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return new DruidLongPredicate()
            {
              @Override
              public DruidPredicateMatch applyLong(long input)
              {
                return DruidPredicateMatch.of(bloomKFilter.testLong(input));
              }

              @Override
              public DruidPredicateMatch applyNull()
              {
                return DruidPredicateMatch.of(bloomKFilter.testBytes(null, 0, 0));
              }
            };
          }

          @Override
          public DruidFloatPredicate makeFloatPredicate()
          {
            return new DruidFloatPredicate()
            {
              @Override
              public DruidPredicateMatch applyFloat(float input)
              {
                return DruidPredicateMatch.of(bloomKFilter.testFloat(input));
              }

              @Override
              public DruidPredicateMatch applyNull()
              {
                return DruidPredicateMatch.of(bloomKFilter.testBytes(null, 0, 0));
              }
            };
          }

          @Override
          public DruidDoublePredicate makeDoublePredicate()
          {
            return new DruidDoublePredicate()
            {
              @Override
              public DruidPredicateMatch applyDouble(double input)
              {
                return DruidPredicateMatch.of(bloomKFilter.testDouble(input));
              }

              @Override
              public DruidPredicateMatch applyNull()
              {
                return DruidPredicateMatch.of(bloomKFilter.testBytes(null, 0, 0));
              }
            };
          }
        },
        extractionFn,
        filterTuning
    );
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public BloomKFilter getBloomKFilter()
  {
    return bloomKFilter;
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
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public String toString()
  {
    return new DimFilterToStringBuilder().appendDimension(dimension, extractionFn)
                                         .appendEquals(hash.toString())
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
    BloomDimFilter that = (BloomDimFilter) o;
    return dimension.equals(that.dimension) &&
           hash.equals(that.hash) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, hash, extractionFn, filterTuning);
  }
}
