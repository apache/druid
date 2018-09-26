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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.filter.DimensionPredicateFilter;
import org.apache.hive.common.util.BloomKFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;

/**
 */
public class BloomDimFilter implements DimFilter
{

  private final String dimension;
  private final BloomKFilter bloomKFilter;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public BloomDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bloomKFilter") BloomKFilter bloomKFilter,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkNotNull(bloomKFilter);
    this.dimension = dimension;
    this.bloomKFilter = bloomKFilter;
    this.extractionFn = extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      BloomKFilter.serialize(byteArrayOutputStream, bloomKFilter);
    }
    catch (IOException e) {
      throw new IllegalStateException(StringUtils.format("Exception when generating cache key for [%s]", this), e);
    }
    byte[] bloomFilterBytes = byteArrayOutputStream.toByteArray();
    return new CacheKeyBuilder(DimFilterUtils.BLOOM_DIM_FILTER_CACHE_ID)
        .appendString(dimension)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(extractionFn == null ? new byte[0] : extractionFn.getCacheKey())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(bloomFilterBytes)
        .build();
  }


  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new DimensionPredicateFilter(
        dimension,
        new DruidPredicateFactory()
        {
          @Override
          public Predicate<String> makeStringPredicate()
          {
            return str -> {
              if (str == null) {
                return bloomKFilter.testBytes(null, 0, 0);
              }
              return bloomKFilter.testString(str);
            };
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return bloomKFilter.testLong(input);
              }

              @Override
              public boolean applyNull()
              {
                return bloomKFilter.testBytes(null, 0, 0);
              }
            };
          }

          @Override
          public DruidFloatPredicate makeFloatPredicate()
          {
            return new DruidFloatPredicate()
            {
              @Override
              public boolean applyFloat(float input)
              {
                return bloomKFilter.testFloat(input);
              }

              @Override
              public boolean applyNull()
              {
                return bloomKFilter.testBytes(null, 0, 0);
              }
            };
          }

          @Override
          public DruidDoublePredicate makeDoublePredicate()
          {
            return new DruidDoublePredicate()
            {
              @Override
              public boolean applyDouble(double input)
              {
                return bloomKFilter.testDouble(input);
              }

              @Override
              public boolean applyNull()
              {
                return bloomKFilter.testBytes(null, 0, 0);
              }
            };
          }
        },
        extractionFn
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

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public String toString()
  {
    if (extractionFn != null) {
      return StringUtils.format("%s(%s) = %s", extractionFn, dimension, bloomKFilter);
    } else {
      return StringUtils.format("%s = %s", dimension, bloomKFilter);
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

    BloomDimFilter that = (BloomDimFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (bloomKFilter != null ? !bloomKFilter.equals(that.bloomKFilter) : that.bloomKFilter != null) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public HashSet<String> getRequiredColumns()
  {
    return Sets.newHashSet(dimension);
  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + (bloomKFilter != null ? bloomKFilter.hashCode() : 0);
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }
}
