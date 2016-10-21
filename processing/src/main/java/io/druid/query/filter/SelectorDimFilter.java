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
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.filter.DimensionPredicateFilter;
import io.druid.segment.filter.SelectorFilter;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 */
public class SelectorDimFilter implements DimFilter
{
  private final String dimension;
  private final String value;
  private final ExtractionFn extractionFn;

  private final Object initLock = new Object();
  private volatile boolean longsInitialized = false;
  private volatile Long valueAsLong;


  @JsonCreator
  public SelectorDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");

    this.dimension = dimension;
    this.value = Strings.nullToEmpty(value);
    this.extractionFn = extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    byte[] valueBytes = (value == null) ? new byte[]{} : StringUtils.toUtf8(value);
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + valueBytes.length + extractionFnBytes.length)
                     .put(DimFilterUtils.SELECTOR_CACHE_ID)
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
    return new InDimFilter(dimension, ImmutableList.of(value), extractionFn).optimize();
  }

  @Override
  public Filter toFilter()
  {
    if (extractionFn == null) {
      return new SelectorFilter(dimension, value);
    } else {
      final String valueOrNull = Strings.emptyToNull(value);

      final DruidPredicateFactory predicateFactory = new DruidPredicateFactory()
      {
        @Override
        public Predicate<String> makeStringPredicate()
        {
          return new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              return Objects.equals(valueOrNull, input);
            }
          };
        }

        @Override
        public DruidLongPredicate makeLongPredicate()
        {
          initLongValue();

          if (valueAsLong == null) {
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return false;
              }
            };
          } else {
            // store the primitive, so we don't unbox for every comparison
            final long unboxedLong = valueAsLong.longValue();
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return input == unboxedLong;
              }
            };
          }
        }
      };
      return new DimensionPredicateFilter(dimension, predicateFactory, extractionFn);
    }
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
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(getDimension(), dimension) || getExtractionFn() != null) {
      return null;
    }
    RangeSet<String> retSet = TreeRangeSet.create();
    retSet.add(Range.singleton(Strings.nullToEmpty(value)));
    return retSet;
  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }


  private void initLongValue()
  {
    if (longsInitialized) {
      return;
    }
    synchronized (initLock) {
      if (longsInitialized) {
        return;
      }
      valueAsLong = Longs.tryParse(value);
      longsInitialized = true;
    }
  }
}
