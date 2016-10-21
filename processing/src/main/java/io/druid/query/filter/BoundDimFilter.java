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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.filter.BoundFilter;

import java.nio.ByteBuffer;
import java.util.Objects;

public class BoundDimFilter implements DimFilter
{
  private final String dimension;
  private final String upper;
  private final String lower;
  private final boolean lowerStrict;
  private final boolean upperStrict;
  private final ExtractionFn extractionFn;
  private final StringComparator ordering;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;

  @JsonCreator
  public BoundDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("lower") String lower,
      @JsonProperty("upper") String upper,
      @JsonProperty("lowerStrict") Boolean lowerStrict,
      @JsonProperty("upperStrict") Boolean upperStrict,
      @Deprecated @JsonProperty("alphaNumeric") Boolean alphaNumeric,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      @JsonProperty("ordering") StringComparator ordering
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkState((lower != null) || (upper != null), "lower and upper can not be null at the same time");
    this.upper = upper;
    this.lower = lower;
    this.lowerStrict = (lowerStrict == null) ? false : lowerStrict;
    this.upperStrict = (upperStrict == null) ? false : upperStrict;

    // For backwards compatibility, we retain the 'alphaNumeric' property. It will be used if the new 'ordering'
    // property is missing. If both 'ordering' and 'alphaNumeric' are present, make sure they are consistent.
    if (ordering == null) {
      if (alphaNumeric == null || !alphaNumeric) {
        this.ordering = StringComparators.LEXICOGRAPHIC;
      } else {
        this.ordering = StringComparators.ALPHANUMERIC;
      }
    } else {
      this.ordering = ordering;
      if (alphaNumeric != null) {
        boolean orderingIsAlphanumeric = this.ordering.equals(StringComparators.ALPHANUMERIC);
        Preconditions.checkState(
            alphaNumeric == orderingIsAlphanumeric,
            "mismatch between alphanumeric and ordering property"
        );
      }
    }
    this.extractionFn = extractionFn;
    this.longPredicateSupplier = makeLongPredicateSupplier();
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getUpper()
  {
    return upper;
  }

  @JsonProperty
  public String getLower()
  {
    return lower;
  }

  @JsonProperty
  public boolean isLowerStrict()
  {
    return lowerStrict;
  }

  @JsonProperty
  public boolean isUpperStrict()
  {
    return upperStrict;
  }

  public boolean hasLowerBound()
  {
    return lower != null;
  }

  public boolean hasUpperBound()
  {
    return upper != null;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @JsonProperty
  public StringComparator getOrdering()
  {
    return ordering;
  }

  public Supplier<DruidLongPredicate> getLongPredicateSupplier()
  {
    return longPredicateSupplier;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(this.getDimension());
    byte[] lowerBytes = this.getLower() == null ? new byte[0] : StringUtils.toUtf8(this.getLower());
    byte[] upperBytes = this.getUpper() == null ? new byte[0] : StringUtils.toUtf8(this.getUpper());
    byte boundType = 0x1;
    if (this.getLower() == null) {
      boundType = 0x2;
    } else if (this.getUpper() == null) {
      boundType = 0x3;
    }

    byte lowerStrictByte = (this.isLowerStrict() == false) ? 0x0 : (byte) 1;
    byte upperStrictByte = (this.isUpperStrict() == false) ? 0x0 : (byte) 1;

    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    byte[] orderingBytes = ordering.getCacheKey();

    ByteBuffer boundCacheBuffer = ByteBuffer.allocate(
        9
        + dimensionBytes.length
        + upperBytes.length
        + lowerBytes.length
        + extractionFnBytes.length
        + orderingBytes.length
    );
    boundCacheBuffer.put(DimFilterUtils.BOUND_CACHE_ID)
                    .put(boundType)
                    .put(upperStrictByte)
                    .put(lowerStrictByte)
                    .put(DimFilterUtils.STRING_SEPARATOR)
                    .put(dimensionBytes)
                    .put(DimFilterUtils.STRING_SEPARATOR)
                    .put(upperBytes)
                    .put(DimFilterUtils.STRING_SEPARATOR)
                    .put(lowerBytes)
                    .put(DimFilterUtils.STRING_SEPARATOR)
                    .put(extractionFnBytes)
                    .put(DimFilterUtils.STRING_SEPARATOR)
                    .put(orderingBytes);
    return boundCacheBuffer.array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new BoundFilter(this);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!(Objects.equals(getDimension(), dimension)
          && getExtractionFn() == null
          && ordering.equals(StringComparators.LEXICOGRAPHIC))) {
      return null;
    }

    RangeSet<String> retSet = TreeRangeSet.create();
    Range<String> range;
    if (getLower() == null) {
      range = isUpperStrict() ? Range.lessThan(getUpper()) : Range.atMost(getUpper());
    } else if (getUpper() == null) {
      range = isLowerStrict() ? Range.greaterThan(getLower()) : Range.atLeast(getLower());
    } else {
      range = Range.range(getLower(), isLowerStrict() ? BoundType.OPEN : BoundType.CLOSED,
                          getUpper(), isUpperStrict() ? BoundType.OPEN : BoundType.CLOSED);
    }
    retSet.add(range);
    return retSet;
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

    BoundDimFilter that = (BoundDimFilter) o;

    if (isLowerStrict() != that.isLowerStrict()) {
      return false;
    }
    if (isUpperStrict() != that.isUpperStrict()) {
      return false;
    }
    if (!getDimension().equals(that.getDimension())) {
      return false;
    }
    if (getUpper() != null ? !getUpper().equals(that.getUpper()) : that.getUpper() != null) {
      return false;
    }
    if (getLower() != null ? !getLower().equals(that.getLower()) : that.getLower() != null) {
      return false;
    }
    if (getExtractionFn() != null
        ? !getExtractionFn().equals(that.getExtractionFn())
        : that.getExtractionFn() != null) {
      return false;
    }
    return getOrdering().equals(that.getOrdering());

  }

  @Override
  public int hashCode()
  {
    int result = getDimension().hashCode();
    result = 31 * result + (getUpper() != null ? getUpper().hashCode() : 0);
    result = 31 * result + (getLower() != null ? getLower().hashCode() : 0);
    result = 31 * result + (isLowerStrict() ? 1 : 0);
    result = 31 * result + (isUpperStrict() ? 1 : 0);
    result = 31 * result + (getExtractionFn() != null ? getExtractionFn().hashCode() : 0);
    result = 31 * result + getOrdering().hashCode();
    return result;
  }

  private Supplier<DruidLongPredicate> makeLongPredicateSupplier()
  {
    return new Supplier<DruidLongPredicate>()
    {
      private final Object initLock = new Object();
      private volatile boolean longsInitialized = false;
      private volatile boolean hasLowerLongBoundVolatile;
      private volatile boolean hasUpperLongBoundVolatile;
      private volatile long lowerLongBoundVolatile;
      private volatile long upperLongBoundVolatile;

      @Override
      public DruidLongPredicate get()
      {
        initLongData();

        return new DruidLongPredicate()
        {
          private final boolean hasLowerLongBound = hasLowerLongBoundVolatile;
          private final boolean hasUpperLongBound = hasUpperLongBoundVolatile;
          private final long lowerLongBound = hasLowerLongBound ? lowerLongBoundVolatile : 0L;
          private final long upperLongBound = hasUpperLongBound ? upperLongBoundVolatile : 0L;

          @Override
          public boolean applyLong(long input)
          {
            int lowerComparing = 1;
            int upperComparing = 1;
            if (hasLowerLongBound) {
              lowerComparing = Long.compare(input, lowerLongBound);
            }
            if (hasUpperLongBound) {
              upperComparing = Long.compare(upperLongBound, input);
            }
            if (lowerStrict && upperStrict) {
              return ((lowerComparing > 0)) && (upperComparing > 0);
            } else if (lowerStrict) {
              return (lowerComparing > 0) && (upperComparing >= 0);
            } else if (upperStrict) {
              return (lowerComparing >= 0) && (upperComparing > 0);
            }
            return (lowerComparing >= 0) && (upperComparing >= 0);

          }
        };
      }

      private void initLongData()
      {
        if (longsInitialized) {
          return;
        }

        synchronized (initLock) {
          if (longsInitialized) {
            return;
          }

          Long lowerLong = Longs.tryParse(Strings.nullToEmpty(lower));
          if (hasLowerBound() && lowerLong != null) {
            hasLowerLongBoundVolatile = true;
            lowerLongBoundVolatile = lowerLong;
          } else {
            hasLowerLongBoundVolatile = false;
          }

          Long upperLong = Longs.tryParse(Strings.nullToEmpty(upper));
          if (hasUpperBound() && upperLong != null) {
            hasUpperLongBoundVolatile = true;
            upperLongBoundVolatile = upperLong;
          } else {
            hasUpperLongBoundVolatile = false;
          }

          longsInitialized = true;
        }
      }
    };
  }


}
