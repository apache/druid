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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.filter.BoundFilter;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;

public class BoundDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  private final String dimension;
  @Nullable
  private final String upper;
  @Nullable
  private final String lower;
  private final boolean lowerStrict;
  private final boolean upperStrict;
  @Nullable
  private final ExtractionFn extractionFn;
  private final StringComparator ordering;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;
  @Nullable
  private final FilterTuning filterTuning;

  @JsonCreator
  public BoundDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("lower") @Nullable String lower,
      @JsonProperty("upper") @Nullable String upper,
      @JsonProperty("lowerStrict") @Nullable Boolean lowerStrict,
      @JsonProperty("upperStrict") @Nullable Boolean upperStrict,
      @Deprecated @JsonProperty("alphaNumeric") @Nullable Boolean alphaNumeric,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("ordering") @Nullable StringComparator ordering,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning)
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkState((lower != null) || (upper != null), "lower and upper can not be null at the same time");
    this.upper = upper;
    this.lower = lower;
    this.lowerStrict = (lowerStrict == null) ? false : lowerStrict;
    this.upperStrict = (upperStrict == null) ? false : upperStrict;

    // For backwards compatibility, we retain the 'alphaNumeric' property. It
    // will be used if the new 'ordering'
    // property is missing. If both 'ordering' and 'alphaNumeric' are present,
    // make sure they are consistent.
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
    this.floatPredicateSupplier = makeFloatPredicateSupplier();
    this.doublePredicateSupplier = makeDoublePredicateSupplier();
    this.filterTuning = filterTuning;
  }

  @VisibleForTesting
  public BoundDimFilter(
      String dimension,
      @Nullable String lower,
      @Nullable String upper,
      @Nullable Boolean lowerStrict,
      @Nullable Boolean upperStrict,
      @Nullable Boolean alphaNumeric,
      @Nullable ExtractionFn extractionFn,
      @Nullable StringComparator ordering)
  {
    this(dimension, lower, upper, lowerStrict, upperStrict, alphaNumeric, extractionFn, ordering, null);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getUpper()
  {
    return upper;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getLower()
  {
    return lower;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isLowerStrict()
  {
    return lowerStrict;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
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

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @JsonProperty
  public StringComparator getOrdering()
  {
    return ordering;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  public Supplier<DruidLongPredicate> getLongPredicateSupplier()
  {
    return longPredicateSupplier;
  }

  public Supplier<DruidFloatPredicate> getFloatPredicateSupplier()
  {
    return floatPredicateSupplier;
  }

  public Supplier<DruidDoublePredicate> getDoublePredicateSupplier()
  {
    return doublePredicateSupplier;
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
            + orderingBytes.length);
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
      range = Range.range(
          getLower(),
          isLowerStrict() ? BoundType.OPEN : BoundType.CLOSED,
          getUpper(),
          isUpperStrict() ? BoundType.OPEN : BoundType.CLOSED
      );
    }
    retSet.add(range);
    return retSet;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
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
    return lowerStrict == that.lowerStrict &&
        upperStrict == that.upperStrict &&
        dimension.equals(that.dimension) &&
        Objects.equals(upper, that.upper) &&
        Objects.equals(lower, that.lower) &&
        Objects.equals(extractionFn, that.extractionFn) &&
        Objects.equals(ordering, that.ordering) &&
        Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dimension,
        upper,
        lower,
        lowerStrict,
        upperStrict,
        extractionFn,
        ordering,
        filterTuning
    );
  }

  @Override
  public String toString()
  {
    final DimFilterToStringBuilder builder = new DimFilterToStringBuilder();

    if (lower != null) {
      builder.append(lower);
      if (lowerStrict) {
        builder.append(" < ");
      } else {
        builder.append(" <= ");
      }
    }

    builder.appendDimension(dimension, extractionFn);

    if (!ordering.equals(StringComparators.LEXICOGRAPHIC)) {
      builder.append(StringUtils.format(" as %s", ordering.toString()));
    }

    if (upper != null) {
      if (upperStrict) {
        builder.append(" < ");
      } else {
        builder.append(" <= ");
      }
      builder.append(upper);
    }

    return builder.appendFilterTuning(filterTuning).build();
  }

  private Supplier<DruidLongPredicate> makeLongPredicateSupplier()
  {
    return Suppliers.memoize(() -> {
      boolean hasLowerLongBound;
      boolean hasUpperLongBound;
      long lowerLongBound;
      long upperLongBound;
      boolean matchesNothing = false;

      if (hasLowerBound()) {
        final Long lowerLong = GuavaUtils.tryParseLong(lower);
        if (lowerLong == null) {
          BigDecimal lowerBigDecimal = getBigDecimalLowerBoundFromFloatString(lower);
          if (lowerBigDecimal == null) {
            // Unparseable values fall before all actual numbers, so all numbers
            // will match the lower bound.
            hasLowerLongBound = false;
            lowerLongBound = 0L;
          } else {
            try {
              lowerLongBound = lowerBigDecimal.longValueExact();
              hasLowerLongBound = true;
            }
            catch (ArithmeticException ae) { // the BigDecimal can't be contained in a long
              hasLowerLongBound = false;
              lowerLongBound = 0L;
              if (lowerBigDecimal.compareTo(BigDecimal.ZERO) > 0) {
                // positive lower bound, > all longs, will match nothing
                matchesNothing = true;
              }
            }
          }
        } else {
          hasLowerLongBound = true;
          lowerLongBound = lowerLong;
        }
      } else {
        hasLowerLongBound = false;
        lowerLongBound = 0L;
      }

      if (hasUpperBound()) {
        Long upperLong = GuavaUtils.tryParseLong(upper);
        if (upperLong == null) {
          BigDecimal upperBigDecimal = getBigDecimalUpperBoundFromFloatString(upper);
          if (upperBigDecimal == null) {
            // Unparseable values fall before all actual numbers, so no numbers
            // can match the upper bound.
            matchesNothing = true;
            hasUpperLongBound = false;
            upperLongBound = 0L;
          } else {
            try {
              upperLongBound = upperBigDecimal.longValueExact();
              hasUpperLongBound = true;
            }
            catch (ArithmeticException ae) { // the BigDecimal can't be
              // contained in a long
              hasUpperLongBound = false;
              upperLongBound = 0L;
              if (upperBigDecimal.compareTo(BigDecimal.ZERO) < 0) {
                // negative upper bound, < all longs, will match nothing
                matchesNothing = true;
              }
            }
          }
        } else {
          hasUpperLongBound = true;
          upperLongBound = upperLong;
        }
      } else {
        hasUpperLongBound = false;
        upperLongBound = 0L;
      }

      if (matchesNothing) {
        return DruidLongPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
      } else {
        final RangeFilter.RangeType rangeType = RangeFilter.RangeType.of(
            hasLowerLongBound,
            lowerStrict,
            hasUpperLongBound,
            upperStrict
        );
        return RangeFilter.makeLongPredicate(rangeType, lowerLongBound, upperLongBound);
      }
    });
  }

  private Supplier<DruidFloatPredicate> makeFloatPredicateSupplier()
  {
    return Suppliers.memoize(() -> {
      final boolean hasLowerFloatBound;
      final boolean hasUpperFloatBound;
      final float lowerFloatBound;
      final float upperFloatBound;
      boolean matchesNothing = false;

      if (hasLowerBound()) {
        final Float lowerFloat = Floats.tryParse(lower);
        if (lowerFloat == null) {
          // Unparseable values fall before all actual numbers, so all numbers
          // will match the lower bound.
          hasLowerFloatBound = false;
          lowerFloatBound = 0L;
        } else {
          hasLowerFloatBound = true;
          lowerFloatBound = lowerFloat;
        }
      } else {
        hasLowerFloatBound = false;
        lowerFloatBound = 0L;
      }

      if (hasUpperBound()) {
        Float upperFloat = Floats.tryParse(upper);
        if (upperFloat == null) {
          // Unparseable values fall before all actual numbers, so no numbers
          // can match the upper bound.
          matchesNothing = true;
          hasUpperFloatBound = false;
          upperFloatBound = 0L;
        } else {
          hasUpperFloatBound = true;
          upperFloatBound = upperFloat;
        }
      } else {
        hasUpperFloatBound = false;
        upperFloatBound = 0L;
      }


      if (matchesNothing) {
        return DruidFloatPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
      } else {

        final RangeFilter.RangeType rangeType = RangeFilter.RangeType.of(
            hasLowerFloatBound,
            lowerStrict,
            hasUpperFloatBound,
            upperStrict
        );
        final DruidDoublePredicate doublePredicate = RangeFilter.makeDoublePredicate(
            rangeType,
            lowerFloatBound,
            upperFloatBound
        );
        return doublePredicate::applyDouble;
      }
    });
  }

  private Supplier<DruidDoublePredicate> makeDoublePredicateSupplier()
  {
    return Suppliers.memoize(() -> {
      final boolean hasLowerBound;
      final boolean hasUpperBound;
      final double lowerDoubleBound;
      final double upperDoubleBound;
      boolean matchesNothing = false;

      if (hasLowerBound()) {
        final Double lowerDouble = Doubles.tryParse(lower);
        if (lowerDouble == null) {
          // Unparseable values fall before all actual numbers, so all numbers
          // will match the lower bound.
          hasLowerBound = false;
          lowerDoubleBound = 0L;
        } else {
          hasLowerBound = true;
          lowerDoubleBound = lowerDouble;
        }
      } else {
        hasLowerBound = false;
        lowerDoubleBound = 0L;
      }

      if (hasUpperBound()) {
        Double upperDouble = Doubles.tryParse(upper);
        if (upperDouble == null) {
          // Unparseable values fall before all actual numbers, so no numbers can
          // match the upper bound.
          matchesNothing = true;
          hasUpperBound = false;
          upperDoubleBound = 0L;
        } else {
          hasUpperBound = true;
          upperDoubleBound = upperDouble;
        }
      } else {
        hasUpperBound = false;
        upperDoubleBound = 0L;
      }

      if (matchesNothing) {
        return DruidDoublePredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
      } else {
        final RangeFilter.RangeType rangeType = RangeFilter.RangeType.of(
            hasLowerBound,
            lowerStrict,
            hasUpperBound,
            upperStrict
        );
        return RangeFilter.makeDoublePredicate(rangeType, lowerDoubleBound, upperDoubleBound);
      }
    });
  }

  @Nullable
  private BigDecimal getBigDecimalLowerBoundFromFloatString(String floatStr)
  {
    BigDecimal convertedBD;
    try {
      convertedBD = new BigDecimal(floatStr);
    }
    catch (NumberFormatException nfe) {
      return null;
    }

    if (lowerStrict) {
      return convertedBD.setScale(0, RoundingMode.FLOOR);
    } else {
      return convertedBD.setScale(0, RoundingMode.CEILING);
    }
  }

  @Nullable
  private BigDecimal getBigDecimalUpperBoundFromFloatString(String floatStr)
  {
    BigDecimal convertedBD;
    try {
      convertedBD = new BigDecimal(floatStr);
    }
    catch (NumberFormatException nfe) {
      return null;
    }

    if (upperStrict) {
      return convertedBD.setScale(0, RoundingMode.CEILING);
    } else {
      return convertedBD.setScale(0, RoundingMode.FLOOR);
    }
  }
}
