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
import com.google.common.base.Supplier;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.filter.BoundFilter;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

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
    this.floatPredicateSupplier = makeFloatPredicateSupplier();
    this.doublePredicateSupplier = makeDoublePredicateSupplier();
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
                          getUpper(), isUpperStrict() ? BoundType.OPEN : BoundType.CLOSED
      );
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

  @Override
  public String toString()
  {
    final StringBuilder builder = new StringBuilder();

    if (lower != null) {
      builder.append(lower);
      if (lowerStrict) {
        builder.append(" < ");
      } else {
        builder.append(" <= ");
      }
    }

    if (extractionFn != null) {
      builder.append(StringUtils.format("%s(%s)", extractionFn, dimension));
    } else {
      builder.append(dimension);
    }

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

    return builder.toString();
  }

  private Supplier<DruidLongPredicate> makeLongPredicateSupplier()
  {
    class BoundLongPredicateSupplier implements Supplier<DruidLongPredicate>
    {
      private final Object initLock = new Object();
      private DruidLongPredicate predicate;

      @Override
      public DruidLongPredicate get()
      {
        initPredicate();
        return predicate;
      }

      private void initPredicate()
      {
        if (predicate != null) {
          return;
        }

        synchronized (initLock) {
          if (predicate != null) {
            return;
          }

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
                // Unparseable values fall before all actual numbers, so all numbers will match the lower bound.
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
                // Unparseable values fall before all actual numbers, so no numbers can match the upper bound.
                matchesNothing = true;
                hasUpperLongBound = false;
                upperLongBound = 0L;
              } else {
                try {
                  upperLongBound = upperBigDecimal.longValueExact();
                  hasUpperLongBound = true;
                }
                catch (ArithmeticException ae) { // the BigDecimal can't be contained in a long
                  hasUpperLongBound = false;
                  upperLongBound = 0L;
                  if (upperBigDecimal.compareTo(BigDecimal.ZERO) < 0) {
                    // negative upper bound, < all longs,  will match nothing
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
            predicate = DruidLongPredicate.ALWAYS_FALSE;
          } else {
            predicate = makeLongPredicateFromBounds(
                hasLowerLongBound,
                hasUpperLongBound,
                lowerStrict,
                upperStrict,
                lowerLongBound,
                upperLongBound
            );
          }
        }
      }
    }
    return new BoundLongPredicateSupplier();
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

  private Supplier<DruidFloatPredicate> makeFloatPredicateSupplier()
  {
    class BoundFloatPredicateSupplier implements Supplier<DruidFloatPredicate>
    {
      private final Object initLock = new Object();
      private DruidFloatPredicate predicate;

      @Override
      public DruidFloatPredicate get()
      {
        initPredicate();
        return predicate;
      }

      private void initPredicate()
      {
        if (predicate != null) {
          return;
        }

        synchronized (initLock) {
          if (predicate != null) {
            return;
          }

          final boolean hasLowerFloatBound;
          final boolean hasUpperFloatBound;
          final float lowerFloatBound;
          final float upperFloatBound;
          boolean matchesNothing = false;

          if (hasLowerBound()) {
            final Float lowerFloat = Floats.tryParse(lower);
            if (lowerFloat == null) {
              // Unparseable values fall before all actual numbers, so all numbers will match the lower bound.
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
              // Unparseable values fall before all actual numbers, so no numbers can match the upper bound.
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
            predicate = DruidFloatPredicate.ALWAYS_FALSE;
          } else {
            predicate = input -> {
              final DruidDoublePredicate druidDoublePredicate = makeDoublePredicateFromBounds(
                  hasLowerFloatBound,
                  hasUpperFloatBound,
                  lowerStrict,
                  upperStrict,
                  (double) lowerFloatBound,
                  (double) upperFloatBound
              );
              return druidDoublePredicate.applyDouble((double) input);
            };
          }
        }
      }
    }
    return new BoundFloatPredicateSupplier();
  }

  private Supplier<DruidDoublePredicate> makeDoublePredicateSupplier()
  {
    class BoundDoublePredicateSupplier implements Supplier<DruidDoublePredicate>
    {
      private final Object initLock = new Object();
      private DruidDoublePredicate predicate;

      @Override
      public DruidDoublePredicate get()
      {
        initPredicate();
        return predicate;
      }

      private void initPredicate()
      {
        if (predicate != null) {
          return;
        }

        synchronized (initLock) {
          if (predicate != null) {
            return;
          }

          final boolean hasLowerBound;
          final boolean hasUpperBound;
          final double lowerDoubleBound;
          final double upperDoubleBound;
          boolean matchesNothing = false;

          if (hasLowerBound()) {
            final Double lowerDouble = Doubles.tryParse(lower);
            if (lowerDouble == null) {
              // Unparseable values fall before all actual numbers, so all numbers will match the lower bound.
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
              // Unparseable values fall before all actual numbers, so no numbers can match the upper bound.
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
            predicate = DruidDoublePredicate.ALWAYS_FALSE;
          } else {
            predicate = makeDoublePredicateFromBounds(
                hasLowerBound,
                hasUpperBound,
                lowerStrict,
                upperStrict,
                lowerDoubleBound,
                upperDoubleBound
            );
          }
        }
      }
    }
    return new BoundDoublePredicateSupplier();
  }

  private static DruidLongPredicate makeLongPredicateFromBounds(
      final boolean hasLowerLongBound,
      final boolean hasUpperLongBound,
      final boolean lowerStrict,
      final boolean upperStrict,
      final long lowerLongBound,
      final long upperLongBound
  )
  {
    if (hasLowerLongBound && hasUpperLongBound) {
      if (upperStrict && lowerStrict) {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            final int lowerComparing = Long.compare(input, lowerLongBound);
            final int upperComparing = Long.compare(upperLongBound, input);
            return ((lowerComparing > 0)) && (upperComparing > 0);
          }
        };
      } else if (lowerStrict) {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            final int lowerComparing = Long.compare(input, lowerLongBound);
            final int upperComparing = Long.compare(upperLongBound, input);
            return (lowerComparing > 0) && (upperComparing >= 0);
          }
        };
      } else if (upperStrict) {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            final int lowerComparing = Long.compare(input, lowerLongBound);
            final int upperComparing = Long.compare(upperLongBound, input);
            return (lowerComparing >= 0) && (upperComparing > 0);
          }
        };
      } else {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            final int lowerComparing = Long.compare(input, lowerLongBound);
            final int upperComparing = Long.compare(upperLongBound, input);
            return (lowerComparing >= 0) && (upperComparing >= 0);
          }
        };
      }
    } else if (hasUpperLongBound) {
      if (upperStrict) {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            final int upperComparing = Long.compare(upperLongBound, input);
            return upperComparing > 0;
          }
        };
      } else {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            final int upperComparing = Long.compare(upperLongBound, input);
            return upperComparing >= 0;
          }
        };
      }
    } else if (hasLowerLongBound) {
      if (lowerStrict) {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            final int lowerComparing = Long.compare(input, lowerLongBound);
            return lowerComparing > 0;
          }
        };
      } else {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            final int lowerComparing = Long.compare(input, lowerLongBound);
            return lowerComparing >= 0;
          }
        };
      }
    } else {
      return DruidLongPredicate.ALWAYS_TRUE;
    }
  }

  private static DruidDoublePredicate makeDoublePredicateFromBounds(
      final boolean hasLowerDoubleBound,
      final boolean hasUpperDoubleBound,
      final boolean lowerStrict,
      final boolean upperStrict,
      final double lowerDoubleBound,
      final double upperDoubleBound
  )
  {
    if (hasLowerDoubleBound && hasUpperDoubleBound) {
      if (upperStrict && lowerStrict) {
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return ((lowerComparing > 0)) && (upperComparing > 0);
        };
      } else if (lowerStrict) {
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return (lowerComparing > 0) && (upperComparing >= 0);
        };
      } else if (upperStrict) {
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return (lowerComparing >= 0) && (upperComparing > 0);
        };
      } else {
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return (lowerComparing >= 0) && (upperComparing >= 0);
        };
      }
    } else if (hasUpperDoubleBound) {
      if (upperStrict) {
        return input -> {
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return upperComparing > 0;
        };
      } else {
        return input -> {
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return upperComparing >= 0;
        };
      }
    } else if (hasLowerDoubleBound) {
      if (lowerStrict) {
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          return lowerComparing > 0;
        };
      } else {
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          return lowerComparing >= 0;
        };
      }
    } else {
      return DruidDoublePredicate.ALWAYS_TRUE;
    }
  }
}
