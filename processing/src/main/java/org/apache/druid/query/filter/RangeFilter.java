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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.DimensionPredicateFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndex;
import org.apache.druid.segment.index.semantic.NumericRangeIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RangeFilter extends AbstractOptimizableDimFilter implements Filter
{
  private final String column;
  private final ColumnType matchValueType;

  @Nullable
  private final Object upper;

  @Nullable
  private final Object lower;

  private final ExprEval<?> upperEval;
  private final ExprEval<?> lowerEval;
  private final boolean lowerStrict;
  private final boolean upperStrict;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;
  private final Supplier<Predicate<String>> stringPredicateSupplier;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

  @JsonCreator
  public RangeFilter(
      @JsonProperty("column") String column,
      @JsonProperty("matchValueType") ColumnType matchValueType,
      @JsonProperty("lower") @Nullable Object lower,
      @JsonProperty("upper") @Nullable Object upper,
      @JsonProperty("lowerStrict") @Nullable Boolean lowerStrict,
      @JsonProperty("upperStrict") @Nullable Boolean upperStrict,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    if (column == null) {
      throw InvalidInput.exception("Invalid range filter, column cannot be null");
    }
    this.column = column;
    if (matchValueType == null) {
      throw InvalidInput.exception("Invalid range filter on column [%s], matchValueType cannot be null", column);
    }
    this.matchValueType = matchValueType;
    if (lower == null && upper == null) {
      throw InvalidInput.exception(
          "Invalid range filter on column [%s], lower and upper cannot be null at the same time",
          column
      );
    }
    final ExpressionType expressionType = ExpressionType.fromColumnType(matchValueType);
    this.upper = upper;
    this.lower = lower;
    this.upperEval = ExprEval.ofType(expressionType, upper);
    this.lowerEval = ExprEval.ofType(expressionType, lower);
    if (expressionType.isNumeric()) {
      if (lower != null && lowerEval.value() == null) {
        throw InvalidInput.exception(
            "Invalid range filter on column [%s], lower bound [%s] cannot be parsed as specified match value type [%s]",
            column,
            lower,
            expressionType
        );
      }
      if (upper != null && upperEval.value() == null) {
        throw InvalidInput.exception(
            "Invalid range filter on column [%s], upper bound [%s] cannot be parsed as specified match value type [%s]",
            column,
            upper,
            expressionType
        );
      }
    }
    this.lowerStrict = lowerStrict != null && lowerStrict;
    this.upperStrict = upperStrict != null && upperStrict;
    // remove once SQL planner no longer uses extractionFn
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;
    this.stringPredicateSupplier = makeStringPredicateSupplier();
    this.longPredicateSupplier = makeLongPredicateSupplier();
    this.floatPredicateSupplier = makeFloatPredicateSupplier();
    this.doublePredicateSupplier = makeDoublePredicateSupplier();
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
  }

  @JsonProperty
  public ColumnType getMatchValueType()
  {
    return matchValueType;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Object getUpper()
  {
    return upper;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Object getLower()
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

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] lowerBytes;
    final byte[] upperBytes;
    if (hasLowerBound()) {
      final TypeStrategy<Object> typeStrategy = matchValueType.getStrategy();
      final int size = typeStrategy.estimateSizeBytes(lower);
      final ByteBuffer valueBuffer = ByteBuffer.allocate(size);
      typeStrategy.write(valueBuffer, lower, size);
      lowerBytes = valueBuffer.array();
    } else {
      lowerBytes = new byte[0];
    }
    if (hasUpperBound()) {
      final TypeStrategy<Object> typeStrategy = matchValueType.getStrategy();
      final int size = typeStrategy.estimateSizeBytes(upper);
      final ByteBuffer valueBuffer = ByteBuffer.allocate(size);
      typeStrategy.write(valueBuffer, upper, size);
      upperBytes = valueBuffer.array();
    } else {
      upperBytes = new byte[0];
    }
    byte boundType = 0x1;
    if (this.getLower() == null) {
      boundType = 0x2;
    } else if (this.getUpper() == null) {
      boundType = 0x3;
    }

    final byte lowerStrictByte = this.isLowerStrict() ? (byte) 1 : 0x0;
    final byte upperStrictByte = this.isUpperStrict() ? (byte) 1 : 0x0;

    return new CacheKeyBuilder(DimFilterUtils.RANGE_CACHE_ID)
        .appendByte(boundType)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(column)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(matchValueType.asTypeString())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(upperBytes)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(lowerBytes)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByte(lowerStrictByte)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByte(upperStrictByte)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(extractionFn == null ? new byte[0] : extractionFn.getCacheKey())
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
    if (extractionFn != null) {
      return new DimensionPredicateFilter(column, getPredicateFactory(), extractionFn, filterTuning);
    }
    return this;
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    // range partitioning converts stuff to strings.. so do that i guess

    String lowerString = lowerEval.asString();
    String upperString = upperEval.asString();
    RangeSet<String> retSet = TreeRangeSet.create();
    Range<String> range;
    if (getLower() == null) {
      range = isUpperStrict() ? Range.lessThan(upperString) : Range.atMost(upperString);
    } else if (getUpper() == null) {
      range = isLowerStrict() ? Range.greaterThan(lowerString) : Range.atLeast(lowerString);
    } else {
      range = Range.range(
          lowerString,
          isLowerStrict() ? BoundType.OPEN : BoundType.CLOSED,
          upperString,
          isUpperStrict() ? BoundType.OPEN : BoundType.CLOSED
      );
    }
    retSet.add(range);
    return retSet;
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(column, selector, filterTuning)) {
      return null;
    }
    if (matchValueType.is(ValueType.STRING) && extractionFn == null) {
      final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(column);
      if (indexSupplier == null) {
        return Filters.makeNullIndex(false, selector);
      }
      final LexicographicalRangeIndex rangeIndex = indexSupplier.as(LexicographicalRangeIndex.class);
      if (rangeIndex != null) {
        final String lower = hasLowerBound() ? lowerEval.asString() : null;
        final String upper = hasUpperBound() ? upperEval.asString() : null;
        if (NullHandling.isNullOrEquivalent(lower) && NullHandling.isNullOrEquivalent(upper)) {
          return Filters.makeNullIndex(false, selector);
        }
        final BitmapColumnIndex rangeBitmaps = rangeIndex.forRange(
            lower,
            lowerStrict,
            upper,
            upperStrict
        );
        if (rangeBitmaps != null) {
          return rangeBitmaps;
        }
      }
    }
    if (matchValueType.isNumeric() && extractionFn == null) {
      final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(column);
      if (indexSupplier == null) {
        return Filters.makeNullIndex(false, selector);
      }
      final NumericRangeIndex rangeIndex = indexSupplier.as(NumericRangeIndex.class);
      if (rangeIndex != null) {
        final Number lower = (Number) lowerEval.value();
        final Number upper = (Number) upperEval.value();
        final BitmapColumnIndex rangeBitmaps = rangeIndex.forRange(
            lower,
            isLowerStrict(),
            upper,
            isUpperStrict()
        );
        if (rangeBitmaps != null) {
          return rangeBitmaps;
        }
      }
    }

    // fall back to predicate based index if it is available
    return Filters.makePredicateIndex(column, selector, getPredicateFactory());
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, column, getPredicateFactory());
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        column,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(getPredicateFactory());
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector)
  {
    return Filters.supportsSelectivityEstimation(this, column, columnSelector, indexSelector);
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(column);
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(column);

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          column
      );
    }
    return new RangeFilter(
        rewriteDimensionTo,
        matchValueType,
        lower,
        upper,
        lowerStrict,
        upperStrict,
        extractionFn,
        filterTuning
    );
  }

  public boolean isEquality()
  {
    if (!hasUpperBound() || !hasLowerBound() || lowerStrict || upperStrict) {
      return false;
    }
    if (matchValueType.isArray()) {
      ExpressionType matchArrayType = ExpressionType.fromColumnType(matchValueType);
      return Arrays.deepEquals(
          ExprEval.ofType(matchArrayType, upper).asArray(),
          ExprEval.ofType(matchArrayType, lower).asArray()
      );
    } else {
      return Objects.equals(upper, lower);
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

    RangeFilter that = (RangeFilter) o;
    boolean upperSame;
    boolean lowerSame;
    if (matchValueType.isArray()) {
      ExpressionType matchArrayType = ExpressionType.fromColumnType(matchValueType);
      upperSame = Arrays.deepEquals(
          ExprEval.ofType(matchArrayType, upper).asArray(),
          ExprEval.ofType(matchArrayType, that.upper).asArray()
      );
      lowerSame = Arrays.deepEquals(
          ExprEval.ofType(matchArrayType, lower).asArray(),
          ExprEval.ofType(matchArrayType, that.lower).asArray()
      );
    } else {
      upperSame = Objects.equals(upper, that.upper);
      lowerSame = Objects.equals(lower, that.lower);
    }

    return lowerStrict == that.lowerStrict &&
           upperStrict == that.upperStrict &&
           column.equals(that.column) &&
           Objects.equals(matchValueType, that.matchValueType) &&
           upperSame &&
           lowerSame &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        column,
        matchValueType,
        upper,
        lower,
        lowerStrict,
        upperStrict,
        extractionFn,
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

    builder.appendDimension(column, extractionFn);

    builder.append(StringUtils.format(" as %s", matchValueType.toString()));

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

  private DruidPredicateFactory getPredicateFactory()
  {
    return new RangePredicateFactory(this);
  }

  private Supplier<DruidLongPredicate> makeLongPredicateSupplier()
  {
    return Suppliers.memoize(() -> {
      boolean hasLowerBound;
      boolean hasUpperBound;
      long lowerBound;
      long upperBound;

      if (hasLowerBound()) {
        ExprEval<?> lowerCast = lowerEval.castTo(ExpressionType.LONG);
        if (lowerCast.isNumericNull()) {
          hasLowerBound = false;
          lowerBound = Long.MIN_VALUE;
        } else {
          lowerBound = lowerCast.asLong();
          hasLowerBound = true;
        }
      } else {
        hasLowerBound = false;
        lowerBound = Long.MIN_VALUE;
      }

      if (hasUpperBound()) {
        ExprEval<?> upperCast = upperEval.castTo(ExpressionType.LONG);
        if (upperCast.isNumericNull()) {
          // upper value is not null, but isn't convertible to a long so is effectively null, nothing matches
          return DruidLongPredicate.ALWAYS_FALSE;
        } else {
          hasUpperBound = true;
          upperBound = upperCast.asLong();
        }
      } else {
        hasUpperBound = false;
        upperBound = Long.MAX_VALUE;
      }
      return BoundDimFilter.makeLongPredicateFromBounds(
          hasLowerBound,
          hasUpperBound,
          lowerStrict,
          upperStrict,
          lowerBound,
          upperBound
      );
    });
  }

  private Supplier<DruidFloatPredicate> makeFloatPredicateSupplier()
  {
    return Suppliers.memoize(() -> {
      DruidDoublePredicate doublePredicate = makeDoublePredicateSupplier().get();
      return doublePredicate::applyDouble;
    });
  }

  private Supplier<DruidDoublePredicate> makeDoublePredicateSupplier()
  {
    return Suppliers.memoize(() -> {
      boolean hasLowerBound;
      boolean hasUpperBound;
      double lowerBound;
      double upperBound;

      if (hasLowerBound()) {
        ExprEval<?> lowerCast = lowerEval.castTo(ExpressionType.DOUBLE);
        if (lowerCast.isNumericNull()) {
          hasLowerBound = false;
          lowerBound = Double.NEGATIVE_INFINITY;
        } else {
          lowerBound = lowerCast.asDouble();
          hasLowerBound = true;
        }
      } else {
        hasLowerBound = false;
        lowerBound = Double.NEGATIVE_INFINITY;
      }

      if (hasUpperBound()) {
        ExprEval<?> upperCast = upperEval.castTo(ExpressionType.DOUBLE);
        if (upperCast.isNumericNull()) {
          // upper value is not null, but isn't convertible to a long so is effectively null, nothing matches
          return DruidDoublePredicate.ALWAYS_FALSE;
        } else {
          hasUpperBound = true;
          upperBound = upperCast.asDouble();
        }
      } else {
        hasUpperBound = false;
        upperBound = Double.POSITIVE_INFINITY;
      }

      return BoundDimFilter.makeDoublePredicateFromBounds(
          hasLowerBound,
          hasUpperBound,
          lowerStrict,
          upperStrict,
          lowerBound,
          upperBound
      );
    });
  }

  private Supplier<Predicate<String>> makeStringPredicateSupplier()
  {
    return Suppliers.memoize(() -> {
      final Comparator<String> stringComparator = matchValueType.isNumeric()
                                                  ? StringComparators.NUMERIC
                                                  : StringComparators.LEXICOGRAPHIC;
      final String lowerBound = lowerEval.castTo(ExpressionType.STRING).asString();
      final String upperBound = upperEval.castTo(ExpressionType.STRING).asString();

      if (hasLowerBound() && hasUpperBound()) {
        if (upperStrict && lowerStrict) {
          return input -> {
            if (NullHandling.isNullOrEquivalent(input)) {
              return false;
            }
            final int lowerComparing = stringComparator.compare(input, lowerBound);
            final int upperComparing = stringComparator.compare(upperBound, input);
            return ((lowerComparing > 0)) && (upperComparing > 0);
          };
        } else if (lowerStrict) {
          return input -> {
            if (NullHandling.isNullOrEquivalent(input)) {
              return false;
            }
            final int lowerComparing = stringComparator.compare(input, lowerBound);
            final int upperComparing = stringComparator.compare(upperBound, input);
            return (lowerComparing > 0) && (upperComparing >= 0);
          };
        } else if (upperStrict) {
          return input -> {
            if (NullHandling.isNullOrEquivalent(input)) {
              return false;
            }
            final int lowerComparing = stringComparator.compare(input, lowerBound);
            final int upperComparing = stringComparator.compare(upperBound, input);
            return (lowerComparing >= 0) && (upperComparing > 0);
          };
        } else {
          return input -> {
            if (NullHandling.isNullOrEquivalent(input)) {
              return false;
            }
            final int lowerComparing = stringComparator.compare(input, lowerBound);
            final int upperComparing = stringComparator.compare(upperBound, input);
            return (lowerComparing >= 0) && (upperComparing >= 0);
          };
        }
      } else if (hasUpperBound()) {
        if (upperStrict) {
          return input -> {
            if (NullHandling.isNullOrEquivalent(input)) {
              return false;
            }
            final int upperComparing = stringComparator.compare(upperBound, input);
            return upperComparing > 0;
          };
        } else {
          return input -> {
            if (NullHandling.isNullOrEquivalent(input)) {
              return false;
            }
            final int upperComparing = stringComparator.compare(upperBound, input);
            return upperComparing >= 0;
          };
        }
      } else if (hasLowerBound()) {
        if (lowerStrict) {
          return input -> {
            if (NullHandling.isNullOrEquivalent(input)) {
              return false;
            }
            final int lowerComparing = stringComparator.compare(input, lowerBound);
            return lowerComparing > 0;
          };
        } else {
          return input -> {
            if (NullHandling.isNullOrEquivalent(input)) {
              return false;
            }
            final int lowerComparing = stringComparator.compare(input, lowerBound);
            return lowerComparing >= 0;
          };
        }
      } else {
        return Predicates.notNull();
      }
    });
  }

  private Predicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> arrayType)
  {
    if (hasLowerBound() && hasUpperBound()) {
      if (upperStrict && lowerStrict) {
        if (arrayType != null) {
          final Object[] lowerBound = lowerEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Object[] upperBound = upperEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          return input -> {
            if (input == null) {
              return false;
            }
            final int lowerComparing = arrayComparator.compare(input, lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, input);
            return ((lowerComparing > 0)) && (upperComparing > 0);
          };
        } else {
          // fall back to per row type detection
          return input -> {
            if (input == null) {
              return false;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> comparator = val.type().getNullableStrategy();
            final int lowerComparing = comparator.compare(val.asArray(), lowerBound);
            final int upperComparing = comparator.compare(upperBound, val.asArray());
            return ((lowerComparing > 0)) && (upperComparing > 0);
          };
        }
      } else if (lowerStrict) {
        if (arrayType != null) {
          final Object[] lowerBound = lowerEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Object[] upperBound = upperEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          return input -> {
            if (input == null) {
              return false;
            }
            final int lowerComparing = arrayComparator.compare(input, lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, input);
            return (lowerComparing > 0) && (upperComparing >= 0);
          };
        } else {
          // fall back to per row type detection
          return input -> {
            if (input == null) {
              return false;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(val.asArray(), lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return (lowerComparing > 0) && (upperComparing >= 0);
          };
        }
      } else if (upperStrict) {
        if (arrayType != null) {
          final Object[] lowerBound = lowerEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Object[] upperBound = upperEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          return input -> {
            if (input == null) {
              return false;
            }
            final int lowerComparing = arrayComparator.compare(input, lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, input);
            return (lowerComparing >= 0) && (upperComparing > 0);
          };
        } else {
          // fall back to per row type detection
          return input -> {
            if (input == null) {
              return false;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(val.asArray(), lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return (lowerComparing >= 0) && (upperComparing > 0);
          };
        }
      } else {
        if (arrayType != null) {
          final Object[] lowerBound = lowerEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Object[] upperBound = upperEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          return input -> {
            if (input == null) {
              return false;
            }
            final int lowerComparing = arrayComparator.compare(input, lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, input);
            return (lowerComparing >= 0) && (upperComparing >= 0);
          };
        } else {
          // fall back to per row type detection
          return input -> {
            if (input == null) {
              return false;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(val.asArray(), lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return (lowerComparing >= 0) && (upperComparing >= 0);
          };
        }
      }
    } else if (hasUpperBound()) {
      if (upperStrict) {
        if (arrayType != null) {
          final Object[] upperBound = upperEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          return input -> {
            if (input == null) {
              return false;
            }
            final int upperComparing = arrayComparator.compare(upperBound, input);
            return upperComparing > 0;
          };
        } else {
          // fall back to per row type detection
          return input -> {
            if (input == null) {
              return false;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return upperComparing > 0;
          };
        }
      } else {
        if (arrayType != null) {
          final Object[] upperBound = upperEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          return input -> {
            if (input == null) {
              return false;
            }
            final int upperComparing = arrayComparator.compare(upperBound, input);
            return upperComparing >= 0;
          };
        } else {
          // fall back to per row type detection
          return input -> {
            if (input == null) {
              return false;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return upperComparing >= 0;
          };
        }
      }
    } else if (hasLowerBound()) {
      if (lowerStrict) {
        if (arrayType != null) {
          final Object[] lowerBound = lowerEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          return input -> {
            if (input == null) {
              return false;
            }
            final int lowerComparing = arrayComparator.compare(input, lowerBound);
            return lowerComparing > 0;
          };
        } else {
          // fall back to per row type detection
          return input -> {
            if (input == null) {
              return false;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(lowerBound, val.asArray());
            return lowerComparing > 0;
          };
        }
      } else {
        if (arrayType != null) {
          final Object[] lowerBound = lowerEval.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          return input -> {
            if (input == null) {
              return false;
            }
            final int lowerComparing = arrayComparator.compare(input, lowerBound);
            return lowerComparing >= 0;
          };
        } else {
          // fall back to per row type detection
          return input -> {
            if (input == null) {
              return false;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(lowerBound, val.asArray());
            return lowerComparing >= 0;
          };
        }
      }
    } else {
      return Predicates.notNull();
    }
  }

  private class RangePredicateFactory implements DruidPredicateFactory
  {
    private final RangeFilter rangeFilter;

    private RangePredicateFactory(RangeFilter rangeFilter)
    {
      this.rangeFilter = rangeFilter;
    }

    @Override
    public Predicate<String> makeStringPredicate()
    {
      return stringPredicateSupplier.get();
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      if (matchValueType.isNumeric()) {
        return longPredicateSupplier.get();
      }
      Predicate<String> stringPredicate = stringPredicateSupplier.get();
      return input -> stringPredicate.apply(String.valueOf(input));
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      if (matchValueType.isNumeric()) {
        return floatPredicateSupplier.get();
      }
      Predicate<String> stringPredicate = stringPredicateSupplier.get();
      return input -> stringPredicate.apply(String.valueOf(input));
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      if (matchValueType.isNumeric()) {
        return doublePredicateSupplier.get();
      }
      Predicate<String> stringPredicate = stringPredicateSupplier.get();
      return input -> stringPredicate.apply(String.valueOf(input));
    }

    @Override
    public Predicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> inputType)
    {
      return RangeFilter.this.makeArrayPredicate(inputType);
    }

    @Override
    public int hashCode()
    {
      return rangeFilter.hashCode();
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
      RangePredicateFactory that = (RangePredicateFactory) o;
      return Objects.equals(rangeFilter, that.rangeFilter);
    }

    @Override
    public String toString()
    {
      return "RangePredicateFactory{" +
             "rangeFilter=" + rangeFilter +
             '}';
    }
  }
}
