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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.druid.segment.index.semantic.NumericRangeIndexes;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
  private final boolean lowerOpen;
  private final boolean upperOpen;
  @Nullable
  private final FilterTuning filterTuning;
  private final Supplier<DruidObjectPredicate<String>> stringPredicateSupplier;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;
  private final ConcurrentHashMap<TypeSignature<ValueType>, DruidObjectPredicate<Object[]>> arrayPredicates;
  private final Supplier<DruidObjectPredicate<Object[]>> typeDetectingArrayPredicateSupplier;

  @JsonCreator
  public RangeFilter(
      @JsonProperty("column") String column,
      @JsonProperty("matchValueType") ColumnType matchValueType,
      @JsonProperty("lower") @Nullable Object lower,
      @JsonProperty("upper") @Nullable Object upper,
      @JsonProperty("lowerOpen") @Nullable Boolean lowerOpen,
      @JsonProperty("upperOpen") @Nullable Boolean upperOpen,
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
    this.upper = upper;
    this.lower = lower;
    ExpressionType matchValueExpressionType = ExpressionType.fromColumnTypeStrict(matchValueType);
    this.upperEval = ExprEval.ofType(matchValueExpressionType, upper);
    this.lowerEval = ExprEval.ofType(matchValueExpressionType, lower);

    if (lowerEval.value() == null && upperEval.value() == null) {
      throw InvalidInput.exception(
          "Invalid range filter on column [%s], lower and upper cannot be null at the same time",
          column
      );
    }
    if (this.matchValueType.isNumeric()) {
      if (lower != null && lowerEval.value() == null) {
        throw InvalidInput.exception(
            "Invalid range filter on column [%s], lower bound [%s] cannot be parsed as specified match value type [%s]",
            column,
            lower,
            matchValueExpressionType
        );
      }
      if (upper != null && upperEval.value() == null) {
        throw InvalidInput.exception(
            "Invalid range filter on column [%s], upper bound [%s] cannot be parsed as specified match value type [%s]",
            column,
            upper,
            matchValueExpressionType
        );
      }
    }
    this.lowerOpen = lowerOpen != null && lowerOpen;
    this.upperOpen = upperOpen != null && upperOpen;
    this.filterTuning = filterTuning;
    this.stringPredicateSupplier = makeStringPredicateSupplier();
    this.longPredicateSupplier = makeLongPredicateSupplier();
    this.floatPredicateSupplier = makeFloatPredicateSupplier();
    this.doublePredicateSupplier = makeDoublePredicateSupplier();
    this.arrayPredicates = new ConcurrentHashMap<>();
    this.typeDetectingArrayPredicateSupplier = makeTypeDetectingArrayPredicate();

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
  public boolean isLowerOpen()
  {
    return lowerOpen;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isUpperOpen()
  {
    return upperOpen;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  public boolean hasLowerBound()
  {
    return lower != null;
  }

  public boolean hasUpperBound()
  {
    return upper != null;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] lowerBytes;
    final byte[] upperBytes;
    if (hasLowerBound()) {
      final TypeStrategy<Object> typeStrategy = lowerEval.type().getStrategy();
      final int size = typeStrategy.estimateSizeBytes(lowerEval.value());
      final ByteBuffer valueBuffer = ByteBuffer.allocate(size);
      typeStrategy.write(valueBuffer, lowerEval.value(), size);
      lowerBytes = valueBuffer.array();
    } else {
      lowerBytes = new byte[0];
    }
    if (hasUpperBound()) {
      final TypeStrategy<Object> typeStrategy = upperEval.type().getStrategy();
      final int size = typeStrategy.estimateSizeBytes(upperEval.value());
      final ByteBuffer valueBuffer = ByteBuffer.allocate(size);
      typeStrategy.write(valueBuffer, upperEval.value(), size);
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

    final byte lowerStrictByte = this.isLowerOpen() ? (byte) 1 : 0x0;
    final byte upperStrictByte = this.isUpperOpen() ? (byte) 1 : 0x0;

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
        .build();
  }

  @Override
  public Filter toFilter()
  {
    return this;
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(column, dimension)) {
      return null;
    }

    // We need to return a RangeSet<String>, but we have Object, not String.  We align with the interface by
    // converting things to String, but we'd probably be better off adjusting the interface to something that is
    // more type aware in the future

    final Supplier<String> lowerString = () -> lowerEval.isArray() ? Arrays.deepToString(lowerEval.asArray()) : lowerEval.asString();
    final Supplier<String> upperString = () -> upperEval.isArray() ? Arrays.deepToString(upperEval.asArray()) : upperEval.asString();
    RangeSet<String> retSet = TreeRangeSet.create();
    final Range<String> range;
    if (!hasLowerBound()) {
      range = isUpperOpen() ? Range.lessThan(upperString.get()) : Range.atMost(upperString.get());
    } else if (!hasUpperBound()) {
      range = isLowerOpen() ? Range.greaterThan(lowerString.get()) : Range.atLeast(lowerString.get());
    } else {
      range = Range.range(
          lowerString.get(),
          isLowerOpen() ? BoundType.OPEN : BoundType.CLOSED,
          upperString.get(),
          isUpperOpen() ? BoundType.OPEN : BoundType.CLOSED
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
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(column);
    if (indexSupplier == null) {
      return new AllUnknownBitmapColumnIndex(selector);
    }

    if (matchValueType.is(ValueType.STRING)) {
      final LexicographicalRangeIndexes rangeIndexes = indexSupplier.as(LexicographicalRangeIndexes.class);
      if (rangeIndexes != null) {
        final String lower = hasLowerBound() ? lowerEval.asString() : null;
        final String upper = hasUpperBound() ? upperEval.asString() : null;
        return rangeIndexes.forRange(lower, lowerOpen, upper, upperOpen);
      }
    }
    if (matchValueType.isNumeric()) {
      final NumericRangeIndexes rangeIndexes = indexSupplier.as(NumericRangeIndexes.class);
      if (rangeIndexes != null) {
        final Number lower = (Number) lowerEval.value();
        final Number upper = (Number) upperEval.value();
        return rangeIndexes.forRange(lower, lowerOpen, upper, upperOpen);
      }
    }

    // fall back to predicate based index if it is available
    final DruidPredicateIndexes predicateIndexes = indexSupplier.as(DruidPredicateIndexes.class);
    if (predicateIndexes != null) {
      return predicateIndexes.forPredicate(getPredicateFactory());
    }
    // index doesn't exist
    return null;
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
        lowerOpen,
        upperOpen,
        filterTuning
    );
  }

  public boolean isEquality()
  {
    if (!hasUpperBound() || !hasLowerBound() || lowerOpen || upperOpen) {
      return false;
    }
    if (matchValueType.isArray()) {
      return Arrays.deepEquals(
          lowerEval.asArray(),
          upperEval.asArray()
      );
    } else {
      return Objects.equals(upperEval.value(), lowerEval.value());
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
      upperSame = Arrays.deepEquals(
          upperEval.asArray(),
          that.upperEval.asArray()
      );
      lowerSame = Arrays.deepEquals(
          lowerEval.asArray(),
          that.lowerEval.asArray()
      );
    } else {
      upperSame = Objects.equals(upperEval.value(), that.upperEval.value());
      lowerSame = Objects.equals(lowerEval.value(), that.lowerEval.value());
    }

    return lowerOpen == that.lowerOpen &&
           upperOpen == that.upperOpen &&
           column.equals(that.column) &&
           Objects.equals(matchValueType, that.matchValueType) &&
           upperSame &&
           lowerSame &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        column,
        matchValueType,
        upperEval.value(),
        lowerEval.value(),
        lowerOpen,
        upperOpen,
        filterTuning
    );
  }

  @Override
  public String toString()
  {
    final DimFilterToStringBuilder builder = new DimFilterToStringBuilder();

    if (lower != null) {
      if (matchValueType.isArray()) {
        builder.append(Arrays.deepToString(lowerEval.asArray()));
      } else {
        builder.append(lower);
      }
      if (lowerOpen) {
        builder.append(" < ");
      } else {
        builder.append(" <= ");
      }
    }

    builder.appendDimension(column, null);

    builder.append(StringUtils.format(" as %s", matchValueType.toString()));

    if (upper != null) {
      if (upperOpen) {
        builder.append(" < ");
      } else {
        builder.append(" <= ");
      }
      if (matchValueType.isArray()) {
        builder.append(Arrays.deepToString(upperEval.asArray()));
      } else {
        builder.append(upper);
      }
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
      final boolean hasLowerBound;
      final boolean hasUpperBound;
      final long lowerBound;
      final long upperBound;

      if (hasLowerBound()) {
        ExprEval<?> lowerCast = lowerEval.castTo(ExpressionType.LONG);
        if (lowerCast.isNumericNull()) {
          // lower value is not null, but isn't convertible to a long so is effectively null, nothing matches
          // this shouldn't be possible because we only use numeric predicates when the match value type is numeric
          // but just in case...
          return DruidLongPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
        } else {
          if (lowerOpen) {
            // lower bound is open, so take the floor of the value so that x > 1.1 can match 2 but not 1
            lowerBound = (long) Math.floor(lowerEval.asDouble());
          } else {
            // lower bound is closed, tkae the ceil of the value so that x >= 1.1 can match 2 but not 1
            lowerBound = (long) Math.ceil(lowerEval.asDouble());
          }
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
          // this shouldn't be possible because we only use numeric predicates when the match value type is numeric
          // but just in case...
          return DruidLongPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
        } else {
          if (upperOpen) {
            // upper bound is open, take the ceil so that x < 1.1 can match 1 but not 2
            upperBound = (long) Math.ceil(upperEval.asDouble());
          } else {
            // upper bound is closed, take the floor so that x <= 1.1 can match 1 but not 2
            upperBound = (long) Math.floor(upperEval.asDouble());
          }
          hasUpperBound = true;
        }
      } else {
        hasUpperBound = false;
        upperBound = Long.MAX_VALUE;
      }
      final RangeType rangeType = RangeType.of(hasLowerBound, lowerOpen, hasUpperBound, upperOpen);
      return makeLongPredicate(rangeType, lowerBound, upperBound);
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
      final boolean hasLowerBound;
      final boolean hasUpperBound;
      final double lowerBound;
      final double upperBound;

      if (hasLowerBound()) {
        ExprEval<?> lowerCast = lowerEval.castTo(ExpressionType.DOUBLE);
        if (lowerCast.isNumericNull()) {
          // lower value is not null, but isn't convertible to a long so is effectively null, nothing matches
          // this shouldn't be possible because we only use numeric predicates when the match value type is numeric
          // but just in case...
          return DruidDoublePredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
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
          // this shouldn't be possible because we only use numeric predicates when the match value type is numeric
          // but just in case...
          return DruidDoublePredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
        } else {
          hasUpperBound = true;
          upperBound = upperCast.asDouble();
        }
      } else {
        hasUpperBound = false;
        upperBound = Double.POSITIVE_INFINITY;
      }

      RangeType rangeType = RangeType.of(hasLowerBound, lowerOpen, hasUpperBound, upperOpen);
      return makeDoublePredicate(rangeType, lowerBound, upperBound);
    });
  }

  private Supplier<DruidObjectPredicate<String>> makeStringPredicateSupplier()
  {
    return Suppliers.memoize(() -> {
      final Comparator<String> stringComparator =
          matchValueType.isNumeric() ? StringComparators.NUMERIC : StringComparators.LEXICOGRAPHIC;
      final String lowerBound = hasLowerBound() ? lowerEval.castTo(ExpressionType.STRING).asString() : null;
      final String upperBound = hasUpperBound() ? upperEval.castTo(ExpressionType.STRING).asString() : null;

      final RangeType rangeType = RangeType.of(hasLowerBound(), lowerOpen, hasUpperBound(), upperOpen);

      return makeComparatorPredicate(rangeType, stringComparator, lowerBound, upperBound);
    });
  }

  private DruidObjectPredicate<Object[]> makeArrayPredicate(TypeSignature<ValueType> inputType)
  {
    final Comparator<Object[]> arrayComparator;
    if (inputType.getElementType().is(ValueType.STRING) && Types.isNumericOrNumericArray(matchValueType)) {
      arrayComparator = new NumericStringArrayComparator();
    } else {
      arrayComparator = inputType.getNullableStrategy();
    }
    final ExpressionType expressionType = ExpressionType.fromColumnTypeStrict(inputType);
    final RangeType rangeType = RangeType.of(hasLowerBound(), lowerOpen, hasUpperBound(), upperOpen);

    final Object[] lowerBound;
    final Object[] upperBound;
    if (hasLowerBound()) {
      if (lowerOpen) {
        lowerBound = lowerEval.castTo(expressionType).asArray();
      } else {
        lowerBound = castArrayForComparisonWithCeilIfNeeded(lowerEval, expressionType);
      }
    } else {
      lowerBound = null;
    }
    if (hasUpperBound()) {
      if (upperOpen) {
        upperBound = castArrayForComparisonWithCeilIfNeeded(upperEval, expressionType);
      } else {
        upperBound = upperEval.castTo(expressionType).asArray();
      }
    } else {
      upperBound = null;
    }
    return makeComparatorPredicate(rangeType, arrayComparator, lowerBound, upperBound);
  }

  private Supplier<DruidObjectPredicate<Object[]>> makeTypeDetectingArrayPredicate()
  {
    return Suppliers.memoize(() -> {
      RangeType rangeType = RangeType.of(hasLowerBound(), lowerOpen, hasUpperBound(), upperOpen);
      switch (rangeType) {
        case OPEN:
          return input -> {
            if (input == null) {
              return DruidPredicateMatch.UNKNOWN;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Object[] upperBound = castArrayForComparisonWithCeilIfNeeded(upperEval, val.asArrayType());
            final Comparator<Object[]> comparator = val.type().getNullableStrategy();
            final int lowerComparing = comparator.compare(val.asArray(), lowerBound);
            final int upperComparing = comparator.compare(upperBound, val.asArray());
            return DruidPredicateMatch.of(((lowerComparing > 0)) && (upperComparing > 0));
          };
        case LOWER_OPEN_UPPER_CLOSED:
          return input -> {
            if (input == null) {
              return DruidPredicateMatch.UNKNOWN;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(val.asArray(), lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return DruidPredicateMatch.of((lowerComparing > 0) && (upperComparing >= 0));
          };
        case LOWER_CLOSED_UPPER_OPEN:
          return input -> {
            if (input == null) {
              return DruidPredicateMatch.UNKNOWN;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = castArrayForComparisonWithCeilIfNeeded(lowerEval, val.asArrayType());
            final Object[] upperBound = castArrayForComparisonWithCeilIfNeeded(upperEval, val.asArrayType());
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(val.asArray(), lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return DruidPredicateMatch.of((lowerComparing >= 0) && (upperComparing > 0));
          };
        case CLOSED:
          return input -> {
            if (input == null) {
              return DruidPredicateMatch.UNKNOWN;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = castArrayForComparisonWithCeilIfNeeded(lowerEval, val.asArrayType());
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(val.asArray(), lowerBound);
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return DruidPredicateMatch.of((lowerComparing >= 0) && (upperComparing >= 0));
          };
        case LOWER_UNBOUNDED_UPPER_OPEN:
          return input -> {
            if (input == null) {
              return DruidPredicateMatch.UNKNOWN;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] upperBound = castArrayForComparisonWithCeilIfNeeded(upperEval, val.asArrayType());
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return DruidPredicateMatch.of(upperComparing > 0);
          };
        case LOWER_UNBOUNDED_UPPER_CLOSED:
          return input -> {
            if (input == null) {
              return DruidPredicateMatch.UNKNOWN;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] upperBound = upperEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int upperComparing = arrayComparator.compare(upperBound, val.asArray());
            return DruidPredicateMatch.of(upperComparing >= 0);
          };
        case LOWER_OPEN_UPPER_UNBOUNDED:
          return input -> {
            if (input == null) {
              return DruidPredicateMatch.UNKNOWN;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = lowerEval.castTo(val.type()).asArray();
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(lowerBound, val.asArray());
            return DruidPredicateMatch.of(lowerComparing > 0);
          };
        case LOWER_CLOSED_UPPER_UNBOUNDED:
          return input -> {
            if (input == null) {
              return DruidPredicateMatch.UNKNOWN;
            }
            ExprEval<?> val = ExprEval.bestEffortOf(input);
            final Object[] lowerBound = castArrayForComparisonWithCeilIfNeeded(lowerEval, val.asArrayType());
            final Comparator<Object[]> arrayComparator = val.type().getNullableStrategy();
            final int lowerComparing = arrayComparator.compare(lowerBound, val.asArray());
            return DruidPredicateMatch.of(lowerComparing >= 0);
          };
        case UNBOUNDED:
        default:
          return DruidObjectPredicate.notNull();
      }
    });
  }

  private class RangePredicateFactory implements DruidPredicateFactory
  {
    private final RangeFilter rangeFilter;

    private RangePredicateFactory(RangeFilter rangeFilter)
    {
      this.rangeFilter = rangeFilter;
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate()
    {
      return new FallbackPredicate<>(
          stringPredicateSupplier.get(),
          ExpressionType.STRING
      );
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      if (matchValueType.isNumeric()) {
        return longPredicateSupplier.get();
      }
      DruidObjectPredicate<String> stringPredicate = makeStringPredicate();
      return input -> stringPredicate.apply(Evals.asString(input));
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      if (matchValueType.isNumeric()) {
        return floatPredicateSupplier.get();
      }
      DruidObjectPredicate<String> stringPredicate = makeStringPredicate();
      return input -> stringPredicate.apply(Evals.asString(input));
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      if (matchValueType.isNumeric()) {
        return doublePredicateSupplier.get();
      }
      DruidObjectPredicate<String> stringPredicate = makeStringPredicate();
      return input -> stringPredicate.apply(Evals.asString(input));
    }

    @Override
    public DruidObjectPredicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> inputType)
    {
      if (inputType == null) {
        return typeDetectingArrayPredicateSupplier.get();
      }
      return new FallbackPredicate<>(
          arrayPredicates.computeIfAbsent(inputType, (existing) -> RangeFilter.this.makeArrayPredicate(inputType)),
          ExpressionType.fromColumnTypeStrict(inputType)
      );
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

  /**
   * This method is like {@link ExprEval#castTo(ExpressionType)} and {@link ExprEval#asArray()}, but when the target
   * type is {@link ExpressionType#LONG_ARRAY}, the array elements are treated as decimals and passed to
   * {@link Math#ceil(double)} before converting to a LONG instead of the typical flooring that would happen when
   * casting.
   */
  private static Object[] castArrayForComparisonWithCeilIfNeeded(ExprEval<?> valueToCast, ExpressionType typeToCastTo)
  {
    if (ExpressionType.LONG_ARRAY.equals(typeToCastTo)) {
      final ExprEval<?> doubleArray = valueToCast.castTo(ExpressionType.DOUBLE_ARRAY);
      final Object[] o = doubleArray.asArray();
      final Object[] ceilArray = new Object[o.length];
      for (int i = 0; i < o.length; i++) {
        ceilArray[i] = o[i] == null ? null : (long) Math.ceil((Double) o[i]);
      }
      return ceilArray;
    } else {
      return valueToCast.castTo(typeToCastTo).asArray();
    }
  }

  public static DruidLongPredicate makeLongPredicate(
      final RangeType rangeType,
      final long lowerLongBound,
      final long upperLongBound
  )
  {
    switch (rangeType) {
      case OPEN:
        return input -> DruidPredicateMatch.of(input > lowerLongBound && input < upperLongBound);
      case LOWER_OPEN_UPPER_CLOSED:
        return input -> DruidPredicateMatch.of(input > lowerLongBound && input <= upperLongBound);
      case LOWER_CLOSED_UPPER_OPEN:
        return input -> DruidPredicateMatch.of(input >= lowerLongBound && input < upperLongBound);
      case CLOSED:
        return input -> DruidPredicateMatch.of(input >= lowerLongBound && input <= upperLongBound);
      case LOWER_UNBOUNDED_UPPER_OPEN:
        return input -> DruidPredicateMatch.of(input < upperLongBound);
      case LOWER_UNBOUNDED_UPPER_CLOSED:
        return input -> DruidPredicateMatch.of(input <= upperLongBound);
      case LOWER_OPEN_UPPER_UNBOUNDED:
        return input -> DruidPredicateMatch.of(input > lowerLongBound);
      case LOWER_CLOSED_UPPER_UNBOUNDED:
        return input -> DruidPredicateMatch.of(input >= lowerLongBound);
      case UNBOUNDED:
      default:
        return DruidLongPredicate.ALWAYS_TRUE;
    }
  }

  public static DruidDoublePredicate makeDoublePredicate(
      final RangeType rangeType,
      final double lowerDoubleBound,
      final double upperDoubleBound
  )
  {
    switch (rangeType) {
      case OPEN:
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return DruidPredicateMatch.of(((lowerComparing > 0)) && (upperComparing > 0));
        };
      case LOWER_OPEN_UPPER_CLOSED:
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return DruidPredicateMatch.of((lowerComparing > 0) && (upperComparing >= 0));
        };
      case LOWER_CLOSED_UPPER_OPEN:
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return DruidPredicateMatch.of((lowerComparing >= 0) && (upperComparing > 0));
        };
      case CLOSED:
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return DruidPredicateMatch.of((lowerComparing >= 0) && (upperComparing >= 0));
        };
      case LOWER_UNBOUNDED_UPPER_OPEN:
        return input -> {
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return DruidPredicateMatch.of(upperComparing > 0);
        };
      case LOWER_UNBOUNDED_UPPER_CLOSED:
        return input -> {
          final int upperComparing = Double.compare(upperDoubleBound, input);
          return DruidPredicateMatch.of(upperComparing >= 0);
        };
      case LOWER_OPEN_UPPER_UNBOUNDED:
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          return DruidPredicateMatch.of(lowerComparing > 0);
        };
      case LOWER_CLOSED_UPPER_UNBOUNDED:
        return input -> {
          final int lowerComparing = Double.compare(input, lowerDoubleBound);
          return DruidPredicateMatch.of(lowerComparing >= 0);
        };
      case UNBOUNDED:
      default:
        return DruidDoublePredicate.ALWAYS_TRUE;
    }
  }

  public static <T> DruidObjectPredicate<T> makeComparatorPredicate(
      RangeType rangeType,
      Comparator<T> comparator,
      @Nullable T lowerBound,
      @Nullable T upperBound
  )
  {
    switch (rangeType) {
      case OPEN:
        return input -> {
          if (input == null) {
            return DruidPredicateMatch.UNKNOWN;
          }
          final int lowerComparing = comparator.compare(input, lowerBound);
          final int upperComparing = comparator.compare(upperBound, input);
          return DruidPredicateMatch.of(((lowerComparing > 0)) && (upperComparing > 0));
        };
      case LOWER_OPEN_UPPER_CLOSED:
        return input -> {
          if (input == null) {
            return DruidPredicateMatch.UNKNOWN;
          }
          final int lowerComparing = comparator.compare(input, lowerBound);
          final int upperComparing = comparator.compare(upperBound, input);
          return DruidPredicateMatch.of((lowerComparing > 0) && (upperComparing >= 0));
        };
      case LOWER_CLOSED_UPPER_OPEN:
        return input -> {
          if (input == null) {
            return DruidPredicateMatch.UNKNOWN;
          }
          final int lowerComparing = comparator.compare(input, lowerBound);
          final int upperComparing = comparator.compare(upperBound, input);
          return DruidPredicateMatch.of((lowerComparing >= 0) && (upperComparing > 0));
        };
      case CLOSED:
        return input -> {
          if (input == null) {
            return DruidPredicateMatch.UNKNOWN;
          }
          final int lowerComparing = comparator.compare(input, lowerBound);
          final int upperComparing = comparator.compare(upperBound, input);
          return DruidPredicateMatch.of((lowerComparing >= 0) && (upperComparing >= 0));
        };
      case LOWER_UNBOUNDED_UPPER_OPEN:
        return input -> {
          if (input == null) {
            return DruidPredicateMatch.UNKNOWN;
          }
          final int upperComparing = comparator.compare(upperBound, input);
          return DruidPredicateMatch.of(upperComparing > 0);
        };
      case LOWER_UNBOUNDED_UPPER_CLOSED:
        return input -> {
          if (input == null) {
            return DruidPredicateMatch.UNKNOWN;
          }
          final int upperComparing = comparator.compare(upperBound, input);
          return DruidPredicateMatch.of(upperComparing >= 0);
        };
      case LOWER_OPEN_UPPER_UNBOUNDED:
        return input -> {
          if (input == null) {
            return DruidPredicateMatch.UNKNOWN;
          }
          final int lowerComparing = comparator.compare(input, lowerBound);
          return DruidPredicateMatch.of(lowerComparing > 0);
        };
      case LOWER_CLOSED_UPPER_UNBOUNDED:
        return input -> {
          if (input == null) {
            return DruidPredicateMatch.UNKNOWN;
          }
          final int lowerComparing = comparator.compare(input, lowerBound);
          return DruidPredicateMatch.of(lowerComparing >= 0);
        };
      case UNBOUNDED:
      default:
        return DruidObjectPredicate.notNull();
    }
  }

  public enum RangeType
  {
    /**
     * (...)
     */
    OPEN,
    /**
     * [...]
     */
    CLOSED,
    /**
     * [...)
     */
    LOWER_CLOSED_UPPER_OPEN,
    /**
     * (...]
     */
    LOWER_OPEN_UPPER_CLOSED,
    /**
     * (...∞
     */
    LOWER_OPEN_UPPER_UNBOUNDED,
    /**
     * [...∞
     */
    LOWER_CLOSED_UPPER_UNBOUNDED,
    /**
     * -∞...)
     */
    LOWER_UNBOUNDED_UPPER_OPEN,
    /**
     * -∞...]
     */
    LOWER_UNBOUNDED_UPPER_CLOSED,
    /**
     * -∞...∞
     */
    UNBOUNDED;

    public static RangeType of(boolean hasLower, boolean lowerOpen, boolean hasUpper, boolean upperOpen)
    {
      if (hasLower && hasUpper) {
        if (lowerOpen) {
          return upperOpen ? OPEN : LOWER_OPEN_UPPER_CLOSED;
        } else {
          return upperOpen ? LOWER_CLOSED_UPPER_OPEN : CLOSED;
        }
      } else if (hasLower) {
        return lowerOpen ? LOWER_OPEN_UPPER_UNBOUNDED : LOWER_CLOSED_UPPER_UNBOUNDED;
      } else if (hasUpper) {
        return upperOpen ? LOWER_UNBOUNDED_UPPER_OPEN : LOWER_UNBOUNDED_UPPER_CLOSED;
      }
      return UNBOUNDED;
    }
  }

  private static class NumericStringArrayComparator implements Comparator<Object[]>
  {
    @Override
    public int compare(Object[] o1, Object[] o2)
    {
      //noinspection ArrayEquality
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      final int iter = Math.min(o1.length, o2.length);
      for (int i = 0; i < iter; i++) {
        final int cmp = StringComparators.NUMERIC.compare((String) o1[i], (String) o2[i]);
        if (cmp == 0) {
          continue;
        }
        return cmp;
      }
      return Integer.compare(o1.length, o2.length);
    }
  }
}
