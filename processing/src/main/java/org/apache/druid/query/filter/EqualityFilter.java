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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.DimensionPredicateFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.PredicateValueMatcherFactory;
import org.apache.druid.segment.filter.ValueMatchers;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndex;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class EqualityFilter extends AbstractOptimizableDimFilter implements Filter
{
  private final String column;
  private final ColumnType matchValueType;
  private final Object matchValue;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;
  private final DruidPredicateFactory predicateFactory;

  @JsonCreator
  public EqualityFilter(
      @JsonProperty("column") String column,
      @JsonProperty("matchValueType") ColumnType matchValueType,
      @JsonProperty("matchValue") Object matchValue,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    if (column == null) {
      throw InvalidInput.exception("Invalid equality filter, column cannot be null");
    }
    this.column = column;
    if (matchValueType == null) {
      throw InvalidInput.exception("Invalid equality filter on column [%s], matchValueType cannot be null", column);
    }
    this.matchValueType = matchValueType;
    if (matchValue == null) {
      throw InvalidInput.exception("Invalid equality filter on column [%s], matchValue cannot be null", column);
    }
    this.matchValue = matchValue;
    // remove once SQL planner no longer uses extractionFn
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;
    this.predicateFactory = new EqualityPredicateFactory(matchValue, matchValueType);
  }

  @Override
  public byte[] getCacheKey()
  {
    final TypeStrategy<Object> typeStrategy = matchValueType.getStrategy();
    final int size = typeStrategy.estimateSizeBytes(matchValue);
    final ByteBuffer valueBuffer = ByteBuffer.allocate(size);
    typeStrategy.write(valueBuffer, matchValue, size);
    return new CacheKeyBuilder(DimFilterUtils.EQUALS_CACHE_ID)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(column)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(matchValueType.asTypeString())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(valueBuffer.array())
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
    if (extractionFn == null) {
      return this;
    } else {
      return new DimensionPredicateFilter(column, predicateFactory, extractionFn, filterTuning);
    }
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

  @JsonProperty
  public Object getMatchValue()
  {
    return matchValue;
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
    DimFilter.DimFilterToStringBuilder bob =
        new DimFilter.DimFilterToStringBuilder().appendDimension(column, extractionFn)
                                                .append(" = ")
                                                .append(matchValue);

    if (!ColumnType.STRING.equals(matchValueType)) {
      bob.append(" (" + matchValueType.asTypeString() + ")");
    }
    return bob.appendFilterTuning(filterTuning).build();
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
    EqualityFilter that = (EqualityFilter) o;
    if (!column.equals(that.column)) {
      return false;
    }
    if (!Objects.equals(matchValueType, that.matchValueType)) {
      return false;
    }
    if (!Objects.equals(extractionFn, that.extractionFn)) {
      return false;
    }
    if (!Objects.equals(filterTuning, that.filterTuning)) {
      return false;
    }
    if (matchValueType.isArray()) {
      // just use predicate to see if the values are the same
      final ExprEval<?> thatValue = ExprEval.ofType(
          ExpressionType.fromColumnType(that.matchValueType),
          that.matchValue
      );
      final Predicate<Object[]> arrayPredicate = predicateFactory.makeArrayPredicate(matchValueType);
      return arrayPredicate.apply(thatValue.asArray());
    } else {
      return Objects.equals(matchValue, that.matchValue);
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(column, matchValueType, matchValue, extractionFn, filterTuning);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(getColumn(), dimension) || getExtractionFn() != null) {
      return null;
    }
    RangeSet<String> retSet = TreeRangeSet.create();
    retSet.add(Range.singleton(String.valueOf(matchValue)));
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
      return Filters.makeNullIndex(false, selector);
    }

    final StringValueSetIndex valueSetIndex = indexSupplier.as(StringValueSetIndex.class);
    if (valueSetIndex == null) {
      // column exists, but has no index
      return null;
    }
    return valueSetIndex.forValue(String.valueOf(matchValue));
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeProcessor(
        column,
        new TypedConstantValueMatcherFactory(matchValue, matchValueType),
        factory
    );
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    final ColumnCapabilities capabilities = factory.getColumnCapabilities(column);

    if (matchValueType.isPrimitive() && (capabilities == null || capabilities.isPrimitive())) {
      return ColumnProcessors.makeVectorProcessor(
          column,
          VectorValueMatcherColumnProcessorFactory.instance(),
          factory
      ).makeMatcher(matchValue, matchValueType);
    }
    return ColumnProcessors.makeVectorProcessor(
        column,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(new EqualityPredicateFactory(matchValue, matchValueType));
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
          columnRewrites
      );
    }

    return new EqualityFilter(
        rewriteDimensionTo,
        matchValueType,
        matchValue,
        extractionFn,
        filterTuning
    );
  }

  private static class EqualityPredicateFactory implements DruidPredicateFactory
  {
    private final ExprEval<?> matchValue;
    private final ColumnType matchValueType;

    private final Object initLock = new Object();

    private volatile DruidLongPredicate longPredicate;
    private volatile DruidFloatPredicate floatPredicate;
    private volatile DruidDoublePredicate doublePredicate;

    public EqualityPredicateFactory(Object matchValue, ColumnType matchValueType)
    {
      this.matchValue = ExprEval.ofType(ExpressionType.fromColumnType(matchValueType), matchValue);
      this.matchValueType = matchValueType;
    }

    @Override
    public Predicate<String> makeStringPredicate()
    {
      return Predicates.equalTo(matchValue.castTo(ExpressionType.STRING).asString());
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
      return doublePredicate;
    }

    @Override
    public Predicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> arrayType)
    {
      if (arrayType != null) {
        final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
        final Object[] matchArray = matchValue.castTo(ExpressionType.fromColumnType(arrayType)).asArray();
        return input -> arrayComparator.compare(input, matchArray) == 0;
      } else {
        // fall back to per row detection if input array type is unknown
        return input -> {
          final ExprEval<?> eval = ExprEval.bestEffortOf(input);
          final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();
          final Object[] matchArray = matchValue.castTo(eval.type()).asArray();
          return arrayComparator.compare(input, matchArray) == 0;
        };
      }
    }

    @Override
    public Predicate<Object> makeObjectPredicate()
    {
      if (matchValueType.equals(ColumnType.NESTED_DATA)) {
        return input -> Objects.equals(StructuredData.unwrap(input), StructuredData.unwrap(matchValue.value()));
      }
      return Predicates.equalTo(matchValue.valueOrDefault());
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
        final Long valueAsLong = (Long) matchValue.castTo(ExpressionType.LONG).valueOrDefault();

        if (valueAsLong == null) {
          longPredicate = DruidLongPredicate.ALWAYS_FALSE;
        } else {
          // store the primitive, so we don't unbox for every comparison
          final long unboxedLong = valueAsLong;
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
        final Double doubleValue = (Double) matchValue.castTo(ExpressionType.DOUBLE).valueOrDefault();

        if (doubleValue == null) {
          floatPredicate = DruidFloatPredicate.ALWAYS_FALSE;
        } else {
          // Compare with floatToIntBits instead of == to canonicalize NaNs.
          final int floatBits = Float.floatToIntBits(doubleValue.floatValue());
          floatPredicate = input -> Float.floatToIntBits(input) == floatBits;
        }
      }
    }

    private void initDoublePredicate()
    {
      if (doublePredicate != null) {
        return;
      }
      synchronized (initLock) {
        if (doublePredicate != null) {
          return;
        }
        final Double aDouble = (Double) matchValue.castTo(ExpressionType.DOUBLE).valueOrDefault();

        if (aDouble == null) {
          doublePredicate = DruidDoublePredicate.ALWAYS_FALSE;
        } else {
          // Compare with doubleToLongBits instead of == to canonicalize NaNs.
          final long bits = Double.doubleToLongBits(aDouble);
          doublePredicate = input -> Double.doubleToLongBits(input) == bits;
        }
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
      EqualityPredicateFactory that = (EqualityPredicateFactory) o;
      return Objects.equals(matchValue, that.matchValue) && Objects.equals(matchValueType, that.matchValueType);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(matchValue, matchValueType);
    }
  }

  private static class TypedConstantValueMatcherFactory implements ColumnProcessorFactory<ValueMatcher>
  {
    private final ExprEval<?> matchValue;
    private final ColumnType matchValueType;

    public TypedConstantValueMatcherFactory(Object matchValue, ColumnType matchValueType)
    {
      this.matchValue = ExprEval.ofType(ExpressionType.fromColumnType(matchValueType), matchValue);
      this.matchValueType = matchValueType;
    }

    @Override
    public ColumnType defaultType()
    {
      return ColumnType.UNKNOWN_COMPLEX;
    }

    @Override
    public ValueMatcher makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
    {
      return ValueMatchers.makeStringValueMatcher(
          selector,
          matchValue.castTo(ExpressionType.STRING).asString(),
          multiValue
      );
    }

    @Override
    public ValueMatcher makeFloatProcessor(BaseFloatColumnValueSelector selector)
    {
      return ValueMatchers.makeFloatValueMatcher(selector, (float) matchValue.castTo(ExpressionType.DOUBLE).asDouble());
    }

    @Override
    public ValueMatcher makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
    {
      return ValueMatchers.makeDoubleValueMatcher(selector, matchValue.castTo(ExpressionType.DOUBLE).asDouble());
    }

    @Override
    public ValueMatcher makeLongProcessor(BaseLongColumnValueSelector selector)
    {
      return ValueMatchers.makeLongValueMatcher(selector, matchValue.castTo(ExpressionType.LONG).asLong());
    }

    @Override
    public ValueMatcher makeArrayProcessor(
        BaseObjectColumnValueSelector<?> selector,
        ColumnCapabilities columnCapabilities
    )
    {
      return new PredicateValueMatcherFactory(
          new EqualityPredicateFactory(matchValue.valueOrDefault(), matchValueType)
      ).makeArrayProcessor(selector, columnCapabilities);
    }

    @Override
    public ValueMatcher makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
    {
      return new PredicateValueMatcherFactory(
          new EqualityPredicateFactory(matchValue.valueOrDefault(), matchValueType)
      ).makeComplexProcessor(selector);
    }
  }
}
