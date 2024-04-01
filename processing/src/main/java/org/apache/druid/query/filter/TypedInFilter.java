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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.Utf8ValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueSetIndexes;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Approximately like the SQL 'IN' filter, with the main difference being that this will match NULL values if contained
 * in the values list instead of ignoring them.
 * <p>
 * This is a typed version of {@link InDimFilter}, which allows the match values to exist in their native type and
 * sorted in their type native order for better performance matching against all column types.
 */
public class TypedInFilter extends AbstractOptimizableDimFilter implements Filter
{
  /**
   * Column to match {@link #sortedMatchValues} or {@link #sortedUtf8MatchValueBytes} against.
   */
  private final String column;

  /**
   * Type of values contained in {@link #sortedMatchValues}. This might be the same or different than the
   * {@link ColumnType} of {@link #column}, but is encouraged to be the same there are several optimizations available
   * if they match.
   */
  private final ColumnType matchValueType;

  /**
   * Unsorted values. This will be null if the values are found to be sorted, or have been already sorted "upstream".
   * Otherwise, this set of values will be lazily computed into {@link #sortedMatchValues} as needed, e.g. for
   * JSON serialization, cache key building, building a hashcode, or checking equality.
   */
  @Nullable
  private final List<?> unsortedValues;

  /**
   * Supplier for list of values sorted by {@link #matchValueType}. This is lazily computed if
   * {@link #unsortedValues} is not null and previously sorted. Data will be deduplicated upon sorting if computed.
   * Manually set this value with unsorted or duplicated values at your own risk. Duplicated values are unlikely to
   * cause a problem, but unsorted values can result in incorrect results.
   */
  private final Supplier<List<?>> sortedMatchValues;

  /**
   * Supplier for list of utf8 byte values sorted by {@link #matchValueType}. If {@link #sortedMatchValues} was supplied
   * directly instead of lazily computed, and {@link #matchValueType} is {@link ColumnType#STRING}, the backing list
   * will be eagerly computed. If {@link #sortedMatchValues} is lazily computed, this value will be null.
   */
  @Nullable
  private final Supplier<List<ByteBuffer>> sortedUtf8MatchValueBytes;
  @Nullable
  private final FilterTuning filterTuning;
  private final Supplier<DruidPredicateFactory> predicateFactorySupplier;
  @JsonIgnore
  private final Supplier<byte[]> cacheKeySupplier;

  /**
   * Creates a new filter.
   *
   * @param column         column to search
   * @param values         set of values to match, may or may not be sorted.
   * @param sortedValues   set of values to match this is sorted in matchValueType order. These values absolutely must
   *                       be sorted in the specified order for proper operation. This value is computed from values to
   *                       be used 'downstream' to avoid repeating the work of sorting and checking for sortedness over
   *                       and over.
   * @param matchValueType type of values contained in values/sortedValues
   * @param filterTuning   optional tuning
   */
  @JsonCreator
  public TypedInFilter(
      @JsonProperty("column") String column,
      @JsonProperty("matchValueType") ColumnType matchValueType,
      @JsonProperty("values") @Nullable List<?> values,
      @JsonProperty("sortedValues") @Nullable List<?> sortedValues,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    this.column = column;
    if (column == null) {
      throw InvalidInput.exception("Invalid IN filter, column cannot be null");
    }
    this.filterTuning = filterTuning;
    this.matchValueType = matchValueType;
    if (matchValueType == null) {
      throw InvalidInput.exception("Invalid IN filter on column [%s], matchValueType cannot be null", column);
    }
    // one of sorted or not sorted
    if ((values == null && sortedValues == null) || (values != null && sortedValues != null)) {
      throw InvalidInput.exception(
          "Invalid IN filter on column [%s], exactly one of values or sortedValues must be non-null",
          column
      );
    }
    if (sortedValues != null) {
      this.unsortedValues = null;
      this.sortedMatchValues = () -> sortedValues;
      if (matchValueType.is(ValueType.STRING)) {
        final List<ByteBuffer> matchValueBytes = Lists.newArrayListWithCapacity(sortedValues.size());
        for (Object s : sortedMatchValues.get()) {
          matchValueBytes.add(StringUtils.toUtf8ByteBuffer(Evals.asString(s)));
        }
        this.sortedUtf8MatchValueBytes = () -> matchValueBytes;
      } else {
        this.sortedUtf8MatchValueBytes = null;
      }
    } else {
      if (checkSorted(values, matchValueType)) {
        this.unsortedValues = null;
        this.sortedMatchValues = () -> values;
      } else {
        this.unsortedValues = values;
        this.sortedMatchValues = Suppliers.memoize(() -> sortValues(unsortedValues, matchValueType));
      }
      this.sortedUtf8MatchValueBytes = null;
    }

    this.predicateFactorySupplier = Suppliers.memoize(
        () -> new PredicateFactory(sortedMatchValues.get(), matchValueType)
    );
    this.cacheKeySupplier = Suppliers.memoize(this::computeCacheKey);
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
  }

  @JsonProperty
  public List<?> getSortedValues()
  {
    return sortedMatchValues.get();
  }

  @JsonProperty
  public ColumnType getMatchValueType()
  {
    return matchValueType;
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
    return cacheKeySupplier.get();
  }

  @Override
  public DimFilter optimize(final boolean mayIncludeUnknown)
  {
    checkSqlCompatible();
    final List<?> matchValues = this.sortedMatchValues.get();
    if (matchValues.isEmpty()) {
      return FalseDimFilter.instance();
    } else if (matchValues.size() == 1) {
      if (matchValues.get(0) == null) {
        return NullFilter.forColumn(column);
      }
      return new EqualityFilter(
          column,
          matchValueType,
          matchValues.iterator().next(),
          filterTuning
      );
    }
    return this;
  }

  @Override
  public Filter toFilter()
  {
    checkSqlCompatible();
    return this;
  }

  @Nullable
  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(getColumn(), dimension)) {
      return null;
    }
    RangeSet<String> retSet = TreeRangeSet.create();
    for (Object value : sortedMatchValues.get()) {
      String valueEquivalent = Evals.asString(value);
      if (valueEquivalent == null) {
        // Range.singleton(null) is invalid, so use the fact that
        // only null values are less than empty string.
        retSet.add(Range.lessThan(""));
      } else {
        retSet.add(Range.singleton(valueEquivalent));
      }
    }
    return retSet;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(column);
  }

  @Override
  @Nullable
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(column, selector, filterTuning)) {
      return null;
    }
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(column);

    if (indexSupplier == null) {
      // column doesn't exist, match against null
      DruidPredicateMatch match = predicateFactorySupplier.get().makeStringPredicate().apply(null);
      return Filters.makeMissingColumnNullIndex(match, selector);
    }

    if (sortedUtf8MatchValueBytes != null) {
      final Utf8ValueSetIndexes utf8ValueSetIndexes = indexSupplier.as(Utf8ValueSetIndexes.class);
      if (utf8ValueSetIndexes != null) {
        return utf8ValueSetIndexes.forSortedValuesUtf8(sortedUtf8MatchValueBytes.get());
      }
    }

    final ValueSetIndexes valueSetIndexes = indexSupplier.as(ValueSetIndexes.class);
    if (valueSetIndexes != null) {
      return valueSetIndexes.forSortedValues(sortedMatchValues.get(), matchValueType);
    }

    return Filters.makePredicateIndex(
        column,
        selector,
        predicateFactorySupplier.get()
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, column, predicateFactorySupplier.get());
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        column,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(predicateFactorySupplier.get());
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
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
      throw new IAE("Received a non-applicable rewrite: %s, filter's dimension: %s", columnRewrites, column);
    }

    if (rewriteDimensionTo.equals(column)) {
      return this;
    } else {
      return new TypedInFilter(
          rewriteDimensionTo,
          matchValueType,
          null,
          sortedMatchValues.get(),
          filterTuning
      );
    }
  }

  @Override
  public String toString()
  {
    final DimFilter.DimFilterToStringBuilder builder = new DimFilter.DimFilterToStringBuilder();
    return builder.appendDimension(column, null)
                  .append(" IN (")
                  .append(Joiner.on(", ").join(Iterables.transform(sortedMatchValues.get(), String::valueOf)))
                  .append(")")
                  .append(" (" + matchValueType + ")")
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
    TypedInFilter that = (TypedInFilter) o;
    return column.equals(that.column) &&
           Objects.equals(matchValueType, that.matchValueType) &&
           compareValues(sortedMatchValues.get(), that.sortedMatchValues.get(), matchValueType) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sortedMatchValues.get(), column, matchValueType, filterTuning);
  }

  private byte[] computeCacheKey()
  {
    // Hash all values, in sorted order, as their length followed by their content.
    final Hasher hasher = Hashing.sha256().newHasher();
    for (Object v : sortedMatchValues.get()) {
      if (v == null) {
        // Encode null as length -1, no content.
        hasher.putInt(-1);
      } else {
        final String s = Evals.asString(v);
        hasher.putInt(s.length());
        hasher.putString(s, StandardCharsets.UTF_8);
      }
    }

    return new CacheKeyBuilder(DimFilterUtils.TYPED_IN_CACHE_ID)
        .appendString(column)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(matchValueType.asTypeString())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(hasher.hash().asBytes())
        .build();
  }

  private void checkSqlCompatible()
  {
    if (NullHandling.replaceWithDefault()) {
      throw InvalidInput.exception(
          "Invalid IN filter, typed in filter only supports SQL compatible null handling mode, set druid.generic.useDefaultValue=false to use this filter"
      );
    }
  }

  private static boolean checkSorted(List<?> unsortedValues, ColumnType matchValueType)
  {
    final Comparator<Object> comparator = matchValueType.getNullableStrategy();
    Object prev = null;
    for (Object o : unsortedValues) {
      if (o != null) {
        Object coerced = coerceValue(o, matchValueType);
        //noinspection ObjectEquality
        if (coerced != o) {
          return false;
        }
      }
      if (prev != null && comparator.compare(prev, o) >= 0) {
        return false;
      }
      prev = o;
    }
    return true;
  }

  @Nullable
  private static Object coerceValue(@Nullable Object o, ColumnType matchValueType)
  {
    if (o == null) {
      return null;
    }
    switch (matchValueType.getType()) {
      case STRING:
        return DimensionHandlerUtils.convertObjectToString(o);
      case LONG:
        return DimensionHandlerUtils.convertObjectToLong(o);
      case FLOAT:
        return DimensionHandlerUtils.convertObjectToFloat(o);
      case DOUBLE:
        return DimensionHandlerUtils.convertObjectToDouble(o);
      default:
        throw InvalidInput.exception("Unsupported matchValueType[%s]", matchValueType);
    }
  }

  private static List<?> sortValues(List<?> unsortedValues, ColumnType matchValueType)
  {
    final Object[] array = unsortedValues.toArray(new Object[0]);
    // coerce values to matchValueType
    for (int i = 0; i < array.length; i++) {
      Object coerced = coerceValue(array[i], matchValueType);
      array[i] = coerced;
    }
    final Comparator<Object> comparator = matchValueType.getNullableStrategy();
    ObjectArrays.quickSort(array, comparator);
    // dedupe values
    final List<Object> sortedList = Lists.newArrayListWithCapacity(array.length);
    for (int i = 0; i < array.length; i++) {
      if (i > 0 && comparator.compare(array[i - 1], array[i]) == 0) {
        continue;
      }
      sortedList.add(array[i]);
    }
    return sortedList;
  }

  /**
   * Since jackson might translate longs into ints and such, we use the type comparator to check lists
   * for {@link #equals(Object)} instead of directly using {@link Objects#equals(Object, Object)}
   */
  private static boolean compareValues(List<?> o1, List<?> o2, ColumnType matchValueType)
  {
    final NullableTypeStrategy<Object> comparator = matchValueType.getNullableStrategy();
    //noinspection ObjectEquality
    if (o1 == o2) {
      return true;
    }
    if (o1 == null) {
      return false;
    }
    if (o2 == null) {
      return false;
    }
    final int size1 = o1.size();
    final int size2 = o2.size();
    if (size1 != size2) {
      return false;
    }
    for (int i = 0; i < size1; i++) {
      final int cmp = comparator.compare(o1.get(i), o2.get(i));
      if (cmp == 0) {
        continue;
      }
      return false;
    }
    return true;
  }

  private static DruidObjectPredicate<String> createStringPredicate(
      final List<?> sortedValues,
      final ColumnType matchValueType
  )
  {
    Preconditions.checkNotNull(sortedValues, "values");
    final boolean containsNull = !sortedValues.isEmpty() && sortedValues.get(0) == null;
    final Comparator<Object> comparator = matchValueType.getNullableStrategy();
    if (matchValueType.is(ValueType.STRING)) {
      return value -> {
        if (value == null) {
          return containsNull ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN;
        }
        final int index = Collections.binarySearch(sortedValues, value, comparator);
        return DruidPredicateMatch.of(index >= 0);
      };
    }
    // convert set to strings
    final Set<String> stringSet = Sets.newHashSetWithExpectedSize(sortedValues.size());
    for (Object o : sortedValues) {
      stringSet.add(Evals.asString(o));
    }
    return value -> {
      if (value == null) {
        return containsNull ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN;
      }
      return DruidPredicateMatch.of(stringSet.contains(value));
    };
  }

  private static DruidLongPredicate createLongPredicate(final List<?> sortedValues, ColumnType matchValueType)
  {
    boolean matchNulls = !sortedValues.isEmpty() && sortedValues.get(0) == null;
    if (matchValueType.is(ValueType.LONG)) {
      final Comparator<Object> comparator = matchValueType.getNullableStrategy();
      return new DruidLongPredicate()
      {
        @Override
        public DruidPredicateMatch applyLong(long input)
        {
          final int index = Collections.binarySearch(sortedValues, input, comparator);
          return DruidPredicateMatch.of(index >= 0);
        }

        @Override
        public DruidPredicateMatch applyNull()
        {
          return matchNulls ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN;
        }
      };
    }
    // convert set to longs
    LongOpenHashSet longs = new LongOpenHashSet();
    for (Object value : sortedValues) {
      final Long longValue = DimensionHandlerUtils.convertObjectToLong(value);
      if (longValue != null) {
        longs.add(longValue.longValue());
      }
    }
    return new DruidLongPredicate()
    {
      @Override
      public DruidPredicateMatch applyLong(long input)
      {
        return DruidPredicateMatch.of(longs.contains(input));
      }

      @Override
      public DruidPredicateMatch applyNull()
      {
        return matchNulls ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN;
      }
    };
  }

  private static DruidFloatPredicate createFloatPredicate(final List<?> sortedValues, ColumnType matchValueType)
  {
    boolean matchNulls = !sortedValues.isEmpty() && sortedValues.get(0) == null;
    if (matchValueType.is(ValueType.FLOAT)) {
      final Comparator<Object> comparator = matchValueType.getNullableStrategy();
      return new DruidFloatPredicate()
      {
        @Override
        public DruidPredicateMatch applyFloat(float input)
        {
          final int index = Collections.binarySearch(sortedValues, input, comparator);
          return DruidPredicateMatch.of(index >= 0);
        }

        @Override
        public DruidPredicateMatch applyNull()
        {
          return matchNulls ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN;
        }
      };
    }
    // convert set to floats
    final FloatOpenHashSet floatSet = new FloatOpenHashSet();
    for (Object value : sortedValues) {
      final Float floatValue = DimensionHandlerUtils.convertObjectToFloat(value);
      if (floatValue != null) {
        floatSet.add(floatValue.floatValue());
      }
    }
    return new DruidFloatPredicate()
    {
      @Override
      public DruidPredicateMatch applyFloat(float input)
      {
        return DruidPredicateMatch.of(floatSet.contains(input));
      }

      @Override
      public DruidPredicateMatch applyNull()
      {
        return matchNulls ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN;
      }
    };
  }

  private static DruidDoublePredicate createDoublePredicate(final List<?> sortedValues, ColumnType matchValueType)
  {
    boolean matchNulls = !sortedValues.isEmpty() && sortedValues.get(0) == null;
    if (matchValueType.is(ValueType.DOUBLE)) {
      final Comparator<Object> comparator = matchValueType.getNullableStrategy();
      return new DruidDoublePredicate()
      {
        @Override
        public DruidPredicateMatch applyDouble(double input)
        {
          final int index = Collections.binarySearch(sortedValues, input, comparator);
          return DruidPredicateMatch.of(index >= 0);
        }

        @Override
        public DruidPredicateMatch applyNull()
        {
          return matchNulls ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN;
        }
      };
    }

    // convert set to doubles
    final DoubleOpenHashSet doubleSet = new DoubleOpenHashSet(sortedValues.size());
    for (Object value : sortedValues) {
      Double doubleValue = DimensionHandlerUtils.convertObjectToDouble(value);
      if (doubleValue != null) {
        doubleSet.add(doubleValue.doubleValue());
      }
    }
    return new DruidDoublePredicate()
    {
      @Override
      public DruidPredicateMatch applyDouble(double input)
      {
        return DruidPredicateMatch.of(doubleSet.contains(input));
      }

      @Override
      public DruidPredicateMatch applyNull()
      {
        return matchNulls ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN;
      }
    };
  }

  public static class PredicateFactory implements DruidPredicateFactory
  {
    private final ColumnType matchValueType;
    private final List<?> sortedValues;
    private final Supplier<DruidObjectPredicate<String>> stringPredicateSupplier;
    private final Supplier<DruidLongPredicate> longPredicateSupplier;
    private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
    private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

    public PredicateFactory(final List<?> sortedValues, final ColumnType matchValueType)
    {
      this.sortedValues = sortedValues;
      this.matchValueType = matchValueType;

      // As the set of filtered values can be large, parsing them as numbers should be done only if needed, and
      // only once. Pass in a common long predicate supplier to all filters created by .toFilter(), so that we only
      // compute the long hashset/array once per query. This supplier must be thread-safe, since this DimFilter will be
      // accessed in the query runners.
      this.stringPredicateSupplier = Suppliers.memoize(() -> createStringPredicate(sortedValues, matchValueType));
      this.longPredicateSupplier = Suppliers.memoize(() -> createLongPredicate(sortedValues, matchValueType));
      this.floatPredicateSupplier = Suppliers.memoize(() -> createFloatPredicate(sortedValues, matchValueType));
      this.doublePredicateSupplier = Suppliers.memoize(() -> createDoublePredicate(sortedValues, matchValueType));
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate()
    {
      return stringPredicateSupplier.get();
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      return longPredicateSupplier.get();
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      return floatPredicateSupplier.get();
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      return doublePredicateSupplier.get();
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
      PredicateFactory that = (PredicateFactory) o;
      return Objects.equals(matchValueType, that.matchValueType) &&
             Objects.equals(sortedValues, that.sortedValues);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(matchValueType, sortedValues);
    }
  }
}
