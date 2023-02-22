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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ForwardingSortedSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.column.Utf8ValueSetIndex;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class InDimFilter extends AbstractOptimizableDimFilter implements Filter
{
  // Values can contain `null` object. Values are sorted (nulls-first).
  private final ValuesSet values;
  // Computed eagerly, not lazily, because lazy computations would block all processing threads for a given query.
  private final SortedSet<ByteBuffer> valuesUtf8;
  private final String dimension;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;
  private final DruidPredicateFactory predicateFactory;

  @JsonIgnore
  private final Supplier<byte[]> cacheKeySupplier;

  /**
   * Creates a new filter.
   *
   * @param dimension    column to search
   * @param values       set of values to match. This collection may be reused to avoid copying a big collection.
   *                     Therefore, callers should <b>not</b> modify the collection after it is passed to this
   *                     constructor.
   * @param extractionFn extraction function to apply to the column before checking against "values"
   * @param filterTuning optional tuning
   */
  @JsonCreator
  public InDimFilter(
      @JsonProperty("dimension") String dimension,
      // This 'values' collection instance can be reused if possible to avoid copying a big collection.
      // Callers should _not_ modify the collection after it is passed to this constructor.
      @JsonProperty("values") ValuesSet values,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    this(
        dimension,
        values,
        extractionFn,
        filterTuning,
        null
    );
  }

  /**
   * Creates a new filter without an extraction function or any special filter tuning.
   *
   * @param dimension column to search
   * @param values    set of values to match. If this collection is a {@link SortedSet}, it may be reused to avoid
   *                  copying a big collection. Therefore, callers should <b>not</b> modify the collection after it
   *                  is passed to this constructor.
   */
  public InDimFilter(String dimension, Set<String> values)
  {
    this(
        dimension,
        values instanceof ValuesSet ? (ValuesSet) values : new ValuesSet(values),
        null,
        null,
        null
    );
  }

  /**
   * Creates a new filter without an extraction function or any special filter tuning.
   *
   * @param dimension    column to search
   * @param values       set of values to match. If this collection is a {@link SortedSet}, it may be reused to avoid
   *                     copying a big collection. Therefore, callers should <b>not</b> modify the collection after it
   *                     is passed to this constructor.
   * @param extractionFn extraction function to apply to the column before checking against "values"
   */
  public InDimFilter(String dimension, Collection<String> values, @Nullable ExtractionFn extractionFn)
  {
    this(
        dimension,
        values instanceof ValuesSet ? (ValuesSet) values : new ValuesSet(values),
        extractionFn,
        null,
        null
    );
  }

  /**
   * Internal constructor.
   */
  private InDimFilter(
      final String dimension,
      final ValuesSet values,
      @Nullable final ExtractionFn extractionFn,
      @Nullable final FilterTuning filterTuning,
      @Nullable final DruidPredicateFactory predicateFactory
  )
  {
    Preconditions.checkNotNull(values, "values cannot be null");

    this.values = values;

    if (!NullHandling.sqlCompatible() && values.contains("")) {
      // In non-SQL-compatible mode, empty strings must be converted to nulls for the filter.
      this.values.remove("");
      this.values.add(null);
    }

    this.valuesUtf8 = this.values.toUtf8();
    this.dimension = Preconditions.checkNotNull(dimension, "dimension cannot be null");
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;

    if (predicateFactory != null) {
      this.predicateFactory = predicateFactory;
    } else {
      this.predicateFactory = new InFilterDruidPredicateFactory(extractionFn, this.values);
    }

    this.cacheKeySupplier = Suppliers.memoize(this::computeCacheKey);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public SortedSet<String> getValues()
  {
    return values;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
  public byte[] getCacheKey()
  {
    return cacheKeySupplier.get();
  }

  @Override
  public DimFilter optimize()
  {
    InDimFilter inFilter = optimizeLookup();

    if (inFilter.values.isEmpty()) {
      return FalseDimFilter.instance();
    }
    if (inFilter.values.size() == 1) {
      return new SelectorDimFilter(
          inFilter.dimension,
          inFilter.values.iterator().next(),
          inFilter.getExtractionFn(),
          filterTuning
      );
    }
    return inFilter;
  }

  @Override
  public Filter toFilter()
  {
    return this;
  }

  @Nullable
  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(getDimension(), dimension) || getExtractionFn() != null) {
      return null;
    }
    RangeSet<String> retSet = TreeRangeSet.create();
    for (String value : values) {
      String valueEquivalent = NullHandling.nullToEmptyIfNeeded(value);
      if (valueEquivalent == null) {
        // Case when SQL compatible null handling is enabled
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
    return ImmutableSet.of(dimension);
  }

  @Override
  @Nullable
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(dimension, selector, filterTuning)) {
      return null;
    }
    if (extractionFn == null) {
      final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(dimension);

      if (indexSupplier == null) {
        // column doesn't exist, match against null
        return Filters.makeNullIndex(
            predicateFactory.makeStringPredicate().apply(null),
            selector
        );
      }

      final Utf8ValueSetIndex utf8ValueSetIndex = indexSupplier.as(Utf8ValueSetIndex.class);
      if (utf8ValueSetIndex != null) {
        return utf8ValueSetIndex.forSortedValuesUtf8(valuesUtf8);
      }

      final StringValueSetIndex stringValueSetIndex = indexSupplier.as(StringValueSetIndex.class);
      if (stringValueSetIndex != null) {
        return stringValueSetIndex.forSortedValues(values);
      }
    }
    return Filters.makePredicateIndex(
        dimension,
        selector,
        predicateFactory
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, dimension, predicateFactory);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        dimension,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(predicateFactory);
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
    String rewriteDimensionTo = columnRewrites.get(dimension);
    if (rewriteDimensionTo == null) {
      throw new IAE("Received a non-applicable rewrite: %s, filter's dimension: %s", columnRewrites, dimension);
    }

    if (rewriteDimensionTo.equals(dimension)) {
      return this;
    } else {
      return new InDimFilter(
          rewriteDimensionTo,
          values,
          extractionFn,
          filterTuning,
          predicateFactory
      );
    }
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector)
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }

  @Override
  public String toString()
  {
    final DimFilterToStringBuilder builder = new DimFilterToStringBuilder();
    return builder.appendDimension(dimension, extractionFn)
                  .append(" IN (")
                  .append(Joiner.on(", ").join(Iterables.transform(values, StringUtils::nullToEmptyNonDruidDataString)))
                  .append(")")
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
    InDimFilter that = (InDimFilter) o;
    return values.equals(that.values) &&
           dimension.equals(that.dimension) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(values, dimension, extractionFn, filterTuning);
  }

  private byte[] computeCacheKey()
  {
    // Hash all values, in sorted order, as their length followed by their content.
    final Hasher hasher = Hashing.sha256().newHasher();
    for (String v : values) {
      if (v == null) {
        // Encode null as length -1, no content.
        hasher.putInt(-1);
      } else {
        hasher.putInt(v.length());
        hasher.putString(v, StandardCharsets.UTF_8);
      }
    }

    return new CacheKeyBuilder(DimFilterUtils.IN_CACHE_ID)
        .appendString(dimension)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(extractionFn == null ? new byte[0] : extractionFn.getCacheKey())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(hasher.hash().asBytes())
        .build();
  }

  private InDimFilter optimizeLookup()
  {
    if (extractionFn instanceof LookupExtractionFn
        && ((LookupExtractionFn) extractionFn).isOptimize()) {
      LookupExtractionFn exFn = (LookupExtractionFn) extractionFn;
      LookupExtractor lookup = exFn.getLookup();

      final ValuesSet keys = new ValuesSet();
      for (String value : values) {

        // We cannot do an unapply()-based optimization if the selector value
        // and the replaceMissingValuesWith value are the same, since we have to match on
        // all values that are not present in the lookup.
        final String convertedValue = NullHandling.emptyToNullIfNeeded(value);
        if (!exFn.isRetainMissingValue() && Objects.equals(convertedValue, exFn.getReplaceMissingValueWith())) {
          return this;
        }
        keys.addAll(lookup.unapply(convertedValue));

        // If retainMissingValues is true and the selector value is not in the lookup map,
        // there may be row values that match the selector value but are not included
        // in the lookup map. Match on the selector value as well.
        // If the selector value is overwritten in the lookup map, don't add selector value to keys.
        if (exFn.isRetainMissingValue() && NullHandling.isNullOrEquivalent(lookup.apply(convertedValue))) {
          keys.add(convertedValue);
        }
      }

      if (keys.isEmpty()) {
        return this;
      } else {
        return new InDimFilter(dimension, keys, null, filterTuning);
      }
    }
    return this;
  }

  @SuppressWarnings("ReturnValueIgnored")
  private static Predicate<String> createStringPredicate(final Set<String> values)
  {
    Preconditions.checkNotNull(values, "values");
    return values::contains;
  }

  private static DruidLongPredicate createLongPredicate(final Set<String> values)
  {
    LongArrayList longs = new LongArrayList(values.size());
    for (String value : values) {
      final Long longValue = DimensionHandlerUtils.convertObjectToLong(value);
      if (longValue != null) {
        longs.add((long) longValue);
      }
    }

    final LongOpenHashSet longHashSet = new LongOpenHashSet(longs);
    final boolean matchNull = values.contains(null);
    return new DruidLongPredicate()
    {
      @Override
      public boolean applyLong(long n)
      {
        return longHashSet.contains(n);
      }

      @Override
      public boolean applyNull()
      {
        return matchNull;
      }
    };
  }

  private static DruidFloatPredicate createFloatPredicate(final Set<String> values)
  {
    IntArrayList floatBits = new IntArrayList(values.size());
    for (String value : values) {
      Float floatValue = DimensionHandlerUtils.convertObjectToFloat(value);
      if (floatValue != null) {
        floatBits.add(Float.floatToIntBits(floatValue));
      }
    }

    final IntOpenHashSet floatBitsHashSet = new IntOpenHashSet(floatBits);
    final boolean matchNull = values.contains(null);
    return new DruidFloatPredicate()
    {
      @Override
      public boolean applyFloat(float n)
      {
        return floatBitsHashSet.contains(Float.floatToIntBits(n));
      }

      @Override
      public boolean applyNull()
      {
        return matchNull;
      }
    };
  }

  private static DruidDoublePredicate createDoublePredicate(final Set<String> values)
  {
    LongArrayList doubleBits = new LongArrayList(values.size());
    for (String value : values) {
      Double doubleValue = DimensionHandlerUtils.convertObjectToDouble(value);
      if (doubleValue != null) {
        doubleBits.add(Double.doubleToLongBits((doubleValue)));
      }
    }

    final LongOpenHashSet doubleBitsHashSet = new LongOpenHashSet(doubleBits);
    final boolean matchNull = values.contains(null);
    return new DruidDoublePredicate()
    {
      @Override
      public boolean applyDouble(double n)
      {
        return doubleBitsHashSet.contains(Double.doubleToLongBits(n));
      }

      @Override
      public boolean applyNull()
      {
        return matchNull;
      }
    };
  }

  @VisibleForTesting
  public static class InFilterDruidPredicateFactory implements DruidPredicateFactory
  {
    private final ExtractionFn extractionFn;
    private final Set<String> values;
    private final Supplier<Predicate<String>> stringPredicateSupplier;
    private final Supplier<DruidLongPredicate> longPredicateSupplier;
    private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
    private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

    public InFilterDruidPredicateFactory(
        final ExtractionFn extractionFn,
        final ValuesSet values
    )
    {
      this.extractionFn = extractionFn;
      this.values = values;

      // As the set of filtered values can be large, parsing them as numbers should be done only if needed, and
      // only once. Pass in a common long predicate supplier to all filters created by .toFilter(), so that we only
      // compute the long hashset/array once per query. This supplier must be thread-safe, since this DimFilter will be
      // accessed in the query runners.
      this.stringPredicateSupplier = Suppliers.memoize(() -> createStringPredicate(values));
      this.longPredicateSupplier = Suppliers.memoize(() -> createLongPredicate(values));
      this.floatPredicateSupplier = Suppliers.memoize(() -> createFloatPredicate(values));
      this.doublePredicateSupplier = Suppliers.memoize(() -> createDoublePredicate(values));
    }

    @Override
    public Predicate<String> makeStringPredicate()
    {
      if (extractionFn != null) {
        final Predicate<String> stringPredicate = stringPredicateSupplier.get();
        return input -> stringPredicate.apply(extractionFn.apply(input));
      } else {
        return stringPredicateSupplier.get();
      }
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      if (extractionFn != null) {
        final Predicate<String> stringPredicate = stringPredicateSupplier.get();
        return input -> stringPredicate.apply(extractionFn.apply(input));
      } else {
        return longPredicateSupplier.get();
      }
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      if (extractionFn != null) {
        final Predicate<String> stringPredicate = stringPredicateSupplier.get();
        return input -> stringPredicate.apply(extractionFn.apply(input));
      } else {
        return floatPredicateSupplier.get();
      }
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      if (extractionFn != null) {
        final Predicate<String> stringPredicate = stringPredicateSupplier.get();
        return input -> stringPredicate.apply(extractionFn.apply(input));
      } else {
        return doublePredicateSupplier.get();
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
      InFilterDruidPredicateFactory that = (InFilterDruidPredicateFactory) o;
      return Objects.equals(extractionFn, that.extractionFn) &&
             Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(extractionFn, values);
    }
  }

  public static class ValuesSet extends ForwardingSortedSet<String>
  {
    private final SortedSet<String> values;

    public ValuesSet()
    {
      this.values = new TreeSet<>(Comparators.naturalNullsFirst());
    }

    /**
     * Create a ValuesSet from another Collection. The Collection will be reused if it is a {@link SortedSet} with
     * an appropriate comparator.
     */
    public ValuesSet(final Collection<String> values)
    {
      if (values instanceof SortedSet && Comparators.naturalNullsFirst()
                                                    .equals(((SortedSet<String>) values).comparator())) {
        this.values = (SortedSet<String>) values;
      } else {
        this.values = new TreeSet<>(Comparators.naturalNullsFirst());
        this.values.addAll(values);
      }
    }

    public SortedSet<ByteBuffer> toUtf8()
    {
      final TreeSet<ByteBuffer> valuesUtf8 = new TreeSet<>(ByteBufferUtils.utf8Comparator());

      for (final String value : values) {
        if (value == null) {
          valuesUtf8.add(null);
        } else {
          valuesUtf8.add(ByteBuffer.wrap(StringUtils.toUtf8(value)));
        }
      }

      return valuesUtf8;
    }

    @Override
    protected SortedSet<String> delegate()
    {
      return values;
    }
  }
}
