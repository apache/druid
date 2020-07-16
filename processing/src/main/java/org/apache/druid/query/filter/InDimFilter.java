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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.filter.InFilter;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class InDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  // determined through benchmark that binary search on long[] is faster than HashSet until ~16 elements
  // Hashing threshold is not applied to String for now, String still uses ImmutableSortedSet
  public static final int NUMERIC_HASHING_THRESHOLD = 16;

  // Values can contain `null` object
  private final Set<String> values;
  private final String dimension;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

  @JsonIgnore
  private byte[] cacheKey;

  @JsonCreator
  public InDimFilter(
      @JsonProperty("dimension") String dimension,
      // This 'values' collection instance can be reused if possible to avoid copying a big collection.
      // Callers should _not_ modify the collection after it is passed to this constructor.
      @JsonProperty("values") Set<String> values,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkArgument(values != null, "values can not be null");

    // The values set can be huge. Try to avoid copying the set if possible.
    // Note that we may still need to copy values to a list for caching. See getCacheKey().
    if ((NullHandling.sqlCompatible() || values.stream().noneMatch(NullHandling::needsEmptyToNull))) {
      this.values = values;
    } else {
      this.values = values.stream().map(NullHandling::emptyToNullIfNeeded).collect(Collectors.toSet());
    }
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;
    this.longPredicateSupplier = getLongPredicateSupplier();
    this.floatPredicateSupplier = getFloatPredicateSupplier();
    this.doublePredicateSupplier = getDoublePredicateSupplier();
  }

  /**
   * This constructor should be called only in unit tests since it creates a new hash set wrapping the given values.
   */
  @VisibleForTesting
  public InDimFilter(String dimension, Collection<String> values, @Nullable ExtractionFn extractionFn)
  {
    this(dimension, new HashSet<>(values), extractionFn, null);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public Set<String> getValues()
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
    if (cacheKey == null) {
      final List<String> sortedValues = new ArrayList<>(values);
      sortedValues.sort(Comparator.nullsFirst(Ordering.natural()));
      final Hasher hasher = Hashing.sha256().newHasher();
      for (String v : sortedValues) {
        if (v == null) {
          hasher.putInt(0);
        } else {
          hasher.putString(v, StandardCharsets.UTF_8);
        }
      }
      cacheKey = new CacheKeyBuilder(DimFilterUtils.IN_CACHE_ID)
          .appendString(dimension)
          .appendByte(DimFilterUtils.STRING_SEPARATOR)
          .appendByteArray(extractionFn == null ? new byte[0] : extractionFn.getCacheKey())
          .appendByte(DimFilterUtils.STRING_SEPARATOR)
          .appendByteArray(hasher.hash().asBytes())
          .build();
    }
    return cacheKey;
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

  private InDimFilter optimizeLookup()
  {
    if (extractionFn instanceof LookupExtractionFn
        && ((LookupExtractionFn) extractionFn).isOptimize()) {
      LookupExtractionFn exFn = (LookupExtractionFn) extractionFn;
      LookupExtractor lookup = exFn.getLookup();

      final Set<String> keys = new HashSet<>();
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

  @Override
  public Filter toFilter()
  {
    return new InFilter(
        dimension,
        values,
        longPredicateSupplier,
        floatPredicateSupplier,
        doublePredicateSupplier,
        extractionFn,
        filterTuning
    );
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

  private DruidLongPredicate createLongPredicate()
  {
    LongArrayList longs = new LongArrayList(values.size());
    for (String value : values) {
      final Long longValue = DimensionHandlerUtils.convertObjectToLong(value);
      if (longValue != null) {
        longs.add((long) longValue);
      }
    }

    if (longs.size() > NUMERIC_HASHING_THRESHOLD) {
      final LongOpenHashSet longHashSet = new LongOpenHashSet(longs);
      return longHashSet::contains;
    } else {
      final long[] longArray = longs.toLongArray();
      Arrays.sort(longArray);
      return input -> Arrays.binarySearch(longArray, input) >= 0;
    }
  }

  // As the set of filtered values can be large, parsing them as longs should be done only if needed, and only once.
  // Pass in a common long predicate supplier to all filters created by .toFilter(), so that
  // we only compute the long hashset/array once per query.
  // This supplier must be thread-safe, since this DimFilter will be accessed in the query runners.
  private Supplier<DruidLongPredicate> getLongPredicateSupplier()
  {
    Supplier<DruidLongPredicate> longPredicate = this::createLongPredicate;
    return Suppliers.memoize(longPredicate);
  }

  private DruidFloatPredicate createFloatPredicate()
  {
    IntArrayList floatBits = new IntArrayList(values.size());
    for (String value : values) {
      Float floatValue = DimensionHandlerUtils.convertObjectToFloat(value);
      if (floatValue != null) {
        floatBits.add(Float.floatToIntBits(floatValue));
      }
    }

    if (floatBits.size() > NUMERIC_HASHING_THRESHOLD) {
      final IntOpenHashSet floatBitsHashSet = new IntOpenHashSet(floatBits);

      return input -> floatBitsHashSet.contains(Float.floatToIntBits(input));
    } else {
      final int[] floatBitsArray = floatBits.toIntArray();
      Arrays.sort(floatBitsArray);

      return input -> Arrays.binarySearch(floatBitsArray, Float.floatToIntBits(input)) >= 0;
    }
  }

  private Supplier<DruidFloatPredicate> getFloatPredicateSupplier()
  {
    Supplier<DruidFloatPredicate> floatPredicate = this::createFloatPredicate;
    return Suppliers.memoize(floatPredicate);
  }

  private DruidDoublePredicate createDoublePredicate()
  {
    LongArrayList doubleBits = new LongArrayList(values.size());
    for (String value : values) {
      Double doubleValue = DimensionHandlerUtils.convertObjectToDouble(value);
      if (doubleValue != null) {
        doubleBits.add(Double.doubleToLongBits((doubleValue)));
      }
    }

    if (doubleBits.size() > NUMERIC_HASHING_THRESHOLD) {
      final LongOpenHashSet doubleBitsHashSet = new LongOpenHashSet(doubleBits);

      return input -> doubleBitsHashSet.contains(Double.doubleToLongBits(input));
    } else {
      final long[] doubleBitsArray = doubleBits.toLongArray();
      Arrays.sort(doubleBitsArray);

      return input -> Arrays.binarySearch(doubleBitsArray, Double.doubleToLongBits(input)) >= 0;
    }
  }

  private Supplier<DruidDoublePredicate> getDoublePredicateSupplier()
  {
    Supplier<DruidDoublePredicate> doublePredicate = this::createDoublePredicate;
    return Suppliers.memoize(doublePredicate);
  }
}
