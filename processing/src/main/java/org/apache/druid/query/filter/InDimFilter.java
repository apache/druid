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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.filter.InFilter;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class InDimFilter implements DimFilter
{
  // determined through benchmark that binary search on long[] is faster than HashSet until ~16 elements
  // Hashing threshold is not applied to String for now, String still uses ImmutableSortedSet
  public static final int NUMERIC_HASHING_THRESHOLD = 16;

  // Values can contain `null` object
  private final SortedSet<String> values;
  private final String dimension;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

  @JsonCreator
  public InDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("values") Collection<String> values,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkArgument(values != null, "values can not be null");

    this.values = new TreeSet<>(Comparators.naturalNullsFirst());
    for (String value : values) {
      this.values.add(NullHandling.emptyToNullIfNeeded(value));
    }
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;
    this.longPredicateSupplier = getLongPredicateSupplier();
    this.floatPredicateSupplier = getFloatPredicateSupplier();
    this.doublePredicateSupplier = getDoublePredicateSupplier();
  }

  @VisibleForTesting
  public InDimFilter(String dimension, Collection<String> values, @Nullable ExtractionFn extractionFn)
  {
    this(dimension, values, extractionFn, null);
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
    boolean hasNull = false;
    for (String value : values) {
      if (value == null) {
        hasNull = true;
        break;
      }
    }
    return new CacheKeyBuilder(DimFilterUtils.IN_CACHE_ID)
        .appendString(dimension)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(extractionFn == null ? new byte[0] : extractionFn.getCacheKey())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByte(hasNull ? NullHandling.IS_NULL_BYTE : NullHandling.IS_NOT_NULL_BYTE)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendStrings(values).build();
  }

  @Override
  public DimFilter optimize()
  {
    InDimFilter inFilter = optimizeLookup();
    if (inFilter.values.size() == 1) {
      return new SelectorDimFilter(inFilter.dimension, inFilter.values.first(), inFilter.getExtractionFn(), filterTuning);
    }
    return inFilter;
  }

  private InDimFilter optimizeLookup()
  {
    if (extractionFn instanceof LookupExtractionFn
        && ((LookupExtractionFn) extractionFn).isOptimize()) {
      LookupExtractionFn exFn = (LookupExtractionFn) extractionFn;
      LookupExtractor lookup = exFn.getLookup();

      final List<String> keys = new ArrayList<>();
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

  // As the set of filtered values can be large, parsing them as longs should be done only if needed, and only once.
  // Pass in a common long predicate supplier to all filters created by .toFilter(), so that
  // we only compute the long hashset/array once per query.
  // This supplier must be thread-safe, since this DimFilter will be accessed in the query runners.
  private Supplier<DruidLongPredicate> getLongPredicateSupplier()
  {
    return new Supplier<DruidLongPredicate>()
    {
      private final Object initLock = new Object();
      private DruidLongPredicate predicate;


      private void initLongValues()
      {
        if (predicate != null) {
          return;
        }

        synchronized (initLock) {
          if (predicate != null) {
            return;
          }

          LongArrayList longs = new LongArrayList(values.size());
          for (String value : values) {
            final Long longValue = DimensionHandlerUtils.getExactLongFromDecimalString(value);
            if (longValue != null) {
              longs.add(longValue);
            }
          }

          if (longs.size() > NUMERIC_HASHING_THRESHOLD) {
            final LongOpenHashSet longHashSet = new LongOpenHashSet(longs);

            predicate = input -> longHashSet.contains(input);
          } else {
            final long[] longArray = longs.toLongArray();
            Arrays.sort(longArray);

            predicate = input -> Arrays.binarySearch(longArray, input) >= 0;
          }
        }
      }

      @Override
      public DruidLongPredicate get()
      {
        initLongValues();
        return predicate;
      }
    };
  }

  private Supplier<DruidFloatPredicate> getFloatPredicateSupplier()
  {
    return new Supplier<DruidFloatPredicate>()
    {
      private final Object initLock = new Object();
      private DruidFloatPredicate predicate;

      private void initFloatValues()
      {
        if (predicate != null) {
          return;
        }

        synchronized (initLock) {
          if (predicate != null) {
            return;
          }

          IntArrayList floatBits = new IntArrayList(values.size());
          for (String value : values) {
            Float floatValue = Floats.tryParse(value);
            if (floatValue != null) {
              floatBits.add(Float.floatToIntBits(floatValue));
            }
          }

          if (floatBits.size() > NUMERIC_HASHING_THRESHOLD) {
            final IntOpenHashSet floatBitsHashSet = new IntOpenHashSet(floatBits);

            predicate = input -> floatBitsHashSet.contains(Float.floatToIntBits(input));
          } else {
            final int[] floatBitsArray = floatBits.toIntArray();
            Arrays.sort(floatBitsArray);

            predicate = input -> Arrays.binarySearch(floatBitsArray, Float.floatToIntBits(input)) >= 0;
          }
        }
      }

      @Override
      public DruidFloatPredicate get()
      {
        initFloatValues();
        return predicate;
      }
    };
  }

  private Supplier<DruidDoublePredicate> getDoublePredicateSupplier()
  {
    return new Supplier<DruidDoublePredicate>()
    {
      private final Object initLock = new Object();
      private DruidDoublePredicate predicate;

      private void initDoubleValues()
      {
        if (predicate != null) {
          return;
        }

        synchronized (initLock) {
          if (predicate != null) {
            return;
          }

          LongArrayList doubleBits = new LongArrayList(values.size());
          for (String value : values) {
            Double doubleValue = Doubles.tryParse(value);
            if (doubleValue != null) {
              doubleBits.add(Double.doubleToLongBits((doubleValue)));
            }
          }

          if (doubleBits.size() > NUMERIC_HASHING_THRESHOLD) {
            final LongOpenHashSet doubleBitsHashSet = new LongOpenHashSet(doubleBits);

            predicate = input -> doubleBitsHashSet.contains(Double.doubleToLongBits(input));
          } else {
            final long[] doubleBitsArray = doubleBits.toLongArray();
            Arrays.sort(doubleBitsArray);

            predicate = input -> Arrays.binarySearch(doubleBitsArray, Double.doubleToLongBits(input)) >= 0;
          }
        }
      }
      @Override
      public DruidDoublePredicate get()
      {
        initDoubleValues();
        return predicate;
      }
    };
  }
}
