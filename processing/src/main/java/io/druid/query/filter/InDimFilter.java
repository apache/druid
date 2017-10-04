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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.lookup.LookupExtractor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.filter.InFilter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class InDimFilter implements DimFilter
{
  // determined through benchmark that binary search on long[] is faster than HashSet until ~16 elements
  // Hashing threshold is not applied to String for now, String still uses ImmutableSortedSet
  public static final int NUMERIC_HASHING_THRESHOLD = 16;

  private final ImmutableSortedSet<String> values;
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
  private final Supplier<DruidDoublePredicate> doublePredicateSupplier;

  @JsonCreator
  public InDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("values") Collection<String> values,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkArgument(values != null && !values.isEmpty(), "values can not be null or empty");
    this.values = ImmutableSortedSet.copyOf(
        Iterables.transform(
            values, new Function<String, String>()
            {
              @Override
              public String apply(String input)
              {
                return Strings.nullToEmpty(input);
              }

            }
        )
    );
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.longPredicateSupplier = getLongPredicateSupplier();
    this.floatPredicateSupplier = getFloatPredicateSupplier();
    this.doublePredicateSupplier = getDoublePredicateSupplier();
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

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[][] valuesBytes = new byte[values.size()][];
    int valuesBytesSize = 0;
    int index = 0;
    for (String value : values) {
      valuesBytes[index] = StringUtils.toUtf8(Strings.nullToEmpty(value));
      valuesBytesSize += valuesBytes[index].length + 1;
      ++index;
    }
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    ByteBuffer filterCacheKey = ByteBuffer.allocate(3
                                                    + dimensionBytes.length
                                                    + valuesBytesSize
                                                    + extractionFnBytes.length)
                                          .put(DimFilterUtils.IN_CACHE_ID)
                                          .put(dimensionBytes)
                                          .put(DimFilterUtils.STRING_SEPARATOR)
                                          .put(extractionFnBytes)
                                          .put(DimFilterUtils.STRING_SEPARATOR);
    for (byte[] bytes : valuesBytes) {
      filterCacheKey.put(bytes)
                    .put((byte) 0xFF);
    }
    return filterCacheKey.array();
  }

  @Override
  public DimFilter optimize()
  {
    InDimFilter inFilter = optimizeLookup();
    if (inFilter.values.size() == 1) {
      return new SelectorDimFilter(inFilter.dimension, inFilter.values.first(), inFilter.getExtractionFn());
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
        final String convertedValue = Strings.emptyToNull(value);
        if (!exFn.isRetainMissingValue() && Objects.equals(convertedValue, exFn.getReplaceMissingValueWith())) {
          return this;
        }
        keys.addAll(lookup.unapply(convertedValue));

        // If retainMissingValues is true and the selector value is not in the lookup map,
        // there may be row values that match the selector value but are not included
        // in the lookup map. Match on the selector value as well.
        // If the selector value is overwritten in the lookup map, don't add selector value to keys.
        if (exFn.isRetainMissingValue() && lookup.apply(convertedValue) == null) {
          keys.add(convertedValue);
        }
      }

      if (keys.isEmpty()) {
        return this;
      } else {
        return new InDimFilter(dimension, keys, null);
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
        extractionFn
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
      retSet.add(Range.singleton(Strings.nullToEmpty(value)));
    }
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

    InDimFilter that = (InDimFilter) o;

    if (values != null ? !values.equals(that.values) : that.values != null) {
      return false;
    }
    if (!dimension.equals(that.dimension)) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = values != null ? values.hashCode() : 0;
    result = 31 * result + dimension.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    final StringBuilder builder = new StringBuilder();

    if (extractionFn != null) {
      builder.append(extractionFn).append("(");
    }

    builder.append(dimension);

    if (extractionFn != null) {
      builder.append(")");
    }

    builder.append(" IN (").append(Joiner.on(", ").join(values)).append(")");

    return builder.toString();
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
