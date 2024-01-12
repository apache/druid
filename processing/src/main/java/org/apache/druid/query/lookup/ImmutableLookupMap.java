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

package org.apache.druid.query.lookup;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.extraction.MapLookupExtractor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Similar to {@link MapLookupExtractor}, but immutable, and also reversible without iterating the entire map.
 *
 * Forward lookup, {@link ImmutableLookupExtractor#apply(String)}, is implemented using an {@link Object2IntOpenHashMap}
 * with load factor {@link #LOAD_FACTOR}. The value of the map is an index into {@link #keys} and {@link #values}.
 *
 * Reverse lookup, {@link ImmutableLookupExtractor#unapply(String)}, is implemented using binary search through
 * {@link #values}. The {@link #keys} and {@link #values} lists are both sorted by value using {@link #VALUE_COMPARATOR}.
 *
 * Relative to {@link MapLookupExtractor} backed by Java {@link HashMap}, this map has been observed to have
 * somewhat lower footprint, same performance for {@link ImmutableLookupExtractor#apply(String)}, and significantly
 * faster for {@link ImmutableLookupExtractor#unapply(String)}. It should be used whenever the map does not need to
 * be mutated.
 */
public final class ImmutableLookupMap extends ForwardingMap<String, String>
{
  /**
   * Default value for {@link #keyToEntry}.
   */
  private static final int NOT_FOUND = -1;

  /**
   * Load factor lower than default {@link it.unimi.dsi.fastutil.Hash#DEFAULT_LOAD_FACTOR} to speed up performance
   * a bit for {@link ImmutableLookupExtractor#apply(String)}.
   */
  private static final float LOAD_FACTOR = 0.6f;

  private static final Comparator<Pair<String, String>> VALUE_COMPARATOR =
      Comparator.comparing(pair -> pair.rhs, Comparators.naturalNullsFirst());
  /**
   * Key to index in {@link #keys} and {@link #values}.
   */
  private final Object2IntMap<String> keyToEntry;
  // Store keys and values as separate lists to avoid storing Entry objects (saves some memory).
  private final List<String> keys;
  private final List<String> values;
  private final Map<String, String> asMap;

  private ImmutableLookupMap(
      final Object2IntMap<String> keyToEntry,
      final List<String> keys,
      final List<String> values
  )
  {
    this.keyToEntry = Preconditions.checkNotNull(keyToEntry, "keyToEntry");
    this.keys = Preconditions.checkNotNull(keys, "keys");
    this.values = Preconditions.checkNotNull(values, "values");
    this.asMap = Collections.unmodifiableMap(Maps.transformValues(keyToEntry, values::get));
  }

  /**
   * Create an {@link ImmutableLookupMap} from a particular map. The provided map will not be stored in the
   * returned {@link ImmutableLookupMap}.
   */
  public static ImmutableLookupMap fromMap(final Map<String, String> srcMap)
  {
    final List<Pair<String, String>> entriesList = new ArrayList<>(srcMap.size());
    for (final Entry<String, String> entry : srcMap.entrySet()) {
      entriesList.add(Pair.of(entry.getKey(), entry.getValue()));
    }
    entriesList.sort(VALUE_COMPARATOR);

    final List<String> keys = new ArrayList<>(entriesList.size());
    final List<String> values = new ArrayList<>(entriesList.size());

    for (final Pair<String, String> entry : entriesList) {
      keys.add(entry.lhs);
      values.add(entry.rhs);
    }

    entriesList.clear(); // save memory

    // Populate keyToEntries map.
    final Object2IntMap<String> keyToEntry = new Object2IntOpenHashMap<>(keys.size(), LOAD_FACTOR);
    keyToEntry.defaultReturnValue(NOT_FOUND);
    for (int i = 0; i < keys.size(); i++) {
      keyToEntry.put(keys.get(i), i);
    }

    return new ImmutableLookupMap(keyToEntry, keys, values);
  }

  @Override
  protected Map<String, String> delegate()
  {
    return asMap;
  }

  public LookupExtractor asLookupExtractor(final boolean isOneToOne, final Supplier<byte[]> cacheKey)
  {
    return new ImmutableLookupExtractor(isOneToOne, cacheKey);
  }

  public class ImmutableLookupExtractor extends LookupExtractor
  {
    private final boolean isOneToOne;
    private final Supplier<byte[]> cacheKeySupplier;

    private ImmutableLookupExtractor(final boolean isOneToOne, final Supplier<byte[]> cacheKeySupplier)
    {
      this.isOneToOne = isOneToOne;
      this.cacheKeySupplier = Preconditions.checkNotNull(cacheKeySupplier, "cacheKeySupplier");
    }

    @Nullable
    @Override
    public String apply(@Nullable String key)
    {
      String keyEquivalent = NullHandling.nullToEmptyIfNeeded(key);
      if (keyEquivalent == null) {
        // keyEquivalent is null only for SQL-compatible null mode
        // Otherwise, null will be replaced with empty string in nullToEmptyIfNeeded above.
        return null;
      }

      final int entryId = keyToEntry.getInt(keyEquivalent);
      if (entryId == NOT_FOUND) {
        return null;
      } else {
        return NullHandling.emptyToNullIfNeeded(values.get(entryId));
      }
    }

    @Override
    protected List<String> unapply(@Nullable String value)
    {
      final List<String> unapplied = unapplyInternal(value, !NullHandling.sqlCompatible());

      if (NullHandling.replaceWithDefault() && value == null) {
        // Also check empty string, if the value was null.
        final List<String> emptyStringUnapplied = unapplyInternal("", true);
        if (!emptyStringUnapplied.isEmpty()) {
          final List<String> combined = new ArrayList<>(unapplied.size() + emptyStringUnapplied.size());
          combined.addAll(unapplied);
          combined.addAll(emptyStringUnapplied);
          return combined;
        }
      }

      return unapplied;
    }

    @Override
    public boolean supportsAsMap()
    {
      return true;
    }

    @Override
    public Map<String, String> asMap()
    {
      return ImmutableLookupMap.this.asMap;
    }

    @Override
    public boolean isOneToOne()
    {
      return isOneToOne;
    }

    @Override
    public long estimateHeapFootprint()
    {
      return MapLookupExtractor.estimateHeapFootprint(asMap().entrySet());
    }

    @Override
    public byte[] getCacheKey()
    {
      return cacheKeySupplier.get();
    }

    /**
     * Unapply a single value, without null-handling-based transformation. Just look for entries in the map that
     * have the provided value.
     *
     * @param value           value to search for
     * @param includeNullKeys whether to include null keys in the returned list
     */
    private List<String> unapplyInternal(@Nullable final String value, boolean includeNullKeys)
    {
      final int index = Collections.binarySearch(values, value, Comparators.naturalNullsFirst());
      if (index < 0) {
        return Collections.emptyList();
      }

      // Found the value at "index". The value may appear multiple times, and "index" isn't guaranteed to be any
      // particular appearance. So we need to expand the search in both directions to find all the matching entries.
      int minIndex = index /* min is inclusive */, maxIndex = index + 1 /* max is exclusive */;

      while (minIndex > 0 && Objects.equals(values.get(minIndex - 1), value)) {
        minIndex--;
      }

      while (maxIndex < values.size() && Objects.equals(values.get(maxIndex), value)) {
        maxIndex++;
      }

      if (minIndex + 1 == maxIndex) {
        // Only found one entry for this value.
        final String key = keys.get(index);
        if (key == null && !includeNullKeys) {
          return Collections.emptyList();
        } else {
          return Collections.singletonList(keys.get(index));
        }
      } else {
        // Found multiple entries.
        final List<String> retVal = new ArrayList<>(maxIndex - minIndex);
        for (int i = minIndex; i < maxIndex; i++) {
          final String key = keys.get(i);
          if (key != null || includeNullKeys) {
            retVal.add(key);
          }
        }
        return retVal;
      }
    }
  }
}
