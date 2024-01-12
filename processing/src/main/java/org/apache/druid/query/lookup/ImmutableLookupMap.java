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
import org.apache.druid.java.util.common.ISE;
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
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

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
public class ImmutableLookupMap extends ForwardingMap<String, String>
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

  private static final Comparator<Map.Entry<String, String>> VALUE_COMPARATOR =
      Map.Entry.comparingByValue(Comparators.naturalNullsFirst());

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

  @Override
  protected Map<String, String> delegate()
  {
    return asMap;
  }

  public LookupExtractor asLookupExtractor(final boolean isOneToOne, final Supplier<byte[]> cacheKey)
  {
    return new ImmutableLookupExtractor(isOneToOne, cacheKey);
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
    return super.equals(o);
  }

  @Override
  public int hashCode()
  {
    return super.hashCode();
  }

  public class ImmutableLookupExtractor extends LookupExtractor
  {
    private final boolean isOneToOne;
    private final Supplier<byte[]> cacheKeySupplier;

    public ImmutableLookupExtractor(final boolean isOneToOne, final Supplier<byte[]> cacheKeySupplier)
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
        // keyEquivalent is null only for SQL-cmpatible null mode
        // Otherwise, null will be replaced with empty string in nullToEmptyIfNeeded above.
        return null;
      }

      final int entryId = keyToEntry.getInt(key);
      if (entryId == NOT_FOUND) {
        return null;
      } else {
        return NullHandling.emptyToNullIfNeeded(values.get(entryId));
      }
    }

    @Override
    protected List<String> unapply(@Nullable String value)
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

      if (minIndex == maxIndex) {
        // Only found one entry for this value.
        return Collections.singletonList(keys.get(index));
      } else {
        // Found multiple entries.
        final List<String> retVal = new ArrayList<>(maxIndex - minIndex);
        for (int i = minIndex; i < maxIndex; i++) {
          retVal.add(keys.get(i));
        }
        return retVal;
      }
    }

    @Override
    public boolean canIterate()
    {
      return true;
    }

    @Override
    public boolean canGetKeySet()
    {
      return true;
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable()
    {
      return () -> IntStream.range(0, keys.size())
                            .<Map.Entry<String, String>>mapToObj(i -> new Entry(keys.get(i), values.get(i)))
                            .iterator();
    }

    @Override
    public Set<String> keySet()
    {
      return keyToEntry.keySet();
    }

    @Override
    public boolean isOneToOne()
    {
      return isOneToOne;
    }

    @Override
    public long estimateHeapFootprint()
    {
      return MapLookupExtractor.estimateHeapFootprint(iterable());
    }

    @Override
    public byte[] getCacheKey()
    {
      return cacheKeySupplier.get();
    }

    public Map<String, String> asMap()
    {
      return ImmutableLookupMap.this.asMap;
    }
  }

  /**
   * Builder. Once done populating, call {@link #build()} to get a {@link LookupExtractor}.
   */
  public static class Builder
  {
    private Map<String, String> entries;
    private boolean built = false;

    /**
     * Create a builder.
     */
    public Builder()
    {
      this.entries = new HashMap<>();
    }

    /**
     * Create a builder starting from a particular map. The provided map will be modified if calls are made to
     * {@link #put(String, String)} or {@link #putAll(Map)}.
     */
    public Builder(final Map<String, String> map)
    {
      this.entries = map;
    }

    public Builder put(final String key, final String value)
    {
      entries.put(key, value);
      return this;
    }

    public Builder putAll(final Map<String, String> map)
    {
      entries.putAll(map);
      return this;
    }

    /**
     * Build the extractor. The returned extractor will not share the map that was passed to the constructor
     * {@link #Builder(Map)}, if any.
     */
    public ImmutableLookupMap build()
    {
      if (built) {
        throw new ISE("Already built");
      } else {
        built = true;
      }

      final List<Map.Entry<String, String>> entriesList = new ArrayList<>(entries.size());
      entriesList.addAll(entries.entrySet());
      entriesList.sort(VALUE_COMPARATOR);

      entries = null; // save memory

      final List<String> keys = new ArrayList<>();
      final List<String> values = new ArrayList<>();

      for (final Map.Entry<String, String> entry : entriesList) {
        keys.add(entry.getKey());
        values.add(entry.getValue());
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
  }

  /**
   * Implementation of {@link Map.Entry} for {@link ImmutableLookupExtractor#iterable()}.
   */
  private static final class Entry implements Map.Entry<String, String>
  {
    private final String key;
    private final String value;

    public Entry(String key, String value)
    {
      this.key = key;
      this.value = value;
    }

    @Override
    public String getKey()
    {
      return key;
    }

    @Override
    public String getValue()
    {
      return value;
    }

    @Override
    public String setValue(String value)
    {
      throw new UnsupportedOperationException();
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
      Entry entry = (Entry) o;
      return Objects.equals(key, entry.key) && Objects.equals(value, entry.value);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(key, value);
    }

    @Override
    public String toString()
    {
      return key + "=" + value;
    }
  }
}
