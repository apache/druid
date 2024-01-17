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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.lookup.ImmutableLookupMap;
import org.apache.druid.query.lookup.LookupExtractor;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Lookup extractor backed by any kind of map.
 *
 * When the map is immutable, use {@link ImmutableLookupMap} instead.
 */
@JsonTypeName("map")
public class MapLookupExtractor extends LookupExtractor
{
  // Each String object has ~40 bytes of overhead, and x 2 for key and value strings
  private static final long HEAP_ENTRY_OVERHEAD = 80;

  private final Map<String, String> map;

  private final boolean isOneToOne;

  @JsonCreator
  public MapLookupExtractor(
      @JsonProperty("map") Map<String, String> map,
      @JsonProperty("isOneToOne") boolean isOneToOne
  )
  {
    this.map = Preconditions.checkNotNull(map, "map");
    this.isOneToOne = isOneToOne;
  }

  /**
   * Estimate the heap footprint of a Map.
   *
   * Important note: the implementation accepts any kind of map entries, but estimates zero footprint for keys and
   * values of types other than String.
   */
  public static <K, V> long estimateHeapFootprint(final Iterable<Map.Entry<K, V>> entries)
  {
    int numEntries = 0;
    long numChars = 0;

    for (Map.Entry<K, V> sEntry : entries) {
      final K key = sEntry.getKey();
      final V value = sEntry.getValue();

      numEntries++;

      if (key instanceof String) {
        numChars += ((String) key).length();
      }

      if (value instanceof String) {
        numChars += ((String) value).length();
      }
    }

    return HEAP_ENTRY_OVERHEAD * numEntries + numChars * Character.BYTES;
  }

  @JsonProperty
  public Map<String, String> getMap()
  {
    return ImmutableMap.copyOf(map);
  }

  @Nullable
  @Override
  public String apply(@Nullable String key)
  {
    String keyEquivalent = NullHandling.nullToEmptyIfNeeded(key);
    if (keyEquivalent == null) {
      // keyEquivalent is null only for SQL Compatible Null Behavior
      // otherwise null will be replaced with empty string in nullToEmptyIfNeeded above.
      return null;
    }
    return NullHandling.emptyToNullIfNeeded(map.get(keyEquivalent));
  }

  @Override
  public List<String> unapply(@Nullable final String value)
  {
    // Not needed, since we override unapplyAll.
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Iterator<String> unapplyAll(Set<String> values)
  {
    return Iterators.transform(
        Iterators.filter(
            map.entrySet().iterator(),
            entry -> {
              if (NullHandling.sqlCompatible() && entry.getKey() == null) {
                // Null keys are omitted in SQL-compatible mode.
                return false;
              } else {
                return values.contains(NullHandling.emptyToNullIfNeeded(entry.getValue()));
              }
            }
        ),
        Map.Entry::getKey
    );
  }

  @Override
  @JsonProperty("isOneToOne")
  public boolean isOneToOne()
  {
    return isOneToOne;
  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        final String key = entry.getKey();
        final String val = entry.getValue();
        if (!Strings.isNullOrEmpty(key)) {
          outputStream.write(StringUtils.toUtf8(key));
        }
        outputStream.write((byte) 0xFF);
        if (!Strings.isNullOrEmpty(val)) {
          outputStream.write(StringUtils.toUtf8(val));
        }
        outputStream.write((byte) 0xFF);
      }
      return outputStream.toByteArray();
    }
    catch (IOException ex) {
      // If ByteArrayOutputStream.write has problems, that is a very bad thing
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean supportsAsMap()
  {
    return true;
  }

  @Override
  public Map<String, String> asMap()
  {
    return Collections.unmodifiableMap(map);
  }

  @Override
  public long estimateHeapFootprint()
  {
    return estimateHeapFootprint(map.entrySet());
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

    MapLookupExtractor that = (MapLookupExtractor) o;

    return isOneToOne == that.isOneToOne && map.equals(that.map);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(isOneToOne, map);
  }
}
