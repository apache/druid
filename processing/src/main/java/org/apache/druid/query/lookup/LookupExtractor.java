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


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Iterators;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.extraction.MapLookupExtractor;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "map", value = MapLookupExtractor.class)
})
public abstract class LookupExtractor
{
  /**
   * Apply a particular lookup methodology to the input string
   *
   * @param key The value to apply the lookup to.
   *
   * @return The value for this key, or null when key is `null` in {@link NullHandling#sqlCompatible()} mode, or when
   * key cannot have the lookup applied to it and should be treated as missing.
   */
  @Nullable
  public abstract String apply(@Nullable String key);

  /**
   * @param keys set of keys to apply lookup for each element
   *
   * @return Returns {@link Map} whose keys are the contents of {@code keys} and whose values are computed on demand using lookup function {@link #unapply(String)}
   * or empty map if {@code values} is `null`
   * User can override this method if there is a better way to perform bulk lookup
   */

  public Map<String, String> applyAll(Iterable<String> keys)
  {
    if (keys == null) {
      return Collections.emptyMap();
    }
    Map<String, String> map = new HashMap<>();
    for (String key : keys) {
      map.put(key, apply(key));
    }
    return map;
  }

  /**
   * Reverse lookup from a given value. Used by the default implementation of {@link #unapplyAll(Set)}.
   * Otherwise unused. Implementations that override {@link #unapplyAll(Set)} may throw
   * {@link UnsupportedOperationException} from this method.
   *
   * @param value the value to apply the reverse lookup. {@link NullHandling#emptyToNullIfNeeded(String)} will have
   *              been applied to the value.
   *
   * @return the list of keys that maps to the provided value. In SQL-compatible null handling mode, null keys
   * are omitted.
   */
  protected abstract List<String> unapply(@Nullable String value);

  /**
   * Reverse lookup from a given set of values.
   *
   * @param values set of values to reverse lookup. {@link NullHandling#emptyToNullIfNeeded(String)} will have
   *               been applied to each value.
   *
   * @return iterator of keys that map to to the provided set of values. May contain duplicate keys. Returns null if
   * this lookup instance does not support reverse lookups. In SQL-compatible null handling mode, null keys are
   * omitted.
   */
  @Nullable
  public Iterator<String> unapplyAll(Set<String> values)
  {
    return Iterators.concat(Iterators.transform(values.iterator(), value -> unapply(value).iterator()));
  }

  /**
   * Returns whether this lookup extractor's {@link #asMap()} will return a valid map.
   */
  public abstract boolean supportsAsMap();

  /**
   * Returns a Map view of this lookup extractor. The map may change along with the underlying lookup data.
   *
   * @throws UnsupportedOperationException if {@link #supportsAsMap()} returns false.
   */
  public abstract Map<String, String> asMap();

  /**
   * Create a cache key for use in results caching
   *
   * @return A byte array that can be used to uniquely identify if results of a prior lookup can use the cached values
   */
  public abstract byte[] getCacheKey();

  // make this abstract again once @drcrallen fix the metmax lookup implementation.
  public boolean isOneToOne()
  {
    return false;
  }

  /**
   * Estimated heap footprint of this object. Not guaranteed to be accurate. For example, some implementations return
   * zero even though they do use on-heap structures. However, the most common classes, {@link MapLookupExtractor}
   * and {@link ImmutableLookupMap}, do have reasonable implementations.
   *
   * This API is provided for best-effort memory management and monitoring.
   */
  public long estimateHeapFootprint()
  {
    return 0;
  }
}
