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

package io.druid.query.lookup;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.query.extraction.MapLookupExtractor;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "map", value = MapLookupExtractor.class)
})
public abstract class LookupExtractor
{
  /**
   * Apply a particular lookup methodology to the input string
   *
   * @param key The value to apply the lookup to. May not be null
   *
   * @return The lookup, or null key cannot have the lookup applied to it and should be treated as missing.
   */
  @Nullable
  public abstract String apply(@NotNull String key);

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
   * Provide the reverse mapping from a given value to a list of keys
   *
   * @param value the value to apply the reverse lookup
   *              Null and empty are considered to be the same value = nullToEmpty(value)
   *
   * @return the list of keys that maps to value or empty list.
   * Note that for the case of a none existing value in the lookup we have two cases either return an empty list OR list with null element.
   * returning an empty list implies that user want to ignore such a lookup value.
   * In the other hand returning a list with the null element implies user want to map the none existing value to the key null.
   */

  public abstract List<String> unapply(String value);

  /**
   * @param values Iterable of values for which will perform reverse lookup
   *
   * @return Returns {@link Map} whose keys are the contents of {@code values} and whose values are computed on demand using the reverse lookup function {@link #unapply(String)}
   * or empty map if {@code values} is `null`
   * User can override this method if there is a better way to perform bulk reverse lookup
   */

  public Map<String, List<String>> unapplyAll(Iterable<String> values)
  {
    if (values == null) {
      return Collections.emptyMap();
    }
    Map<String, List<String>> map = new HashMap<>();
    for (String value : values) {
      map.put(value, unapply(value));
    }
    return map;
  }

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
}
