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

package io.druid.query.extraction;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "map", value = MapLookupExtractor.class)
})
public interface LookupExtractor
{
  /**
   * Apply a particular lookup methodology to the input string
   * @param key The value to apply the lookup to. May not be null
   * @return The lookup, or null key cannot have the lookup applied to it and should be treated as missing.
   */
  @Nullable String apply(@NotNull String key);

  /**
   * Provide the reverse mapping from a given value to a list of keys
   * @param value the value to apply the reverse lookup
   *              Null and empty are considered to be the same value = nullToEmpty(value)
   *
   * @return the list of keys that maps to value or empty list.
   * Note that for the case of a none existing value in the lookup we have to cases either return an empty list OR list with null element.
   * returning an empty list implies that user want to ignore such a lookup value.
   * In the other hand returning a list with the null element implies user want to map the none existing value to the key null.
   */
  List<String> unApply(String value);

  /**
   * Create a cache key for use in results caching
   * @return A byte array that can be used to uniquely identify if results of a prior lookup can use the cached values
   */
  @NotNull byte[] getCacheKey();
}
