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

package org.apache.druid.server.lookup;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.server.lookup.jdbc.JdbcDataFetcher;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * This class will be used to pull data from a given lookup table.
 *
 * @param <K> Keys type
 * @param <V> Values type
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "jdbcDataFetcher", value = JdbcDataFetcher.class)
})
public interface DataFetcher<K, V>
{
  /**
   * Function used to fetch all the pair of key-values from the lookup
   * One use case of this method is to populate a polling cache {@link org.apache.druid.server.lookup.cache.polling.PollingCache}
   *
   * @return Returns an {@link Iterable} of key-value pairs.
   */
  Iterable<Map.Entry<K, V>> fetchAll();

  /**
   * Function to perform a one item lookup.
   * For instance this can be used by a loading cache to fetch the value in case of the cache doesn't have it
   *
   * @param key non-null key used to lookup the value
   *
   * @return Returns null if the key doesn't have an associated value, or the value if it exists
   */
  @Nullable V fetch(K key);

  /**
   * Function used to perform a bulk lookup
   *
   * @param keys used to lookup the respective values
   *
   * @return Returns a {@link Iterable} containing the pair of existing key-value.
   */
  Iterable<Map.Entry<K, V>> fetch(Iterable<K> keys);

  /**
   * Function used to perform reverse lookup
   *
   * @param value use to fetch it's keys from the lookup table
   *
   * @return Returns a list of keys of the given {@code value} or an empty list {@link java.util.Collections.EmptyList}
   */
  List<K> reverseFetchKeys(V value);

}
