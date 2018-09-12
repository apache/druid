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

package org.apache.druid.server.lookup.cache.polling;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Map;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = OnHeapPollingCache.OnHeapPollingCacheProvider.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "onHeapPolling", value = OnHeapPollingCache.OnHeapPollingCacheProvider.class),
    @JsonSubTypes.Type(name = "offHeapPolling", value = OffHeapPollingCache.OffHeapPollingCacheProvider.class)
})
public interface PollingCacheFactory<K, V>
{
  /**
   * @param entries of keys and values used to populate the cache
   *
   * @return Returns a new {@link PollingCache} containing all the entries of {@code map}
   */

  PollingCache<K, V> makeOf(Iterable<Map.Entry<K, V>> entries);
}
