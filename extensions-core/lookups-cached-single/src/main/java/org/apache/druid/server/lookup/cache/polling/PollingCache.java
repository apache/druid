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

import java.util.List;

public interface PollingCache<K, V>
{
  /**
   * @param key Given key to lookup its value from the cache.
   *
   * @return Returns the value associated to the {@code key} or {@code null} if no value exist.
   */
  V get(K key);

  /**
   * @param value Given value to reverse lookup its keys.
   *
   * @return Returns a {@link List} of keys associated to the given {@code value} otherwise {@link java.util.Collections.EmptyList}
   */
  List<K> getKeys(V value);

  /**
   * close and clean the resources used by the cache
   */
  void close();
}
