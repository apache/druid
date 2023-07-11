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

package org.apache.druid.query.lookup.namespace;

import org.apache.druid.server.lookup.namespace.cache.CacheHandler;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;

import javax.annotation.Nullable;

/**
 *
 */
public interface CacheGenerator<T extends ExtractionNamespace>
{
  /**
   * If the lookup source, encapsulated by this {@code CacheGenerator}, has data newer than identified
   * by the given {@code lastVersion} (which is null at the first run of this method, or the version from the previous
   * run), this method populates a {@link CacheHandler} and returns the version string identifying the current version
   * of lookup source, If the lookup source is up-to-date, this methods returns null.
   *
   * @param namespace The ExtractionNamespace for which to generate cache.
   * @param id An object uniquely corresponding to the {@link CacheScheduler.Entry}, for which this generateCache()
   *           method is called. Also it has the same toString() representation, that is useful for logging
   * @param lastVersion The version which was last cached
   * @param cache a cache to populate
   * @return the new version, or null if the last version is up-to-date.
   */
  @Nullable
  String generateCache(
      T namespace,
      CacheScheduler.EntryImpl<T> id,
      String lastVersion,
      CacheHandler cache
  ) throws Exception;
}
