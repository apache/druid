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

package io.druid.query.lookup.namespace;

import io.druid.server.lookup.namespace.cache.CacheScheduler;

import javax.annotation.Nullable;

/**
 *
 */
public interface CacheGenerator<T extends ExtractionNamespace>
{
  /**
   * If the lookup source, encapsulated by this {@code CacheGenerator}, has data newer than identified
   * by the given {@code lastVersion} (which is null at the first run of this method, or the version from the previous
   * run), this method creates a new {@code CacheScheduler.VersionedCache} with {@link
   * CacheScheduler#createVersionedCache}, called on the given {@code scheduler}, with the version string identifying
   * the current version of lookup source, populates the created {@code VersionedCache} and returns it. If the lookup
   * source is up-to-date, this methods returns null.
   *
   * @param namespace The ExtractionNamespace for which to generate cache.
   * @param id An object uniquely corresponding to the {@link CacheScheduler.Entry}, for which this generateCache()
   *           method is called. Also it has the same toString() representation, that is useful for logging
   * @param lastVersion The version which was last cached
   * @param scheduler Should be used only to call {@link CacheScheduler#createVersionedCache}.
   * @return the new cache along with the new version, or null if the last version is up-to-date.
   */
  @Nullable
  CacheScheduler.VersionedCache generateCache(
      T namespace,
      CacheScheduler.EntryImpl<T> id,
      String lastVersion,
      CacheScheduler scheduler
  ) throws Exception;
}
