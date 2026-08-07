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

package org.apache.druid.segment.loading;

/**
 * Opt-in extension of {@link CacheEntry} that supports in-place adjustment of the reservation size after the entry is
 * already registered with a {@link StorageLocation}. Used by entry types whose final size is not known at registration
 * time and is determined later (e.g., a partial-segment metadata entry that reserves a pessimistic estimate up front
 * and shrinks to the actual on-disk header size after the header has been downloaded).
 * <p>
 * Implementations must mutate the field backing {@link #getSize()} so subsequent calls see the new size. Only
 * {@link StorageLocation#adjustReservation(CacheEntryIdentifier, long)} should call {@link #resizeReservation(long)};
 * direct calls bypass the location's bookkeeping atomics and will leave reservation accounting incorrect.
 */
public interface ResizableCacheEntry extends CacheEntry
{
  /**
   * Mutate this entry's size to {@code newSize}. If you are not calling this method from within
   * {@link StorageLocation}, you should be calling {@link StorageLocation#adjustReservation} instead.
   */
  void resizeReservation(long newSize);
}
