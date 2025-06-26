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

import java.io.File;
import java.io.IOException;

/**
 * Common interface for items stored in a {@link StorageLocation}
 */
public interface CacheEntry
{
  /**
   * Unique identifier for the cache entry
   */
  CacheEntryIdentifier getId();

  /**
   * Size in bytes of the cache entry
   */
  long getSize();

  /**
   * Materializes the cache entry into the assigned {@link StorageLocation}. If a cache entry is already mounted in the
   * location, calling this method should be a no-op. If the cache entry is mounted in a different location, this method
   * will unmount the item from the other location and mount in the new location.
   */
  void mount(File location) throws IOException, SegmentLoadingException;

  /**
   * Removes the physical artifacts of a cache entry from the location it is currently mounted
   */
  void unmount();
}
