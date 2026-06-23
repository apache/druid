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

package org.apache.druid.segment.loading.external;

import org.apache.druid.segment.loading.CacheEntry;
import org.apache.druid.segment.loading.StorageLocation;

import java.io.Closeable;
import java.io.File;
import java.util.function.Function;

/**
 * Handle to a cached file that prevents eviction while held.
 * Must be closed when no longer needed to make the file eligible for eviction.
 */
public class CachedFile implements Closeable
{
  private final StorageLocation.ReservationHold<CacheEntry> hold;
  private final DownloadableCacheEntry entry;
  private final String identifier;

  CachedFile(StorageLocation.ReservationHold<CacheEntry> hold, String identifier)
  {
    this.hold = hold;
    this.entry = (DownloadableCacheEntry) hold.getEntry();
    this.identifier = identifier;
  }

  /**
   * Get the identifier for this cached file.
   */
  public String getIdentifier()
  {
    return identifier;
  }

  /**
   * Get the File object for this cached file.
   * The file is guaranteed to exist and be populated while this handle is open.
   */
  public File getFile()
  {
    return entry.getFile();
  }

  /**
   * Extend this cached file with additional functionality. The lifecycle of the extended functionality is
   * tracked along with the lifecycle of the underlying cache entry. The provided supplier is called immediately unless
   * there is already a mapping for the class. Either way, the class is returned.
   *
   * <p>Do not call "close" on the returned object. Its lifecycle is associated with the underlying cache entry,
   * and it may be reused beyond the lifecycle of this particular CachedFile object. It will be closed automatically
   * when the underlying cache entry is unmounted.
   */
  public <T extends Closeable> T extend(Class<T> clazz, Function<CachedFile, T> supplier)
  {
    return entry.extend(clazz, () -> supplier.apply(this));
  }

  /**
   * Release the hold on this cached file, making it eligible for eviction.
   *
   * <p>Usage of the File returned from this object after the hold is closed is undefined and expected to end poorly.
   */
  @Override
  public void close()
  {
    hold.close();
  }
}
