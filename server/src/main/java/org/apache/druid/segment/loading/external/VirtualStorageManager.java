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

import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.io.FilePopulator;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.utils.Throwables;

import javax.annotation.Nullable;
import java.util.function.LongSupplier;

/**
 * Manages a cache of downloaded files across multiple {@link StorageLocation}.
 * Provides a simple API for checking if files exist and reserving space for new downloads.
 */
public interface VirtualStorageManager
{
  /**
   * Check if a cached file exists and acquire a hold on it to prevent eviction.
   *
   * @param identifier Unique identifier for the cached file
   * @return CachedFile handle if the file exists, or null if not found
   */
  @Nullable
  CachedFile get(String identifier);

  /**
   * Reserve space for a new file, populate it via the provided lambda, and return
   * a handle to access it. Population is done in the calling thread.
   *
   * <p>If an entry with the same identifier already exists, returns the existing entry
   * without calling the populator.
   *
   * @param identifier Unique identifier for the cached file
   * @param sizeSupplier  Supplier that provides the number of bytes to reserve
   * @param populator  Lambda that will be called with a File to populate the content
   * @return CachedFile handle to access the populated file
   */
  CachedFile reserveAndPopulate(
      String identifier,
      LongSupplier sizeSupplier,
      FilePopulator populator
  );

  /**
   * Reserve space for a new file, populate it via the provided lambda, and return
   * a handle to access it. Population is done asynchronously.
   *
   * <p>Close the {@link AsyncResource} that is returned from
   * this method when done. Do not close the inner {@link CachedFile}, as this will lead
   * to a double-close.
   *
   * <p>If an entry with the same identifier already exists, returns the existing entry
   * without calling the populator.
   *
   * @param identifier   Unique identifier for the cached file
   * @param sizeSupplier Supplier that provides the number of bytes to reserve
   * @param populator    Lambda that will be called with a File to populate the content
   *
   * @return handle to access the populated file
   */
  AsyncResource<CachedFile> reserveAndPopulateAsync(
      String identifier,
      LongSupplier sizeSupplier,
      FilePopulator populator
  );

  /**
   * Whether the given throwable indicates that a reservation failed because there was not enough space in the
   * storage locations to hold a file.
   */
  static boolean isInsufficientStorage(final Throwable e)
  {
    final DruidException druidException = Throwables.getCauseOfType(e, DruidException.class);
    return druidException != null
           && druidException.getCategory() == DruidException.Category.CAPACITY_EXCEEDED;
  }
}
