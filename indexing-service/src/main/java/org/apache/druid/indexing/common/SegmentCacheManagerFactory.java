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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EphemeralStorageLoading;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.LeastBytesUsedStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.StorageLoadingThreadPool;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class SegmentCacheManagerFactory
{
  private final IndexIO indexIO;
  private final ObjectMapper jsonMapper;
  private final StorageLoadingThreadPool loadingThreadPool;

  @Inject
  public SegmentCacheManagerFactory(
      IndexIO indexIO,
      @Json ObjectMapper mapper,
      @EphemeralStorageLoading StorageLoadingThreadPool loadingThreadPool
  )
  {
    this.indexIO = indexIO;
    this.jsonMapper = mapper;
    this.loadingThreadPool = loadingThreadPool;
  }

  /**
   * Convenience constructor for tests and manual construction. Builds a private, always-virtual loading pool of default
   * size rather than sharing the process-wide {@link EphemeralStorageLoading} pool. The pool is not lifecycle-managed;
   * this is fine for short-lived test JVMs.
   */
  @VisibleForTesting
  public SegmentCacheManagerFactory(
      IndexIO indexIO,
      ObjectMapper mapper
  )
  {
    this(indexIO, mapper, StorageLoadingThreadPool.createForEphemeral(new SegmentLoaderConfig()));
  }

  /**
   * Creates a new {@link SegmentCacheManager} backed by a new storage location in {@code storageDir}. When
   * {@code virtualStorage} is true, the returned manager uses this factory's process-wide
   * {@link EphemeralStorageLoading} loading pool (shared across all per-task caches and stopped by the lifecycle)
   * rather than creating its own.
   *
   * @param storageDir     storage location
   * @param maxSize        size limit, or null for no limit
   * @param virtualStorage whether to configure the cache manager in ephemeral virtual storage mode. In this mode,
   *                       loading is triggered by {@link SegmentCacheManager#acquireSegment(DataSegment, AcquireMode)},
   *                       and segment files are deleted as soon as all holds are closed.
   */
  public SegmentCacheManager manufacturate(File storageDir, Long maxSize, boolean virtualStorage)
  {
    final StorageLocationConfig locationConfig = new StorageLocationConfig(
        storageDir,
        maxSize != null ? maxSize : Long.MAX_VALUE,
        null
    );
    final SegmentLoaderConfig loaderConfig =
        new SegmentLoaderConfig()
            .setLocations(Collections.singletonList(locationConfig))
            .setVirtualStorage(virtualStorage)
            .setVirtualStorageIsEphemeral(virtualStorage);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    return new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        virtualStorage ? loadingThreadPool : StorageLoadingThreadPool.none(),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        indexIO,
        jsonMapper
    );
  }
}
