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
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.util.Providers;
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
  /**
   * The injected {@code druid.segmentCache} config. Per-task caches are derived from it (see {@link #manufacturate})
   * so operator-tuned virtual-storage settings.
   */
  private final SegmentLoaderConfig segmentLoaderConfig;
  /**
   * Resolved lazily (only on the {@code virtualStorage=true} path of {@link #manufacturate}) so that injecting this
   * factory into a component that only builds non-virtual caches (e.g. {@code DruidInputSource}) does not force the
   * process-wide {@link EphemeralStorageLoading} pool to be created on processes that never do on-demand loading
   * (Overlord, MiddleManager).
   */
  private final Provider<StorageLoadingThreadPool> loadingThreadPoolProvider;

  @Inject
  public SegmentCacheManagerFactory(
      IndexIO indexIO,
      @Json ObjectMapper mapper,
      SegmentLoaderConfig segmentLoaderConfig,
      @EphemeralStorageLoading Provider<StorageLoadingThreadPool> loadingThreadPoolProvider
  )
  {
    this.indexIO = indexIO;
    this.jsonMapper = mapper;
    this.segmentLoaderConfig = segmentLoaderConfig;
    this.loadingThreadPoolProvider = loadingThreadPoolProvider;
  }

  /**
   * Creates a factory whose {@code virtualStorage=true} managers get their own private loading pool of default size,
   * instead of the process-wide, lifecycle-managed {@link EphemeralStorageLoading} pool that Guice injects. Intended
   * for tests and manual construction where that shared pool is not available via injection; the created pool is not
   * lifecycle-managed, which is fine for short-lived JVMs.
   */
  public static SegmentCacheManagerFactory createWithOwnedPool(IndexIO indexIO, ObjectMapper mapper)
  {
    final SegmentLoaderConfig defaults = new SegmentLoaderConfig();
    return new SegmentCacheManagerFactory(
        indexIO,
        mapper,
        defaults,
        Providers.of(StorageLoadingThreadPool.createFromConfig(defaults.withVirtualStorage(true)))
    );
  }

  /**
   * Creates a new {@link SegmentCacheManager} backed by a new storage location in {@code storageDir}. When
   * {@code virtualStorage} is true, the returned manager uses this factory's process-wide
   * {@link EphemeralStorageLoading} loading pool (shared across all per-task caches and stopped by the lifecycle)
   * rather than creating its own.
   *
   * @param storageDir              storage location
   * @param maxSize                 size limit, or null for no limit
   * @param virtualStorage          whether to configure the cache manager in ephemeral virtual storage mode. In this
   *                                mode, loading is triggered by
   *                                {@link SegmentCacheManager#acquireSegment(DataSegment, AcquireMode)}, and segment
   *                                files are deleted as soon as all holds are closed.
   * @param partialDownloadsEnabled when true (and {@code virtualStorage} is true), partial-eligible segments are read
   *                                via on-demand column downloads rather than downloaded in full up front. Has no
   *                                effect when {@code virtualStorage} is false.
   */
  public SegmentCacheManager manufacturate(
      File storageDir,
      Long maxSize,
      boolean virtualStorage,
      boolean partialDownloadsEnabled
  )
  {
    final StorageLocationConfig locationConfig = new StorageLocationConfig(
        storageDir,
        maxSize != null ? maxSize : Long.MAX_VALUE,
        null
    );
    final SegmentLoaderConfig loaderConfig =
        segmentLoaderConfig.withVirtualStorage(virtualStorage)
            .setLocations(Collections.singletonList(locationConfig))
            .setVirtualStorageIsEphemeral(virtualStorage)
            .setVirtualStoragePartialDownloadsEnabled(partialDownloadsEnabled);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    return new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        virtualStorage ? loadingThreadPoolProvider.get() : StorageLoadingThreadPool.none(),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        indexIO,
        jsonMapper
    );
  }
}
