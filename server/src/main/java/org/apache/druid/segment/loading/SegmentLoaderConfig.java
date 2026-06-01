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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.druid.utils.RuntimeInfo;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 */
public class SegmentLoaderConfig
{
  @JacksonInject
  private final RuntimeInfo runtimeInfo = new RuntimeInfo();

  @JsonProperty
  private List<StorageLocationConfig> locations = Collections.emptyList();

  @JsonProperty("lazyLoadOnStart")
  private boolean lazyLoadOnStart = false;

  @JsonProperty("deleteOnRemove")
  private boolean deleteOnRemove = true;

  @JsonProperty("dropSegmentDelayMillis")
  private int dropSegmentDelayMillis = (int) TimeUnit.SECONDS.toMillis(30);

  @JsonProperty("announceIntervalMillis")
  private int announceIntervalMillis = 0; // do not background announce

  @JsonProperty("numLoadingThreads")
  private int numLoadingThreads = Math.max(1, runtimeInfo.getAvailableProcessors() / 6);

  @JsonProperty("numBootstrapThreads")
  private Integer numBootstrapThreads = null;

  @JsonProperty("numThreadsToLoadSegmentsIntoPageCacheOnDownload")
  private int numThreadsToLoadSegmentsIntoPageCacheOnDownload = 0;

  @JsonProperty("numThreadsToLoadSegmentsIntoPageCacheOnBootstrap")
  private Integer numThreadsToLoadSegmentsIntoPageCacheOnBootstrap = null;

  @JsonProperty
  private File infoDir = null;

  @JsonProperty
  private int statusQueueMaxSize = 100;

  @JsonProperty("virtualStorage")
  private boolean virtualStorage = false;

  @JsonProperty("virtualStorageLoadThreads")
  private int virtualStorageLoadThreads = Math.max(32, 4 * runtimeInfo.getAvailableProcessors());

  /**
   * When true (the default), the on-demand load executor uses one virtual thread per task with a {@link
   * java.util.concurrent.Semaphore} sized by {@link #virtualStorageLoadThreads} for backpressure. When false, falls back
   * to a fixed platform-thread pool of that size. The escape hatch exists in case virtual threads behave poorly with a
   * particular deep storage SDK or workload.
   */
  @JsonProperty("virtualStorageUseVirtualThreads")
  private boolean virtualStorageUseVirtualThreads = true;

  /**
   * When enabled, weakly-held cache entries are evicted immediately upon release of all holds, rather than
   * waiting for space pressure to trigger eviction. This setting is not intended to be configured directly by
   * administrators. Instead, it is expected to be set when appropriate via {@link #setVirtualStorage}.
   */
  @JsonProperty("virtualStorageIsEphemeral")
  private boolean virtualStorageIsEphemeral = false;

  /**
   * Up-front size reservation (in bytes) used when mounting a partial-segment metadata cache entry. The entry
   * range-reads the V10 header from deep storage at mount time, then calls
   * {@link StorageLocation#adjustReservation} to shrink to the actual on-disk size. If the actual header exceeds this
   * estimate, the mount fails with an operator-facing error directing them to raise this value. Defaults to 16 MiB,
   * which comfortably covers the metadata of typical V10 segments; outliers with many columns and/or projections may
   * need a higher value.
   */
  @JsonProperty("virtualStorageMetadataReservationEstimate")
  private long virtualStorageMetadataReservationEstimate = 16L * 1024L * 1024L;

  /**
   * When true (the default), partial-eligible V10 segments are mounted via the partial machinery and
   * {@link SegmentCacheManager#acquirePartialSegment} returns a metadata-anchored segment whose columns are downloaded
   * on demand. When false, the partial-aware acquire methods fall back to their eager counterparts so the entire
   * segment is downloaded up front (matching pre-partial-download behavior). Provided as an operator escape hatch if
   * partial downloads cause problems in a given deployment.
   */
  @JsonProperty("virtualStoragePartialDownloadsEnabled")
  private boolean virtualStoragePartialDownloadsEnabled = true;

  private long combinedMaxSize = 0;

  public List<StorageLocationConfig> getLocations()
  {
    return locations;
  }

  public boolean isLazyLoadOnStart()
  {
    return lazyLoadOnStart;
  }

  public boolean isDeleteOnRemove()
  {
    return deleteOnRemove;
  }

  public int getDropSegmentDelayMillis()
  {
    return dropSegmentDelayMillis;
  }

  public int getAnnounceIntervalMillis()
  {
    return announceIntervalMillis;
  }

  public int getNumLoadingThreads()
  {
    return numLoadingThreads;
  }

  public int getNumBootstrapThreads()
  {
    return numBootstrapThreads == null ? numLoadingThreads : numBootstrapThreads;
  }

  public int getNumThreadsToLoadSegmentsIntoPageCacheOnDownload()
  {
    return numThreadsToLoadSegmentsIntoPageCacheOnDownload;
  }

  public int getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap()
  {
    return numThreadsToLoadSegmentsIntoPageCacheOnBootstrap == null ?
           numThreadsToLoadSegmentsIntoPageCacheOnDownload :
           numThreadsToLoadSegmentsIntoPageCacheOnBootstrap;
  }

  public File getInfoDir()
  {
    return infoDir;
  }

  public int getStatusQueueMaxSize()
  {
    return statusQueueMaxSize;
  }

  public long getCombinedMaxSize()
  {
    if (combinedMaxSize == 0) {
      combinedMaxSize = getLocations().stream().mapToLong(StorageLocationConfig::getMaxSize).sum();
    }
    return combinedMaxSize;
  }

  public boolean isVirtualStorage()
  {
    return virtualStorage;
  }

  public int getVirtualStorageLoadThreads()
  {
    return virtualStorageLoadThreads;
  }

  public boolean isVirtualStorageUseVirtualThreads()
  {
    return virtualStorageUseVirtualThreads;
  }

  public boolean isVirtualStorageEphemeral()
  {
    return virtualStorageIsEphemeral;
  }

  public long getVirtualStorageMetadataReservationEstimate()
  {
    return virtualStorageMetadataReservationEstimate;
  }

  public boolean isVirtualStoragePartialDownloadsEnabled()
  {
    return virtualStorage && virtualStoragePartialDownloadsEnabled;
  }

  public SegmentLoaderConfig setLocations(List<StorageLocationConfig> locations)
  {
    this.locations = Lists.newArrayList(locations);
    return this;
  }

  public SegmentLoaderConfig setVirtualStoragePartialDownloadsEnabled(boolean enabled)
  {
    this.virtualStoragePartialDownloadsEnabled = enabled;
    return this;
  }

  /**
   * Sets {@link #virtualStorage} and {@link #virtualStorageIsEphemeral}.
   */
  public SegmentLoaderConfig setVirtualStorage(
      boolean virtualStorage,
      boolean virtualStorageFabricEphemeral
  )
  {
    this.virtualStorage = virtualStorage;
    this.virtualStorageIsEphemeral = virtualStorageFabricEphemeral;
    return this;
  }

  /**
   * Convert a list of {@link StorageLocationConfig} objects to {@link StorageLocation} objects.
   * <p>
   * Note: {@link #getLocations} is called instead of variable access because some testcases overrides this method
   */
  public List<StorageLocation> toStorageLocations()
  {
    return this.getLocations()
               .stream()
               .map(locationConfig -> new StorageLocation(locationConfig.getPath(),
                                                          locationConfig.getMaxSize(),
                                                          locationConfig.getFreeSpacePercent()))
               .collect(Collectors.toList());
  }

  @Override
  public String toString()
  {
    return "SegmentLoaderConfig{" +
           "locations=" + locations +
           ", lazyLoadOnStart=" + lazyLoadOnStart +
           ", deleteOnRemove=" + deleteOnRemove +
           ", dropSegmentDelayMillis=" + dropSegmentDelayMillis +
           ", announceIntervalMillis=" + announceIntervalMillis +
           ", numLoadingThreads=" + numLoadingThreads +
           ", numBootstrapThreads=" + numBootstrapThreads +
           ", numThreadsToLoadSegmentsIntoPageCacheOnDownload=" + numThreadsToLoadSegmentsIntoPageCacheOnDownload +
           ", numThreadsToLoadSegmentsIntoPageCacheOnBootstrap=" + numThreadsToLoadSegmentsIntoPageCacheOnBootstrap +
           ", infoDir=" + infoDir +
           ", statusQueueMaxSize=" + statusQueueMaxSize +
           ", virtualStorage=" + virtualStorage +
           ", virtualStorageLoadThreads=" + virtualStorageLoadThreads +
           ", virtualStorageUseVirtualThreads=" + virtualStorageUseVirtualThreads +
           ", virtualStorageIsEphemeral=" + virtualStorageIsEphemeral +
           ", virtualStorageMetadataReservationEstimate=" + virtualStorageMetadataReservationEstimate +
           ", virtualStoragePartialDownloadsEnabled=" + virtualStoragePartialDownloadsEnabled +
           ", combinedMaxSize=" + combinedMaxSize +
           '}';
  }
}
