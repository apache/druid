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
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.utils.RuntimeInfo;

import javax.validation.constraints.Min;
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
   * When true, partial-eligible V10 segments are mounted via the partial machinery and
   * {@link SegmentCacheManager#acquireSegment} with {@link AcquireMode#PARTIAL} returns a metadata-anchored segment
   * whose columns are downloaded on demand. When false (the default), {@link AcquireMode#PARTIAL} falls back to
   * {@link AcquireMode#FULL} so the entire segment is downloaded up front (matching pre-partial-download behavior).
   */
  @JsonProperty("virtualStoragePartialDownloadsEnabled")
  private boolean virtualStoragePartialDownloadsEnabled = false;

  /**
   * Largest unwanted gap, in bytes, that on-demand partial downloads will read through to coalesce two wanted internal
   * files into a single deep-storage range read. Larger values trade extra over-fetched bytes for fewer requests.
   * See {@link org.apache.druid.segment.file.PartialSegmentFileMapperV10#planDownloadRuns}.
   */
  @JsonProperty("virtualStorageCoalesceMaxGapBytes")
  @Min(
      value = 0,
      message = "druid.segmentCache.virtualStorageCoalesceMaxGapBytes must be at least 0 (it is the largest unwanted "
                + "gap, in bytes, read through to merge two adjacent on-demand column reads into a single request)"
  )
  private long virtualStorageCoalesceMaxGapBytes = PartialSegmentFileMapperV10.DEFAULT_COALESCE_MAX_GAP_BYTES;

  /**
   * Largest size, in bytes, of a single coalesced range read for on-demand partial downloads. Bounds how big one fetch
   * can grow and keeps a wide request split into several reads that can be downloaded concurrently rather than
   * collapsing into one serial read. See {@link org.apache.druid.segment.file.PartialSegmentFileMapperV10#planDownloadRuns}.
   */
  @JsonProperty("virtualStorageCoalesceMaxChunkBytes")
  @Min(
      value = 1,
      message = "druid.segmentCache.virtualStorageCoalesceMaxChunkBytes must be at least 1 (it caps the size, in "
                + "bytes, of a single coalesced range read; a small value effectively disables coalescing)"
  )
  private long virtualStorageCoalesceMaxChunkBytes = PartialSegmentFileMapperV10.DEFAULT_COALESCE_MAX_CHUNK_BYTES;

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

  /**
   * Range-coalescing thresholds for on-demand partial downloads, derived from
   * {@link #virtualStorageCoalesceMaxGapBytes} and {@link #virtualStorageCoalesceMaxChunkBytes}.
   */
  public PartialSegmentFileMapperV10.CoalesceConfig getVirtualStorageCoalesceConfig()
  {
    return new PartialSegmentFileMapperV10.CoalesceConfig(
        virtualStorageCoalesceMaxGapBytes,
        virtualStorageCoalesceMaxChunkBytes
    );
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
   * Sets {@link #virtualStorage}.
   */
  public SegmentLoaderConfig setVirtualStorage(boolean virtualStorage)
  {
    this.virtualStorage = virtualStorage;
    return this;
  }

  /**
   * Sets {@link #virtualStorageIsEphemeral}.
   */
  public SegmentLoaderConfig setVirtualStorageIsEphemeral(boolean virtualStorageFabricEphemeral)
  {
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
               .map(locationConfig -> {
                 final StorageLocation location = new StorageLocation(
                     locationConfig.getPath(),
                     locationConfig.getMaxSize(),
                     locationConfig.getFreeSpacePercent()
                 );

                 if (isVirtualStorageEphemeral()) {
                   location.setAreWeakEntriesEphemeral(true);
                 }

                 return location;
               })
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
