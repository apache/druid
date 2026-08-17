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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.utils.RuntimeInfo;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Configuration for a local segment cache, bound from {@code druid.segmentCache}.
 */
public class SegmentLoaderConfig
{
  private static final boolean DEFAULT_LAZY_LOAD_ON_START = false;
  private static final boolean DEFAULT_DELETE_ON_REMOVE = true;
  private static final int DEFAULT_DROP_SEGMENT_DELAY_MILLIS = (int) TimeUnit.SECONDS.toMillis(30);
  private static final int DEFAULT_ANNOUNCE_INTERVAL_MILLIS = 0; // do not background announce
  private static final int DEFAULT_NUM_THREADS_TO_LOAD_INTO_PAGE_CACHE_ON_DOWNLOAD = 0;
  private static final int DEFAULT_STATUS_QUEUE_MAX_SIZE = 100;
  private static final boolean DEFAULT_VIRTUAL_STORAGE = false;
  private static final boolean DEFAULT_VIRTUAL_STORAGE_USE_VIRTUAL_THREADS = true;
  private static final boolean DEFAULT_VIRTUAL_STORAGE_IS_EPHEMERAL = false;
  private static final long DEFAULT_VIRTUAL_STORAGE_METADATA_RESERVATION_ESTIMATE = 16L * 1024L * 1024L;
  private static final boolean DEFAULT_VIRTUAL_STORAGE_PARTIAL_DOWNLOADS_ENABLED = false;

  private final RuntimeInfo runtimeInfo;

  @JsonProperty("locations")
  private final List<StorageLocationConfig> locations;
  @JsonProperty("lazyLoadOnStart")
  private final boolean lazyLoadOnStart;
  @JsonProperty("deleteOnRemove")
  private final boolean deleteOnRemove;
  @JsonProperty("dropSegmentDelayMillis")
  private final int dropSegmentDelayMillis;
  @JsonProperty("announceIntervalMillis")
  private final int announceIntervalMillis;
  @JsonProperty("numLoadingThreads")
  private final int numLoadingThreads;
  /**
   * Nullable so {@link #getNumBootstrapThreads()} can fall back to {@link #numLoadingThreads} when unset.
   */
  @JsonProperty("numBootstrapThreads")
  @Nullable
  private final Integer numBootstrapThreads;
  @JsonProperty("numThreadsToLoadSegmentsIntoPageCacheOnDownload")
  private final int numThreadsToLoadSegmentsIntoPageCacheOnDownload;
  /**
   * Nullable so {@link #getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap()} can fall back to the on-download value.
   */
  @JsonProperty("numThreadsToLoadSegmentsIntoPageCacheOnBootstrap")
  @Nullable
  private final Integer numThreadsToLoadSegmentsIntoPageCacheOnBootstrap;
  @JsonProperty("infoDir")
  @Nullable
  private final File infoDir;
  @JsonProperty("statusQueueMaxSize")
  private final int statusQueueMaxSize;
  @JsonProperty("virtualStorage")
  private final boolean virtualStorage;
  @JsonProperty("virtualStorageLoadThreads")
  private final int virtualStorageLoadThreads;

  /**
   * When true (the default), the on-demand load executor uses one virtual thread per task with a {@link
   * java.util.concurrent.Semaphore} sized by {@link #virtualStorageLoadThreads} for backpressure. When false, falls back
   * to a fixed platform-thread pool of that size. The escape hatch exists in case virtual threads behave poorly with a
   * particular deep storage SDK or workload.
   */
  @JsonProperty("virtualStorageUseVirtualThreads")
  private final boolean virtualStorageUseVirtualThreads;

  /**
   * When enabled, weakly-held cache entries are evicted immediately upon release of all holds, rather than
   * waiting for space pressure to trigger eviction. This setting is not intended to be configured directly by
   * administrators. Instead, it is expected to be set when appropriate via {@link Builder#virtualStorageIsEphemeral}.
   */
  @JsonProperty("virtualStorageIsEphemeral")
  private final boolean virtualStorageIsEphemeral;

  /**
   * Up-front size reservation (in bytes) used when mounting a partial-segment metadata cache entry. The entry
   * range-reads the V10 header from deep storage at mount time, then calls
   * {@link StorageLocation#adjustReservation} to shrink to the actual on-disk size. If the actual header exceeds this
   * estimate, the mount fails with an operator-facing error directing them to raise this value. Defaults to 16 MiB,
   * which comfortably covers the metadata of typical V10 segments; outliers with many columns and/or projections may
   * need a higher value.
   */
  @JsonProperty("virtualStorageMetadataReservationEstimate")
  private final long virtualStorageMetadataReservationEstimate;

  /**
   * When true, partial-eligible V10 segments are mounted via the partial machinery and
   * {@link SegmentCacheManager#acquireSegment} with {@link AcquireMode#PARTIAL} returns a metadata-anchored segment
   * whose columns are downloaded on demand. When false (the default), {@link AcquireMode#PARTIAL} falls back to
   * {@link AcquireMode#FULL} so the entire segment is downloaded up front (matching pre-partial-download behavior).
   */
  @JsonProperty("virtualStoragePartialDownloadsEnabled")
  private final boolean virtualStoragePartialDownloadsEnabled;

  /**
   * Maximum number of unrequested bytes a partial-download range read will fetch in order to bridge two requested
   * internal files into a single deep-storage request. Bridged bytes are whole valid internal files that are kept in
   * the local cache (they aren't waste), so this trades at most this many extra bytes per bridged gap (one read can
   * bridge several gaps) for one fewer deep-storage round trip each. {@code <= 0} disables bridging (adjacent files
   * still coalesce into single reads). Defaults to 1 MiB, which is conservative relative to typical deep-storage
   * request latency vs streaming throughput.
   */
  @JsonProperty("virtualStorageCoalesceGapBytes")
  private final long virtualStorageCoalesceGapBytes;

  /**
   * Maximum size of a single range read in a query-driven partial download. Larger fetches split at internal-file
   * boundaries into multiple reads of at most this size that proceed concurrently, so throughput isn't bounded by a
   * single deep-storage connection. Only applies to the concurrent on-demand path; full-download paths stream
   * containers sequentially and are unaffected. {@code <= 0} disables splitting. Defaults to 64 MiB.
   */
  @JsonProperty("virtualStorageMaxFetchRunBytes")
  private final long virtualStorageMaxFetchRunBytes;

  @JsonCreator
  public SegmentLoaderConfig(
      @JacksonInject @Nullable RuntimeInfo runtimeInfo,
      @JsonProperty("locations") @Nullable List<StorageLocationConfig> locations,
      @JsonProperty("lazyLoadOnStart") @Nullable Boolean lazyLoadOnStart,
      @JsonProperty("deleteOnRemove") @Nullable Boolean deleteOnRemove,
      @JsonProperty("dropSegmentDelayMillis") @Nullable Integer dropSegmentDelayMillis,
      @JsonProperty("announceIntervalMillis") @Nullable Integer announceIntervalMillis,
      @JsonProperty("numLoadingThreads") @Nullable Integer numLoadingThreads,
      @JsonProperty("numBootstrapThreads") @Nullable Integer numBootstrapThreads,
      @JsonProperty("numThreadsToLoadSegmentsIntoPageCacheOnDownload") @Nullable Integer numThreadsToLoadSegmentsIntoPageCacheOnDownload,
      @JsonProperty("numThreadsToLoadSegmentsIntoPageCacheOnBootstrap") @Nullable Integer numThreadsToLoadSegmentsIntoPageCacheOnBootstrap,
      @JsonProperty("infoDir") @Nullable File infoDir,
      @JsonProperty("statusQueueMaxSize") @Nullable Integer statusQueueMaxSize,
      @JsonProperty("virtualStorage") @Nullable Boolean virtualStorage,
      @JsonProperty("virtualStorageLoadThreads") @Nullable Integer virtualStorageLoadThreads,
      @JsonProperty("virtualStorageUseVirtualThreads") @Nullable Boolean virtualStorageUseVirtualThreads,
      @JsonProperty("virtualStorageIsEphemeral") @Nullable Boolean virtualStorageIsEphemeral,
      @JsonProperty("virtualStorageMetadataReservationEstimate") @Nullable Long virtualStorageMetadataReservationEstimate,
      @JsonProperty("virtualStoragePartialDownloadsEnabled") @Nullable Boolean virtualStoragePartialDownloadsEnabled,
      @JsonProperty("virtualStorageCoalesceGapBytes") @Nullable Long virtualStorageCoalesceGapBytes,
      @JsonProperty("virtualStorageMaxFetchRunBytes") @Nullable Long virtualStorageMaxFetchRunBytes
  )
  {
    // RuntimeInfo is an injected @LazySingleton (test-overridable); fall back to a fresh instance when it is not
    // available, e.g. when deserializing outside a Guice context. Only used to size the thread-count defaults.
    final RuntimeInfo resolvedRuntimeInfo = runtimeInfo == null ? new RuntimeInfo() : runtimeInfo;
    this.runtimeInfo = resolvedRuntimeInfo;
    this.locations = locations == null ? Collections.emptyList() : List.copyOf(locations);
    this.lazyLoadOnStart = Configs.valueOrDefault(lazyLoadOnStart, DEFAULT_LAZY_LOAD_ON_START);
    this.deleteOnRemove = Configs.valueOrDefault(deleteOnRemove, DEFAULT_DELETE_ON_REMOVE);
    this.dropSegmentDelayMillis = Configs.valueOrDefault(dropSegmentDelayMillis, DEFAULT_DROP_SEGMENT_DELAY_MILLIS);
    this.announceIntervalMillis = Configs.valueOrDefault(announceIntervalMillis, DEFAULT_ANNOUNCE_INTERVAL_MILLIS);
    this.numLoadingThreads = Configs.valueOrDefault(
        numLoadingThreads,
        Math.max(1, resolvedRuntimeInfo.getAvailableProcessors() / 6)
    );
    this.numBootstrapThreads = numBootstrapThreads;
    this.numThreadsToLoadSegmentsIntoPageCacheOnDownload = Configs.valueOrDefault(
        numThreadsToLoadSegmentsIntoPageCacheOnDownload,
        DEFAULT_NUM_THREADS_TO_LOAD_INTO_PAGE_CACHE_ON_DOWNLOAD
    );
    this.numThreadsToLoadSegmentsIntoPageCacheOnBootstrap = numThreadsToLoadSegmentsIntoPageCacheOnBootstrap;
    this.infoDir = infoDir;
    this.statusQueueMaxSize = Configs.valueOrDefault(statusQueueMaxSize, DEFAULT_STATUS_QUEUE_MAX_SIZE);
    this.virtualStorage = Configs.valueOrDefault(virtualStorage, DEFAULT_VIRTUAL_STORAGE);
    this.virtualStorageLoadThreads = Configs.valueOrDefault(
        virtualStorageLoadThreads,
        Math.max(32, 4 * resolvedRuntimeInfo.getAvailableProcessors())
    );
    this.virtualStorageUseVirtualThreads = Configs.valueOrDefault(
        virtualStorageUseVirtualThreads,
        DEFAULT_VIRTUAL_STORAGE_USE_VIRTUAL_THREADS
    );
    this.virtualStorageIsEphemeral = Configs.valueOrDefault(
        virtualStorageIsEphemeral,
        DEFAULT_VIRTUAL_STORAGE_IS_EPHEMERAL
    );
    this.virtualStorageMetadataReservationEstimate = Configs.valueOrDefault(
        virtualStorageMetadataReservationEstimate,
        DEFAULT_VIRTUAL_STORAGE_METADATA_RESERVATION_ESTIMATE
    );
    this.virtualStoragePartialDownloadsEnabled = Configs.valueOrDefault(
        virtualStoragePartialDownloadsEnabled,
        DEFAULT_VIRTUAL_STORAGE_PARTIAL_DOWNLOADS_ENABLED
    );
    this.virtualStorageCoalesceGapBytes = Configs.valueOrDefault(
        virtualStorageCoalesceGapBytes,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES
    );
    this.virtualStorageMaxFetchRunBytes = Configs.valueOrDefault(
        virtualStorageMaxFetchRunBytes,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
  }

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

  @Nullable
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
    return getLocations().stream().mapToLong(StorageLocationConfig::getMaxSize).sum();
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

  public long getVirtualStorageCoalesceGapBytes()
  {
    return virtualStorageCoalesceGapBytes;
  }

  public long getVirtualStorageMaxFetchRunBytes()
  {
    return virtualStorageMaxFetchRunBytes;
  }

  /**
   * Returns a copy of this config configured as an ephemeral, on-demand virtual-storage cache:
   * {@link #isVirtualStorage()} and {@link #isVirtualStorageEphemeral()} are set, and the settings that only apply to
   * a classic on-disk historical cache are dropped. Virtual-storage tuning ({@link #getVirtualStorageLoadThreads()},
   * {@link #isVirtualStorageUseVirtualThreads()}, {@link #getVirtualStorageMetadataReservationEstimate()}, the
   * coalescing/fetch-run limits) is preserved, so a per-task cache derived from a node's {@code druid.segmentCache}
   * config keeps operator tuning.
   */
  public SegmentLoaderConfig toEphemeralVirtualStorage()
  {
    return toBuilder()
        .virtualStorage(true)
        .virtualStorageIsEphemeral(true)
        .lazyLoadOnStart(false)
        .infoDir(null)
        .numThreadsToLoadSegmentsIntoPageCacheOnDownload(0)
        .numThreadsToLoadSegmentsIntoPageCacheOnBootstrap(null)
        .deleteOnRemove(true)
        .build();
  }

  /**
   * Convert a list of {@link StorageLocationConfig} objects to {@link StorageLocation} objects.
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

  public Builder toBuilder()
  {
    return new Builder()
        .runtimeInfo(runtimeInfo)
        .locations(locations)
        .lazyLoadOnStart(lazyLoadOnStart)
        .deleteOnRemove(deleteOnRemove)
        .dropSegmentDelayMillis(dropSegmentDelayMillis)
        .announceIntervalMillis(announceIntervalMillis)
        .numLoadingThreads(numLoadingThreads)
        .numBootstrapThreads(numBootstrapThreads)
        .numThreadsToLoadSegmentsIntoPageCacheOnDownload(numThreadsToLoadSegmentsIntoPageCacheOnDownload)
        .numThreadsToLoadSegmentsIntoPageCacheOnBootstrap(numThreadsToLoadSegmentsIntoPageCacheOnBootstrap)
        .infoDir(infoDir)
        .statusQueueMaxSize(statusQueueMaxSize)
        .virtualStorage(virtualStorage)
        .virtualStorageLoadThreads(virtualStorageLoadThreads)
        .virtualStorageUseVirtualThreads(virtualStorageUseVirtualThreads)
        .virtualStorageIsEphemeral(virtualStorageIsEphemeral)
        .virtualStorageMetadataReservationEstimate(virtualStorageMetadataReservationEstimate)
        .virtualStoragePartialDownloadsEnabled(virtualStoragePartialDownloadsEnabled)
        .virtualStorageCoalesceGapBytes(virtualStorageCoalesceGapBytes)
        .virtualStorageMaxFetchRunBytes(virtualStorageMaxFetchRunBytes);
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
           ", virtualStorageCoalesceGapBytes=" + virtualStorageCoalesceGapBytes +
           ", virtualStorageMaxFetchRunBytes=" + virtualStorageMaxFetchRunBytes +
           '}';
  }

  public static Builder builder()
  {
    return new Builder();
  }

  /**
   * Builds a {@link SegmentLoaderConfig}. Any field left unset takes the same default the {@link JsonCreator}
   * constructor applies. Obtain one via {@link SegmentLoaderConfig#builder()} (all defaults) or
   * {@link SegmentLoaderConfig#toBuilder()} (a copy of an existing config).
   */
  public static class Builder
  {
    @Nullable
    private RuntimeInfo runtimeInfo;
    @Nullable
    private List<StorageLocationConfig> locations;
    @Nullable
    private Boolean lazyLoadOnStart;
    @Nullable
    private Boolean deleteOnRemove;
    @Nullable
    private Integer dropSegmentDelayMillis;
    @Nullable
    private Integer announceIntervalMillis;
    @Nullable
    private Integer numLoadingThreads;
    @Nullable
    private Integer numBootstrapThreads;
    @Nullable
    private Integer numThreadsToLoadSegmentsIntoPageCacheOnDownload;
    @Nullable
    private Integer numThreadsToLoadSegmentsIntoPageCacheOnBootstrap;
    @Nullable
    private File infoDir;
    @Nullable
    private Integer statusQueueMaxSize;
    @Nullable
    private Boolean virtualStorage;
    @Nullable
    private Integer virtualStorageLoadThreads;
    @Nullable
    private Boolean virtualStorageUseVirtualThreads;
    @Nullable
    private Boolean virtualStorageIsEphemeral;
    @Nullable
    private Long virtualStorageMetadataReservationEstimate;
    @Nullable
    private Boolean virtualStoragePartialDownloadsEnabled;
    @Nullable
    private Long virtualStorageCoalesceGapBytes;
    @Nullable
    private Long virtualStorageMaxFetchRunBytes;

    public Builder runtimeInfo(@Nullable RuntimeInfo runtimeInfo)
    {
      this.runtimeInfo = runtimeInfo;
      return this;
    }

    public Builder locations(@Nullable List<StorageLocationConfig> locations)
    {
      this.locations = locations;
      return this;
    }

    public Builder locations(StorageLocationConfig... locations)
    {
      this.locations = Arrays.asList(locations);
      return this;
    }

    public Builder lazyLoadOnStart(boolean lazyLoadOnStart)
    {
      this.lazyLoadOnStart = lazyLoadOnStart;
      return this;
    }

    public Builder deleteOnRemove(boolean deleteOnRemove)
    {
      this.deleteOnRemove = deleteOnRemove;
      return this;
    }

    public Builder dropSegmentDelayMillis(int dropSegmentDelayMillis)
    {
      this.dropSegmentDelayMillis = dropSegmentDelayMillis;
      return this;
    }

    public Builder announceIntervalMillis(int announceIntervalMillis)
    {
      this.announceIntervalMillis = announceIntervalMillis;
      return this;
    }

    public Builder numLoadingThreads(int numLoadingThreads)
    {
      this.numLoadingThreads = numLoadingThreads;
      return this;
    }

    public Builder numBootstrapThreads(@Nullable Integer numBootstrapThreads)
    {
      this.numBootstrapThreads = numBootstrapThreads;
      return this;
    }

    public Builder numThreadsToLoadSegmentsIntoPageCacheOnDownload(int numThreads)
    {
      this.numThreadsToLoadSegmentsIntoPageCacheOnDownload = numThreads;
      return this;
    }

    public Builder numThreadsToLoadSegmentsIntoPageCacheOnBootstrap(@Nullable Integer numThreads)
    {
      this.numThreadsToLoadSegmentsIntoPageCacheOnBootstrap = numThreads;
      return this;
    }

    public Builder infoDir(@Nullable File infoDir)
    {
      this.infoDir = infoDir;
      return this;
    }

    public Builder statusQueueMaxSize(int statusQueueMaxSize)
    {
      this.statusQueueMaxSize = statusQueueMaxSize;
      return this;
    }

    public Builder virtualStorage(boolean virtualStorage)
    {
      this.virtualStorage = virtualStorage;
      return this;
    }

    public Builder virtualStorageLoadThreads(int virtualStorageLoadThreads)
    {
      this.virtualStorageLoadThreads = virtualStorageLoadThreads;
      return this;
    }

    public Builder virtualStorageUseVirtualThreads(boolean virtualStorageUseVirtualThreads)
    {
      this.virtualStorageUseVirtualThreads = virtualStorageUseVirtualThreads;
      return this;
    }

    public Builder virtualStorageIsEphemeral(boolean virtualStorageIsEphemeral)
    {
      this.virtualStorageIsEphemeral = virtualStorageIsEphemeral;
      return this;
    }

    public Builder virtualStorageMetadataReservationEstimate(long virtualStorageMetadataReservationEstimate)
    {
      this.virtualStorageMetadataReservationEstimate = virtualStorageMetadataReservationEstimate;
      return this;
    }

    public Builder virtualStoragePartialDownloadsEnabled(boolean virtualStoragePartialDownloadsEnabled)
    {
      this.virtualStoragePartialDownloadsEnabled = virtualStoragePartialDownloadsEnabled;
      return this;
    }

    public Builder virtualStorageCoalesceGapBytes(long virtualStorageCoalesceGapBytes)
    {
      this.virtualStorageCoalesceGapBytes = virtualStorageCoalesceGapBytes;
      return this;
    }

    public Builder virtualStorageMaxFetchRunBytes(long virtualStorageMaxFetchRunBytes)
    {
      this.virtualStorageMaxFetchRunBytes = virtualStorageMaxFetchRunBytes;
      return this;
    }

    public SegmentLoaderConfig build()
    {
      return new SegmentLoaderConfig(
          runtimeInfo,
          locations,
          lazyLoadOnStart,
          deleteOnRemove,
          dropSegmentDelayMillis,
          announceIntervalMillis,
          numLoadingThreads,
          numBootstrapThreads,
          numThreadsToLoadSegmentsIntoPageCacheOnDownload,
          numThreadsToLoadSegmentsIntoPageCacheOnBootstrap,
          infoDir,
          statusQueueMaxSize,
          virtualStorage,
          virtualStorageLoadThreads,
          virtualStorageUseVirtualThreads,
          virtualStorageIsEphemeral,
          virtualStorageMetadataReservationEstimate,
          virtualStoragePartialDownloadsEnabled,
          virtualStorageCoalesceGapBytes,
          virtualStorageMaxFetchRunBytes
      );
    }
  }
}
