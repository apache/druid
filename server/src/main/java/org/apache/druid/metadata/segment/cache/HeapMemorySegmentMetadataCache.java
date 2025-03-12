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

package org.apache.druid.metadata.segment.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.TransactionCallback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory implementation of {@link SegmentMetadataCache}.
 * <p>
 * Non-leader Overlords also keep polling the metadata store to keep the cache
 * up-to-date in case leadership changes.
 * <p>
 * The map {@link #datasourceToSegmentCache} contains the cache for each datasource.
 * Items are only added to this map and never removed. This is to avoid handling
 * race conditions where a thread has invoked {@link #getDatasource} but hasn't
 * acquired a lock on the returned cache yet while another thread sees this cache
 * as empty and cleans it up. The first thread would then end up using a stopped
 * cache, resulting in errors.
 */
@ThreadSafe
public class HeapMemorySegmentMetadataCache implements SegmentMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(HeapMemorySegmentMetadataCache.class);

  private static final int SQL_MAX_RETRIES = 10;
  private static final int SQL_QUIET_RETRIES = 3;

  /**
   * Maximum time to wait for cache to be ready.
   */
  private static final int READY_TIMEOUT_MILLIS = 5 * 60_000;
  private static final int MIN_SYNC_DELAY_MILLIS = 1000;
  private static final int MAX_IMMEDIATE_SYNC_RETRIES = 3;

  private enum CacheState
  {
    STOPPED, FOLLOWER, LEADER_FIRST_SYNC_PENDING, LEADER_FIRST_SYNC_STARTED, LEADER_READY
  }

  private final ObjectMapper jsonMapper;
  private final Duration pollDuration;
  private final boolean isCacheEnabled;
  private final MetadataStorageTablesConfig tablesConfig;
  private final SQLMetadataConnector connector;

  private final ListeningScheduledExecutorService pollExecutor;
  private final ServiceEmitter emitter;

  private final Object cacheStateLock = new Object();

  @GuardedBy("cacheStateLock")
  private CacheState currentCacheState = CacheState.STOPPED;
  @GuardedBy("cacheStateLock")
  private ListenableFuture<Long> nextSyncFuture = null;
  @GuardedBy("cacheStateLock")
  private int consecutiveSyncFailures = 0;

  private final ConcurrentHashMap<String, HeapMemoryDatasourceSegmentCache>
      datasourceToSegmentCache = new ConcurrentHashMap<>();

  private final AtomicReference<DateTime> syncFinishTime = new AtomicReference<>();

  @Inject
  public HeapMemorySegmentMetadataCache(
      ObjectMapper jsonMapper,
      Supplier<SegmentsMetadataManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> tablesConfig,
      SQLMetadataConnector connector,
      ScheduledExecutorFactory executorFactory,
      ServiceEmitter emitter
  )
  {
    this.jsonMapper = jsonMapper;
    this.isCacheEnabled = config.get().isUseCache();
    this.pollDuration = config.get().getPollDuration().toStandardDuration();
    this.tablesConfig = tablesConfig.get();
    this.connector = connector;
    this.pollExecutor = isCacheEnabled
                        ? MoreExecutors.listeningDecorator(executorFactory.create(1, "SegmentMetadataCache-%s"))
                        : null;
    this.emitter = emitter;
  }


  @Override
  public void start()
  {
    if (!isCacheEnabled) {
      log.info("Segment metadata cache is not enabled.");
      return;
    }

    synchronized (cacheStateLock) {
      if (currentCacheState == CacheState.STOPPED) {
        updateCacheState(CacheState.FOLLOWER, "Scheduling sync with metadata store");
        scheduleSyncWithMetadataStore(pollDuration.getMillis());
      }
    }
  }

  @Override
  public void stop()
  {
    synchronized (cacheStateLock) {
      if (isCacheEnabled) {
        pollExecutor.shutdownNow();
        datasourceToSegmentCache.forEach((datasource, cache) -> cache.stop());
        datasourceToSegmentCache.clear();
        syncFinishTime.set(null);

        updateCacheState(CacheState.STOPPED, "Stopped sync with metadata store");
      }
    }
  }

  @Override
  public void becomeLeader()
  {
    synchronized (cacheStateLock) {
      if (isCacheEnabled) {
        if (currentCacheState == CacheState.STOPPED) {
          throw DruidException.defensive("Cache has not been started yet");
        } else {
          updateCacheState(CacheState.LEADER_FIRST_SYNC_PENDING, "We are now leader");

          // Cancel the current sync so that a fresh one is scheduled and cache becomes ready sooner
          if (nextSyncFuture != null && !nextSyncFuture.isDone()) {
            nextSyncFuture.cancel(true);
          }
        }
      }
    }
  }

  @Override
  public void stopBeingLeader()
  {
    synchronized (cacheStateLock) {
      if (isCacheEnabled) {
        updateCacheState(CacheState.FOLLOWER, "Not leader anymore");
      }
    }
  }

  @Override
  public boolean isEnabled()
  {
    return isCacheEnabled;
  }

  @Override
  public DatasourceSegmentCache getDatasource(String dataSource)
  {
    verifyCacheIsUsableAndAwaitSync();
    emitMetric(dataSource, Metric.TRANSACTION_COUNT, 1);
    return getCacheForDatasource(dataSource);
  }

  private HeapMemoryDatasourceSegmentCache getCacheForDatasource(String dataSource)
  {
    return datasourceToSegmentCache.computeIfAbsent(dataSource, HeapMemoryDatasourceSegmentCache::new);
  }

  /**
   * Verifies that the cache is enabled, started and has become leader.
   * Also waits for the cache to be synced with metadata store.
   *
   * @throws DruidException if the cache is disabled, stopped or not leader.
   */
  private void verifyCacheIsUsableAndAwaitSync()
  {
    if (!isCacheEnabled) {
      throw DruidException.defensive("Segment metadata cache is not enabled.");
    }

    synchronized (cacheStateLock) {
      switch (currentCacheState) {
        case STOPPED:
          throw InternalServerError.exception("Segment metadata cache has not been started yet.");
        case FOLLOWER:
          throw InternalServerError.exception("Not leader yet. Segment metadata cache is not usable.");
        case LEADER_FIRST_SYNC_PENDING:
        case LEADER_FIRST_SYNC_STARTED:
          waitForCacheToFinishSync();
          verifyCacheIsUsableAndAwaitSync();
        case LEADER_READY:
          // Cache is now ready for use
      }
    }
  }

  /**
   * Waits for cache to become ready if we are leader and current state is
   * {@link CacheState#LEADER_FIRST_SYNC_PENDING} or
   * {@link CacheState#LEADER_FIRST_SYNC_STARTED}.
   */
  private void waitForCacheToFinishSync()
  {
    synchronized (cacheStateLock) {
      log.info("Waiting for cache to finish sync with metadata store.");
      while (currentCacheState == CacheState.LEADER_FIRST_SYNC_PENDING
             || currentCacheState == CacheState.LEADER_FIRST_SYNC_STARTED) {
        try {
          cacheStateLock.wait(READY_TIMEOUT_MILLIS);
        }
        catch (InterruptedException e) {
          log.noStackTrace().info(e, "Interrupted while waiting for cache to be ready");
        }
        catch (Exception e) {
          log.noStackTrace().error(e, "Error while waiting for cache to be ready");
          throw DruidException.defensive(e, "Error while waiting for cache to be ready");
        }
      }
      log.info("Wait complete. Cache is now in state[%s].", currentCacheState);
    }
  }

  private void updateCacheState(CacheState targetState, String message)
  {
    synchronized (cacheStateLock) {
      currentCacheState = targetState;
      log.info("%s. Cache is now in state[%s].", message, currentCacheState);

      // Notify threads waiting for cache to be ready
      if (currentCacheState == CacheState.LEADER_READY) {
        cacheStateLock.notifyAll();
      }
    }
  }

  /**
   * Schedules a sync with metadata store after the given delay in milliseconds.
   */
  private void scheduleSyncWithMetadataStore(long delayMillis)
  {
    synchronized (cacheStateLock) {
      nextSyncFuture = pollExecutor.schedule(this::syncWithMetadataStore, delayMillis, TimeUnit.MILLISECONDS);

      Futures.addCallback(
          nextSyncFuture,
          new FutureCallback<>()
          {
            @Override
            public void onSuccess(Long previousSyncDurationMillis)
            {
              synchronized (cacheStateLock) {
                if (currentCacheState == CacheState.LEADER_FIRST_SYNC_STARTED) {
                  updateCacheState(
                      CacheState.LEADER_READY,
                      StringUtils.format(
                          "Finished sync with metadata store in [%d] millis",
                          previousSyncDurationMillis
                      )
                  );
                }
              }

              emitMetric(Metric.SYNC_DURATION_MILLIS, previousSyncDurationMillis);

              // Schedule the next sync
              final long nextSyncDelay;
              synchronized (cacheStateLock) {
                consecutiveSyncFailures = 0;
                if (currentCacheState == CacheState.LEADER_FIRST_SYNC_PENDING) {
                  nextSyncDelay = 0;
                } else {
                  nextSyncDelay = Math.max(pollDuration.getMillis() - previousSyncDurationMillis, 0);
                }
              }
              scheduleSyncWithMetadataStore(nextSyncDelay);
            }

            @Override
            public void onFailure(Throwable t)
            {
              if (t instanceof CancellationException) {
                log.noStackTrace().info(t, "Sync with metadata store was cancelled");
              } else {
                log.noStackTrace()
                   .makeAlert(t, "Could not sync segment metadata cache with metadata store")
                   .emit();
              }

              // Schedule the next sync
              final long nextSyncDelay;
              synchronized (cacheStateLock) {
                // Retry immediately if first sync is pending or number of consecutive failures is low
                if (++consecutiveSyncFailures > MAX_IMMEDIATE_SYNC_RETRIES
                    || currentCacheState != CacheState.LEADER_FIRST_SYNC_PENDING) {
                  nextSyncDelay = pollDuration.getMillis();
                } else {
                  nextSyncDelay = MIN_SYNC_DELAY_MILLIS;
                }
              }
              scheduleSyncWithMetadataStore(nextSyncDelay);
            }
          },
          pollExecutor
      );
    }
  }

  /**
   * Retrieves segments from the metadata store and updates the cache, if required.
   * <p>
   * The following actions are performed in every sync:
   * <ul>
   * <li>Retrieve all used and unused segment IDs along with their updated timestamps</li>
   * <li>Retrieve payloads of used segments which have been updated in the metadata
   * store but not in the cache</li>
   * <li>Retrieve all pending segments and update the cache as needed</li>
   * <li>Remove segments not present in the metadata store</li>
   * <li>Reset the max unused partition IDs</li>
   * <li>Change the cache state to ready if it is leader and waiting for first sync</li>
   * <li>Emit metrics</li>
   * </ul>
   *
   * @return Time taken in milliseconds for the sync to finish
   */
  private long syncWithMetadataStore()
  {
    final DateTime syncStartTime = DateTimes.nowUtc();
    final Stopwatch totalSyncDuration = Stopwatch.createStarted();

    synchronized (cacheStateLock) {
      if (currentCacheState == CacheState.LEADER_FIRST_SYNC_PENDING) {
        updateCacheState(
            CacheState.LEADER_FIRST_SYNC_STARTED,
            "Started sync of latest updates from metadata store"
        );
      }
    }

    final Map<String, DatasourceSegmentSummary> datasourceToSummary = new HashMap<>();
    retrieveAllSegmentIds(datasourceToSummary);
    updateSegmentIdsInCache(datasourceToSummary, syncStartTime);
    retrieveUsedSegmentPayloads(datasourceToSummary);
    updateUsedSegmentPayloadsInCache(datasourceToSummary);
    retrieveAllPendingSegments(datasourceToSummary);
    updatePendingSegmentsInCache(datasourceToSummary, syncStartTime);

    datasourceToSegmentCache.values().forEach(
        HeapMemoryDatasourceSegmentCache::markCacheSynced
    );

    syncFinishTime.set(DateTimes.nowUtc());
    return totalSyncDuration.millisElapsed();
  }

  /**
   * Retrieves all the segment IDs (used and unused) from the metadata store.
   * Populates the summary for the datasources found in metadata store.
   */
  private void retrieveAllSegmentIds(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final Stopwatch retrieveDuration = Stopwatch.createStarted();
    final String sql = StringUtils.format(
        "SELECT id, dataSource, used, used_status_last_updated FROM %s",
        tablesConfig.getSegmentsTable()
    );

    final int numSkippedRecords = inReadOnlyTransaction((handle, status) -> {
      try (
          ResultIterator<SegmentRecord> iterator =
              handle.createQuery(sql)
                    .setFetchSize(connector.getStreamingFetchSize())
                    .map((index, r, ctx) -> SegmentRecord.fromResultSet(r))
                    .iterator()
      ) {
        int skippedRecords = 0;
        while (iterator.hasNext()) {
          final SegmentRecord record = iterator.next();
          if (record == null) {
            ++skippedRecords;
          } else {
            final SegmentId segmentId = record.getSegmentId();
            datasourceToSummary
                .computeIfAbsent(segmentId.getDataSource(), ds -> new DatasourceSegmentSummary())
                .addSegmentRecord(record);
          }
        }

        return skippedRecords;
      }
    });

    // Emit metrics
    if (numSkippedRecords > 0) {
      emitMetric(Metric.SKIPPED_SEGMENTS, numSkippedRecords);
    }
    datasourceToSummary.forEach((dataSource, summary) -> {
      emitMetric(dataSource, Metric.PERSISTED_USED_SEGMENTS, summary.numPersistedUsedSegments);
      emitMetric(dataSource, Metric.PERSISTED_UNUSED_SEGMENTS, summary.numPersistedUnusedSegments);
    });
    emitMetric(Metric.RETRIEVE_SEGMENT_IDS_DURATION_MILLIS, retrieveDuration.millisElapsed());
  }

  private <T> T inReadOnlyTransaction(TransactionCallback<T> callback)
  {
    return connector.retryReadOnlyTransaction(callback, SQL_QUIET_RETRIES, SQL_MAX_RETRIES);
  }

  /**
   * Retrieves the payloads of required used segments from the metadata store
   * and updates the cache. A segment needs to be refreshed only if
   * {@link HeapMemoryDatasourceSegmentCache#shouldRefreshUsedSegment}
   * returns true for it.
   */
  private void retrieveRequiredUsedSegments(
      String dataSource,
      DatasourceSegmentSummary summary
  )
  {
    final Set<String> segmentIdsToRefresh = summary.usedSegmentIdsToRefresh;
    if (segmentIdsToRefresh.isEmpty()) {
      return;
    }

    inReadOnlyTransaction((handle, status) -> {
      try (
          CloseableIterator<DataSegmentPlus> iterator =
              SqlSegmentsMetadataQuery
                  .forHandle(handle, connector, tablesConfig, jsonMapper)
                  .retrieveSegmentsByIdIterator(dataSource, segmentIdsToRefresh, false)
      ) {
        iterator.forEachRemaining(summary.usedSegments::add);
        return 0;
      }
    });
  }

  /**
   * Updates the cache of each datasource with the segment IDs fetched from the
   * metadata store in {@link #retrieveAllSegmentIds}. The update done on each
   * datasource cache is atomic.
   */
  private void updateSegmentIdsInCache(
      Map<String, DatasourceSegmentSummary> datasourceToSummary,
      DateTime syncStartTime
  )
  {
    final Stopwatch updateDuration = Stopwatch.createStarted();

    // Sync segments for datasources which were retrieved in the latest poll
    datasourceToSummary.forEach((dataSource, summary) -> {
      final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);

      final SegmentSyncResult result = cache.syncSegmentIds(summary.persistedSegments, syncStartTime);
      emitNonZeroMetric(dataSource, Metric.UPDATED_UNUSED_SEGMENTS, result.getUpdated());
      emitNonZeroMetric(dataSource, Metric.STALE_USED_SEGMENTS, result.getExpiredIds().size());
      emitNonZeroMetric(dataSource, Metric.DELETED_SEGMENTS, result.getDeleted());

      summary.usedSegmentIdsToRefresh.addAll(result.getExpiredIds());
    });

    // Update cache for datasources which returned no segments in the latest poll
    datasourceToSegmentCache.forEach((dataSource, cache) -> {
      if (!datasourceToSummary.containsKey(dataSource)) {
        final SegmentSyncResult result = cache.syncSegmentIds(List.of(), syncStartTime);
        emitNonZeroMetric(dataSource, Metric.DELETED_SEGMENTS, result.getDeleted());
      }
    });

    emitMetric(Metric.UPDATE_IDS_DURATION_MILLIS, updateDuration.millisElapsed());
  }

  /**
   * Retrieves the payloads of required used segments from the metadata store
   * and updates the cache. A segment needs to be refreshed only if
   * {@link HeapMemoryDatasourceSegmentCache#shouldRefreshUsedSegment}
   * returns true for it.
   */
  private void retrieveUsedSegmentPayloads(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final Stopwatch retrieveDuration = Stopwatch.createStarted();

    if (syncFinishTime.get() == null) {
      datasourceToSummary.forEach(this::retrieveAllUsedSegments);
    } else {
      datasourceToSummary.forEach(this::retrieveRequiredUsedSegments);
    }

    emitMetric(Metric.RETRIEVE_SEGMENT_PAYLOADS_DURATION_MILLIS, retrieveDuration.millisElapsed());
  }

  /**
   * Retrieves all used segments for a datasource from the metadata store.
   */
  private void retrieveAllUsedSegments(String dataSource, DatasourceSegmentSummary summary)
  {
    inReadOnlyTransaction((handle, status) -> {
      try (
          CloseableIterator<DataSegmentPlus> iterator =
              SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, jsonMapper)
                                      .retrieveUsedSegmentsPlus(dataSource, List.of())
      ) {
        iterator.forEachRemaining(summary.usedSegments::add);
        return 0;
      }
    });
  }

  /**
   * Updates the cache of each datasource with the segment payloads fetched from
   * the metadata store in {@link #retrieveUsedSegmentPayloads}. The update done on
   * each datasource cache is atomic.
   */
  private void updateUsedSegmentPayloadsInCache(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    datasourceToSummary.forEach((dataSource, summary) -> {
      final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);
      final int numUpdatedSegments = cache.insertSegments(summary.usedSegments);
      emitNonZeroMetric(dataSource, Metric.UPDATED_USED_SEGMENTS, numUpdatedSegments);
    });
  }

  /**
   * Updates the cache of each datasource with the pending segments fetched from
   * the metadata store in {@link #retrieveAllPendingSegments}. The update done on
   * each datasource cache is atomic.
   */
  private void updatePendingSegmentsInCache(
      Map<String, DatasourceSegmentSummary> datasourceToSummary,
      DateTime syncStartTime
  )
  {
    datasourceToSummary.forEach((dataSource, summary) -> {
      final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);
      final SegmentSyncResult result = cache.syncPendingSegments(summary.persistedPendingSegments, syncStartTime);

      emitMetric(dataSource, Metric.PERSISTED_PENDING_SEGMENTS, summary.persistedPendingSegments.size());
      emitNonZeroMetric(dataSource, Metric.UPDATED_PENDING_SEGMENTS, result.getUpdated());
      emitNonZeroMetric(dataSource, Metric.DELETED_PENDING_SEGMENTS, result.getDeleted());
    });

    // Update cache for datasources which returned no segments in the latest poll
    datasourceToSegmentCache.forEach((dataSource, cache) -> {
      if (!datasourceToSummary.containsKey(dataSource)) {
        final SegmentSyncResult result = cache.syncPendingSegments(List.of(), syncStartTime);
        emitNonZeroMetric(dataSource, Metric.DELETED_PENDING_SEGMENTS, result.getDeleted());
      }
    });
  }

  /**
   * Retrieves all pending segments from metadata store and populates them in
   * the respective {@link DatasourceSegmentSummary}.
   */
  private void retrieveAllPendingSegments(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final Stopwatch fetchDuration = Stopwatch.createStarted();
    final String sql = StringUtils.format(
        "SELECT id, dataSource, payload, sequence_name, sequence_prev_id,"
        + " upgraded_from_segment_id, task_allocator_id, created_date FROM %1$s",
        tablesConfig.getPendingSegmentsTable()
    );

    final AtomicInteger numSkippedRecords = new AtomicInteger();
    inReadOnlyTransaction(
        (handle, status) -> handle
            .createQuery(sql)
            .setFetchSize(connector.getStreamingFetchSize())
            .map((index, r, ctx) -> {
              String segmentId = null;
              String dataSource = null;
              try {
                // Read the Segment ID and datasource for logging in case
                // the rest of the result set cannot be read
                segmentId = r.getString("id");
                dataSource = r.getString("dataSource");
                datasourceToSummary
                    .computeIfAbsent(dataSource, ds -> new DatasourceSegmentSummary())
                    .addPendingSegmentRecord(PendingSegmentRecord.fromResultSet(r, jsonMapper));
              }
              catch (Exception e) {
                log.noStackTrace().error(
                    e,
                    "Error occurred while reading Pending Segment ID[%s] of datasource[%s].",
                    segmentId, dataSource
                );
                numSkippedRecords.incrementAndGet();
              }

              return 0;
            }).list()
    );

    emitMetric(Metric.RETRIEVE_PENDING_SEGMENTS_DURATION_MILLIS, fetchDuration.millisElapsed());
    if (numSkippedRecords.get() > 0) {
      emitMetric(Metric.SKIPPED_PENDING_SEGMENTS, numSkippedRecords.get());
    }
  }

  private void emitMetric(String metric, long value)
  {
    emitter.emit(
        ServiceMetricEvent.builder().setMetric(metric, value)
    );
  }

  private void emitNonZeroMetric(String datasource, String metric, long value)
  {
    if (value == 0) {
      return;
    }
    emitMetric(datasource, metric, value);
  }

  private void emitMetric(String datasource, String metric, long value)
  {
    emitter.emit(
        ServiceMetricEvent.builder()
                          .setDimension(DruidMetrics.DATASOURCE, datasource)
                          .setMetric(metric, value)
    );
  }

  /**
   * Summary of segments currently present in the metadata store for a single
   * datasource.
   */
  private static class DatasourceSegmentSummary
  {
    final List<SegmentRecord> persistedSegments = new ArrayList<>();
    final List<PendingSegmentRecord> persistedPendingSegments = new ArrayList<>();

    final Set<String> usedSegmentIdsToRefresh = new HashSet<>();
    final Set<DataSegmentPlus> usedSegments = new HashSet<>();

    int numPersistedUsedSegments = 0;
    int numPersistedUnusedSegments = 0;

    private void addSegmentRecord(SegmentRecord record)
    {
      persistedSegments.add(record);
      if (record.isUsed()) {
        ++numPersistedUsedSegments;
      } else {
        ++numPersistedUnusedSegments;
      }
    }

    private void addPendingSegmentRecord(PendingSegmentRecord record)
    {
      persistedPendingSegments.add(record);
    }
  }
}
