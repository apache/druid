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
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
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
import org.joda.time.Interval;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.TransactionCallback;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory implementation of {@link SegmentMetadataCache}.
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
  @LifecycleStart
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
  @LifecycleStop
  public void stop()
  {
    synchronized (cacheStateLock) {
      if (isCacheEnabled) {
        pollExecutor.shutdownNow();
        datasourceToSegmentCache.forEach((datasource, cache) -> cache.stop());
        datasourceToSegmentCache.clear();

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
              syncFinishTime.set(DateTimes.nowUtc());

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
    final DateTime pollStartTime = DateTimes.nowUtc();
    final Stopwatch sincePollStart = Stopwatch.createStarted();

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

    datasourceToSegmentCache.keySet().forEach(
        dataSource -> removeUnknownSegmentsFromCache(
            dataSource,
            datasourceToSummary.computeIfAbsent(dataSource, ds -> new DatasourceSegmentSummary()),
            pollStartTime
        )
    );

    datasourceToSummary.forEach(this::retrieveAndRefreshUsedSegments);

    retrieveAndRefreshAllPendingSegments(datasourceToSummary);
    datasourceToSegmentCache.keySet().forEach(
        dataSource -> removeUnknownPendingSegmentsFromCache(
            dataSource,
            datasourceToSummary.computeIfAbsent(dataSource, ds -> new DatasourceSegmentSummary()),
            pollStartTime
        )
    );

    datasourceToSegmentCache.values().forEach(
        HeapMemoryDatasourceSegmentCache::markCacheSynced
    );

    datasourceToSummary.forEach(this::emitSummaryMetrics);
    return sincePollStart.millisElapsed();
  }

  /**
   * Retrieves all the segment IDs (used and unused) from the metadata store.
   * Populates the summary for the datasources found in metadata store.
   */
  private void retrieveAllSegmentIds(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final String sql = StringUtils.format(
        "SELECT id, dataSource, used, used_status_last_updated FROM %s",
        tablesConfig.getSegmentsTable()
    );

    final AtomicInteger numSkippedRecords = new AtomicInteger(0);
    inReadOnlyTransaction((handle, status) -> {
      try (
          ResultIterator<SegmentRecord> iterator =
              handle.createQuery(sql)
                    .map((index, r, ctx) -> SegmentRecord.fromResultSet(r))
                    .iterator()
      ) {
        while (iterator.hasNext()) {
          final SegmentRecord record = iterator.next();
          if (record == null) {
            numSkippedRecords.incrementAndGet();
            continue;
          }

          final SegmentId segmentId = record.segmentId;
          final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(record.dataSource);
          final DatasourceSegmentSummary summary = datasourceToSummary
              .computeIfAbsent(record.dataSource, ds -> new DatasourceSegmentSummary());

          if (record.isUsed) {
            summary.numPersistedUsedSegments++;

            // Check if the used segment needs to be refreshed
            if (cache.shouldRefreshUsedSegment(segmentId, record.lastUpdatedTime)) {
              summary.usedSegmentIdsToRefresh.add(record.segmentId.toString());
            }
          } else {
            summary.numPersistedUnusedSegments++;
            if (cache.addUnusedSegmentId(segmentId, record.lastUpdatedTime)) {
              summary.numUnusedSegmentsUpdated++;
            }

            // Track max partition number of unused segment if needed
            summary
                .intervalVersionToMaxUnusedPartition
                .computeIfAbsent(segmentId.getInterval(), i -> new HashMap<>())
                .merge(segmentId.getVersion(), segmentId.getPartitionNum(), Math::max);
          }

          summary.persistedSegmentIds.add(segmentId);
        }

        return 0;
      }
    });

    if (numSkippedRecords.get() > 0) {
      emitMetric(Metric.SKIPPED_SEGMENTS, numSkippedRecords.get());
    }
  }

  private <T> T inReadOnlyTransaction(TransactionCallback<T> callback)
  {
    return connector.retryReadOnlyTransaction(callback, SQL_QUIET_RETRIES, SQL_MAX_RETRIES);
  }

  /**
   * Emits metrics for a datasource after the sync has finished.
   * If there are no persisted or cached segments for the datasource, no metrics
   * are emitted.
   */
  private void emitSummaryMetrics(String dataSource, DatasourceSegmentSummary summary)
  {
    final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);
    if (cache.isEmpty() && summary.isEmpty() && !summary.isCacheUpdated()) {
      // This is non-existent datasource and has a dangling entry in the datasourceToSegmentCache map
      return;
    }

    emitMetric(dataSource, Metric.PERSISTED_USED_SEGMENTS, summary.numPersistedUsedSegments);
    emitMetric(dataSource, Metric.PERSISTED_UNUSED_SEGMENTS, summary.numPersistedUnusedSegments);
    emitMetric(dataSource, Metric.PERSISTED_PENDING_SEGMENTS, summary.persistedPendingSegmentIds.size());
    emitMetric(dataSource, Metric.STALE_USED_SEGMENTS, summary.usedSegmentIdsToRefresh.size());

    emitNonZeroMetric(dataSource, Metric.DELETED_SEGMENTS, summary.numSegmentsRemoved);
    emitNonZeroMetric(dataSource, Metric.DELETED_PENDING_SEGMENTS, summary.numPendingSegmentsRemoved);
    emitNonZeroMetric(dataSource, Metric.UPDATED_USED_SEGMENTS, summary.numUsedSegmentsRefreshed);
    emitNonZeroMetric(dataSource, Metric.UPDATED_UNUSED_SEGMENTS, summary.numUnusedSegmentsUpdated);
    emitNonZeroMetric(dataSource, Metric.UPDATED_PENDING_SEGMENTS, summary.numPendingSegmentsUpdated);

    if (summary.isCacheUpdated()) {
      log.info(
          "Updated metadata cache for datasource[%s]."
          + " Added [%d] used, [%d] unused, [%d] pending segments."
          + " Deleted [%d] segments, [%d] pending segments.",
          dataSource, summary.numUsedSegmentsRefreshed,
          summary.numUnusedSegmentsUpdated, summary.numPendingSegmentsUpdated,
          summary.numSegmentsRemoved, summary.numPendingSegmentsRemoved
      );
    }
  }

  /**
   * Retrieves the payloads of required used segments from the metadata store
   * and updates the cache. A segment needs to be refreshed only if
   * {@link HeapMemoryDatasourceSegmentCache#shouldRefreshUsedSegment}
   * returns true for it.
   */
  private void retrieveAndRefreshUsedSegments(
      String dataSource,
      DatasourceSegmentSummary summary
  )
  {
    if (summary.usedSegmentIdsToRefresh.isEmpty()) {
      return;
    }

    final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);

    summary.numUsedSegmentsRefreshed = inReadOnlyTransaction((handle, status) -> {
      int updatedCount = 0;
      try (
          CloseableIterator<DataSegmentPlus> iterator =
              SqlSegmentsMetadataQuery
                  .forHandle(handle, connector, tablesConfig, jsonMapper)
                  .retrieveSegmentsByIdIterator(dataSource, summary.usedSegmentIdsToRefresh, false)
      ) {
        while (iterator.hasNext()) {
          if (cache.addSegment(iterator.next())) {
            ++updatedCount;
          }
        }
      }

      return updatedCount;
    });
  }

  /**
   * Retrieves all pending segments from metadata store and updates the cache if
   * {@link HeapMemoryDatasourceSegmentCache#shouldRefreshPendingSegment} is
   * true for it.
   */
  private void retrieveAndRefreshAllPendingSegments(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
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

                final PendingSegmentRecord record = PendingSegmentRecord.fromResultSet(r, jsonMapper);
                final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);
                final boolean updated = cache.shouldRefreshPendingSegment(record)
                                        && cache.insertPendingSegment(record, false);

                final DatasourceSegmentSummary summary = datasourceToSummary
                    .computeIfAbsent(dataSource, ds -> new DatasourceSegmentSummary());
                if (updated) {
                  summary.numPendingSegmentsUpdated++;
                }
                summary.persistedPendingSegmentIds.add(record.getId().toString());
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

    if (numSkippedRecords.get() > 0) {
      emitMetric(Metric.SKIPPED_PENDING_SEGMENTS, numSkippedRecords.get());
    }
  }

  /**
   * Removes pending segments from cache if they are not present in the metadata
   * and were created strictly before the current sync started.
   */
  private void removeUnknownPendingSegmentsFromCache(
      final String dataSource,
      final DatasourceSegmentSummary summary,
      final DateTime syncStartTime
  )
  {
    summary.numPendingSegmentsRemoved =
        getCacheForDatasource(dataSource)
            .removeUnpersistedPendingSegments(summary.persistedPendingSegmentIds, syncStartTime);
  }

  /**
   * Removes segments from the cache if they are not present in the metadata
   * store and were updated before the latest sync started.
   */
  private void removeUnknownSegmentsFromCache(
      final String dataSource,
      final DatasourceSegmentSummary summary,
      final DateTime syncStartTime
  )
  {
    summary.numSegmentsRemoved =
        getCacheForDatasource(dataSource)
            .removeUnpersistedSegments(summary.persistedSegmentIds, syncStartTime);
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

  @Nullable
  private static DateTime nullSafeDate(String date)
  {
    return date == null ? null : DateTimes.of(date);
  }

  /**
   * Represents a single record in the druid_segments table.
   */
  private static class SegmentRecord
  {
    private final SegmentId segmentId;
    private final String dataSource;
    private final boolean isUsed;
    private final DateTime lastUpdatedTime;

    SegmentRecord(SegmentId segmentId, String dataSource, boolean isUsed, DateTime lastUpdatedTime)
    {
      this.segmentId = segmentId;
      this.dataSource = dataSource;
      this.isUsed = isUsed;
      this.lastUpdatedTime = lastUpdatedTime;
    }

    /**
     * Creates a SegmentRecord from the given result set.
     *
     * @return null if an error occurred while reading the record.
     */
    @Nullable
    static SegmentRecord fromResultSet(ResultSet r)
    {
      String serializedId = null;
      String dataSource = null;
      try {
        serializedId = r.getString("id");
        dataSource = r.getString("dataSource");

        final boolean isUsed = r.getBoolean("used");
        final DateTime lastUpdatedTime = nullSafeDate(r.getString("used_status_last_updated"));

        final SegmentId segmentId = SegmentId.tryParse(dataSource, serializedId);
        if (segmentId == null) {
          log.noStackTrace().error(
              "Could not parse Segment ID[%s] of datasource[%s]",
              serializedId, dataSource
          );
          return null;
        } else {
          return new SegmentRecord(segmentId, dataSource, isUsed, lastUpdatedTime);
        }
      }
      catch (Exception e) {
        log.noStackTrace().error(
            e,
            "Error occurred while reading Segment ID[%s] of datasource[%s]",
            serializedId, dataSource
        );
        return null;
      }
    }
  }

  /**
   * Summary of segments currently present in the metadata store for a single
   * datasource.
   */
  private static class DatasourceSegmentSummary
  {
    final Set<SegmentId> persistedSegmentIds = new HashSet<>();
    final Set<String> persistedPendingSegmentIds = new HashSet<>();

    final Set<String> usedSegmentIdsToRefresh = new HashSet<>();
    final Map<Interval, Map<String, Integer>> intervalVersionToMaxUnusedPartition = new HashMap<>();

    int numPersistedUsedSegments = 0;
    int numPersistedUnusedSegments = 0;
    int numUsedSegmentsRefreshed = 0;
    int numUnusedSegmentsUpdated = 0;
    int numSegmentsRemoved = 0;
    int numPendingSegmentsRemoved = 0;
    int numPendingSegmentsUpdated = 0;

    private boolean isEmpty()
    {
      return persistedPendingSegmentIds.isEmpty() && persistedSegmentIds.isEmpty();
    }

    /**
     * @return true if any of the segments for this datasource have been updated
     * in the cache in the current sync.
     */
    private boolean isCacheUpdated()
    {
      return numSegmentsRemoved > 0
             || numUsedSegmentsRefreshed > 0
             || numUnusedSegmentsUpdated > 0
             || numPendingSegmentsRemoved > 0
             || numPendingSegmentsUpdated > 0;
    }
  }

}
