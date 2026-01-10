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
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
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
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.segment.metadata.CompactionStateCache;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.TransactionCallback;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * In-memory implementation of {@link SegmentMetadataCache}.
 * <p>
 * Only used segments (excluding num_rows and schema_fingerprint) and
 * pending segments are cached. Unused segments are not cached.
 * <p>
 * Non-leader Overlords also keep polling the metadata store to keep the cache
 * up-to-date in case leadership changes.
 * <p>
 * For cache usage modes, see {@link UsageMode}.
 * <p>
 * The map {@link #datasourceToSegmentCache} contains the cache for each datasource.
 * If the cache for a datasource is empty, the sync thread removes it from the map.
 */
@ThreadSafe
public class HeapMemorySegmentMetadataCache implements SegmentMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(HeapMemorySegmentMetadataCache.class);

  private static final int SQL_MAX_RETRIES = 3;
  private static final int SQL_QUIET_RETRIES = 2;

  /**
   * Maximum time to wait for cache to be ready.
   */
  private static final int READY_TIMEOUT_MILLIS = 5 * 60_000;
  private static final int MIN_SYNC_DELAY_MILLIS = 1000;
  private static final int MAX_IMMEDIATE_SYNC_RETRIES = 3;

  /**
   * Buffer duration for which entries are kept in the cache even if the
   * metadata store does not have them. In other words, a segment entry is
   * removed from cache if the entry is not present in metadata store and has a
   * {@code lastUpdatedTime < now - bufferWindow}.
   * <p>
   * This is primarily needed to handle a race condition between insert and sync
   * where an entry with an updated time just before the sync start is added to
   * the cache just after the sync has started.
   * <p>
   * This means that non-leader Overlord and all Coordinators will continue to
   * consider a segment as used if it was marked as unused within the buffer period
   * of a previous update (e.g. segment created, marked used or schema info updated).
   */
  private static final Duration SYNC_BUFFER_DURATION = Duration.standardSeconds(10);

  private enum CacheState
  {
    STOPPED, FOLLOWER, LEADER_FIRST_SYNC_PENDING, LEADER_FIRST_SYNC_STARTED, LEADER_READY
  }

  private final ObjectMapper jsonMapper;
  private final Duration pollDuration;
  private final UsageMode cacheMode;
  private final MetadataStorageTablesConfig tablesConfig;
  private final SQLMetadataConnector connector;

  private final boolean useSchemaCache;
  private final SegmentSchemaCache segmentSchemaCache;

  private final boolean useCompactionStateCache;
  private final CompactionStateCache compactionStateCache;

  private final ListeningScheduledExecutorService pollExecutor;
  private final ServiceEmitter emitter;

  private final Object cacheStateLock = new Object();

  /**
   * Denotes that the cache is in state {@link CacheState#LEADER_READY}.
   * Maintained as a separate variable to avoid acquiring the {@link #cacheStateLock}
   * whenever {@link #isSyncedForRead()} is checked in a transaction.
   */
  private final AtomicBoolean isCacheReady = new AtomicBoolean(false);

  @GuardedBy("cacheStateLock")
  private CacheState currentCacheState = CacheState.STOPPED;
  @GuardedBy("cacheStateLock")
  private ListenableFuture<Long> nextSyncFuture = null;
  @GuardedBy("cacheStateLock")
  private int consecutiveSyncFailures = 0;

  private final ConcurrentHashMap<String, HeapMemoryDatasourceSegmentCache>
      datasourceToSegmentCache = new ConcurrentHashMap<>();

  private final AtomicReference<DateTime> syncFinishTime = new AtomicReference<>();
  private final AtomicReference<DataSourcesSnapshot> datasourcesSnapshot = new AtomicReference<>(null);

  @Inject
  public HeapMemorySegmentMetadataCache(
      ObjectMapper jsonMapper,
      Supplier<SegmentsMetadataManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> tablesConfig,
      SegmentSchemaCache segmentSchemaCache,
      CompactionStateCache compactionStateCache,
      SQLMetadataConnector connector,
      ScheduledExecutorFactory executorFactory,
      ServiceEmitter emitter
  )
  {
    this.jsonMapper = jsonMapper;
    this.cacheMode = config.get().getCacheUsageMode();
    this.pollDuration = config.get().getPollDuration().toStandardDuration();
    this.tablesConfig = tablesConfig.get();
    this.useSchemaCache = segmentSchemaCache.isEnabled();
    this.segmentSchemaCache = segmentSchemaCache;
    this.useCompactionStateCache = compactionStateCache.isEnabled();
    this.compactionStateCache = compactionStateCache;
    this.connector = connector;
    this.pollExecutor = isEnabled()
                        ? MoreExecutors.listeningDecorator(executorFactory.create(1, "SegmentMetadataCache-%s"))
                        : null;
    this.emitter = emitter;
  }


  @Override
  public void start()
  {
    if (!isEnabled()) {
      log.info("Segment metadata cache is not enabled.");
      return;
    }

    synchronized (cacheStateLock) {
      if (currentCacheState == CacheState.STOPPED) {
        updateCacheState(CacheState.FOLLOWER, "Scheduling sync with metadata store");
      }

      if (cacheMode == UsageMode.ALWAYS) {
        // Cache must always be used, do not finish startup until cache has synced
        performFirstSync();
      }

      scheduleSyncWithMetadataStore(pollDuration.getMillis());
    }
  }

  private void performFirstSync()
  {
    try {
      log.info("Cache is in usage mode[%s]. Starting first sync with metadata store.", cacheMode);

      final long syncDurationMillis = syncWithMetadataStore();
      emitMetric(Metric.SYNC_DURATION_MILLIS, syncDurationMillis);
      log.info("Finished first sync of cache with metadata store in [%d] millis.", syncDurationMillis);
    }
    catch (Throwable t) {
      throw InternalServerError.exception(t, "Could not sync segment metadata cache with metadata store");
    }
  }

  @Override
  public void stop()
  {
    synchronized (cacheStateLock) {
      if (isEnabled()) {
        pollExecutor.shutdownNow();
        datasourceToSegmentCache.forEach((datasource, cache) -> cache.stop());
        datasourceToSegmentCache.clear();
        datasourcesSnapshot.set(null);
        if (useCompactionStateCache) {
          compactionStateCache.clear();
        }
        syncFinishTime.set(null);

        updateCacheState(CacheState.STOPPED, "Stopped sync with metadata store");
      }
    }
  }

  @Override
  public void becomeLeader()
  {
    synchronized (cacheStateLock) {
      if (isEnabled()) {
        if (currentCacheState == CacheState.STOPPED) {
          throw DruidException.defensive("Cache has not been started yet");
        } else if (currentCacheState == CacheState.FOLLOWER) {
          updateCacheState(CacheState.LEADER_FIRST_SYNC_PENDING, "We are now leader");

          // Cancel the current sync so that a fresh one is scheduled and cache becomes ready sooner
          if (nextSyncFuture != null && !nextSyncFuture.isDone()) {
            nextSyncFuture.cancel(true);
          }
        } else {
          log.info("We are already the leader. Cache is in state[%s].", currentCacheState);
        }
      }
    }
  }

  @Override
  public void stopBeingLeader()
  {
    synchronized (cacheStateLock) {
      if (isEnabled()) {
        updateCacheState(CacheState.FOLLOWER, "Not leader anymore");
      }
    }
  }

  @Override
  public boolean isEnabled()
  {
    return cacheMode != UsageMode.NEVER;
  }

  @Override
  public boolean isSyncedForRead()
  {
    return isEnabled() && isCacheReady.get();
  }

  @Override
  public DataSourcesSnapshot getDataSourcesSnapshot()
  {
    verifyCacheIsUsableAndAwaitSyncIf(isEnabled());
    return datasourcesSnapshot.get();
  }

  @Override
  public void awaitNextSync(long timeoutMillis)
  {
    final DateTime lastSyncTime = syncFinishTime.get();
    final Supplier<Boolean> lastSyncTimeIsNotUpdated =
        () -> Objects.equals(syncFinishTime.get(), lastSyncTime);

    waitForCacheToFinishSyncWhile(lastSyncTimeIsNotUpdated, timeoutMillis);
  }

  @Override
  public <T> T readCacheForDataSource(String dataSource, Action<T> readAction)
  {
    verifyCacheIsUsableAndAwaitSyncIf(isEnabled());
    try (final HeapMemoryDatasourceSegmentCache datasourceCache = getCacheWithReference(dataSource)) {
      return datasourceCache.withReadLock(
          () -> {
            try {
              return readAction.perform(datasourceCache);
            }
            catch (Exception e) {
              Throwables.throwIfUnchecked(e);
              throw new RuntimeException(e);
            }
          }
      );
    }
  }

  @Override
  public <T> T writeCacheForDataSource(String dataSource, Action<T> writeAction)
  {
    verifyCacheIsUsableAndAwaitSyncIf(cacheMode == UsageMode.ALWAYS);
    try (final HeapMemoryDatasourceSegmentCache datasourceCache = getCacheWithReference(dataSource)) {
      return datasourceCache.withWriteLock(
          () -> {
            try {
              return writeAction.perform(datasourceCache);
            }
            catch (Exception e) {
              Throwables.throwIfUnchecked(e);
              throw new RuntimeException(e);
            }
          }
      );
    }
  }

  /**
   * Returns the (existing or new) cache instance for the given datasource and
   * acquires a single reference to it, which must be closed after the cache
   * has been read or updated.
   */
  private HeapMemoryDatasourceSegmentCache getCacheWithReference(String dataSource)
  {
    return datasourceToSegmentCache.compute(
        dataSource,
        (ds, existingCache) -> {
          final HeapMemoryDatasourceSegmentCache newCache
              = existingCache == null ? new HeapMemoryDatasourceSegmentCache(ds) : existingCache;
          newCache.acquireReference();
          return newCache;
        }
    );
  }

  /**
   * Returns the (existing or new) cache instance for the given datasource.
   * Similar to {@link #getCacheWithReference} but does not acquire references
   * that need to be closed. This method should be called only by the sync thread.
   */
  private HeapMemoryDatasourceSegmentCache getCacheForDatasource(String dataSource)
  {
    return datasourceToSegmentCache.computeIfAbsent(dataSource, HeapMemoryDatasourceSegmentCache::new);
  }

  /**
   * Verifies that the cache is enabled, started and has become leader.
   * Also waits for the cache to be synced with the metadata store after becoming
   * leader if {@code shouldWait} is true.
   *
   * @throws DruidException if the cache is disabled, stopped or not leader.
   */
  private void verifyCacheIsUsableAndAwaitSyncIf(boolean shouldWait)
  {
    if (!isEnabled()) {
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
          if (shouldWait) {
            waitForCacheToFinishSyncWhile(this::isLeaderSyncPending, READY_TIMEOUT_MILLIS);
            verifyCacheIsUsableAndAwaitSyncIf(true);
          }
        case LEADER_READY:
          // Cache is now ready for use
      }
    }
  }

  private boolean isLeaderSyncPending()
  {
    synchronized (cacheStateLock) {
      return currentCacheState == CacheState.LEADER_FIRST_SYNC_PENDING
             || currentCacheState == CacheState.LEADER_FIRST_SYNC_STARTED;
    }
  }

  /**
   * Waits for cache to finish sync as long as the wait condition is true, or
   * until the timeout elapses.
   */
  private void waitForCacheToFinishSyncWhile(Supplier<Boolean> waitCondition, long timeoutMillis)
  {
    final Stopwatch totalWaitTime = Stopwatch.createStarted();

    synchronized (cacheStateLock) {
      log.info("Waiting for cache to finish sync with metadata store.");
      while (waitCondition.get() && totalWaitTime.millisElapsed() <= timeoutMillis) {
        try {
          cacheStateLock.wait(timeoutMillis);
        }
        catch (InterruptedException e) {
          log.noStackTrace().info(e, "Interrupted while waiting for cache to be ready");
          throw DruidException.defensive(e, "Interrupted while waiting for cache to be ready");
        }
        catch (Exception e) {
          log.error(e, "Error while waiting for cache to be ready");
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

      isCacheReady.set(currentCacheState == CacheState.LEADER_READY);
      notifyThreadsWaitingOnCacheSync();
    }
  }

  private void notifyThreadsWaitingOnCacheSync()
  {
    synchronized (cacheStateLock) {
      cacheStateLock.notifyAll();
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
                } else {
                  notifyThreadsWaitingOnCacheSync();
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
   * <li>Retrieve all used segment IDs along with their updated timestamps.</li>
   * <li>Sync segment IDs in the cache with the retrieved segment IDs.</li>
   * <li>Retrieve payloads of used segments which have been updated in the metadata
   * store but not in the cache.</li>
   * <li>Retrieve all pending segments and update the cache as needed.</li>
   * <li>If schema caching is enabled, retrieve segment schemas and reset them
   * in the {@link SegmentSchemaCache}.</li>
   * <li>Clean up entries for datasources which have no segments in the cache anymore.</li>
   * <li>Change the cache state to ready if it is leader and waiting for first sync.</li>
   * <li>Emit metrics</li>
   * </ul>
   * </p>
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

    // Fetch all used segments if this is the first sync
    if (syncFinishTime.get() == null) {
      retrieveAllUsedSegments(datasourceToSummary);
    } else {
      retrieveUsedSegmentIds(datasourceToSummary);
      updateSegmentIdsInCache(datasourceToSummary, syncStartTime.minus(SYNC_BUFFER_DURATION));
      retrieveUsedSegmentPayloads(datasourceToSummary);
    }

    updateUsedSegmentPayloadsInCache(datasourceToSummary);
    retrieveAllPendingSegments(datasourceToSummary);
    updatePendingSegmentsInCache(datasourceToSummary, syncStartTime.minus(SYNC_BUFFER_DURATION));

    if (useSchemaCache) {
      retrieveAndResetUsedSegmentSchemas(datasourceToSummary);
    }

    if (useCompactionStateCache) {
      retrieveAndResetUsedCompactionStates();
    }

    markCacheSynced(syncStartTime);

    syncFinishTime.set(DateTimes.nowUtc());
    return totalSyncDuration.millisElapsed();
  }

  /**
   * Marks the cache for all datasources as synced and emit total stats.
   */
  private void markCacheSynced(DateTime syncStartTime)
  {
    final Stopwatch updateDuration = Stopwatch.createStarted();

    final Set<String> cachedDatasources = Set.copyOf(datasourceToSegmentCache.keySet());
    final Map<String, Set<DataSegment>> datasourceToUsedSegments = new HashMap<>();

    for (String dataSource : cachedDatasources) {
      final HeapMemoryDatasourceSegmentCache cache = datasourceToSegmentCache.getOrDefault(
          dataSource,
          new HeapMemoryDatasourceSegmentCache(dataSource)
      );
      final CacheStats stats = cache.markCacheSynced();

      if (cache.isEmpty()) {
        // If the cache is empty and not currently in use, remove it from the map
        datasourceToSegmentCache.compute(
            dataSource,
            (ds, existingCache) -> {
              if (existingCache != null && existingCache.isEmpty()
                  && !existingCache.isBeingUsedByTransaction()) {
                emitMetric(dataSource, Metric.DELETED_DATASOURCES, 1L);
                return null;
              } else {
                return existingCache;
              }
            }
        );
      } else {
        emitMetric(dataSource, Metric.CACHED_INTERVALS, stats.getNumIntervals());
        emitMetric(dataSource, Metric.CACHED_USED_SEGMENTS, stats.getNumUsedSegments());
        emitMetric(dataSource, Metric.CACHED_UNUSED_SEGMENTS, stats.getNumUnusedSegments());
        emitMetric(dataSource, Metric.CACHED_PENDING_SEGMENTS, stats.getNumPendingSegments());

        datasourceToUsedSegments.put(dataSource, cache.findUsedSegmentsOverlappingAnyOf(List.of()));
      }
    }

    datasourcesSnapshot.set(
        DataSourcesSnapshot.fromUsedSegments(datasourceToUsedSegments, syncStartTime)
    );
    emitMetric(Metric.UPDATE_SNAPSHOT_DURATION_MILLIS, updateDuration.millisElapsed());
  }

  /**
   * Retrieves all used segment IDs from the metadata store.
   * Populates the summary for the datasources found in metadata store.
   */
  private void retrieveUsedSegmentIds(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final Stopwatch retrieveDuration = Stopwatch.createStarted();
    final String sql = StringUtils.format(
        "SELECT id, dataSource, used_status_last_updated FROM %s WHERE used = true",
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
    datasourceToSummary.forEach(
        (dataSource, summary) ->
            emitMetric(dataSource, Metric.PERSISTED_USED_SEGMENTS, summary.persistedSegments.size())
    );
    emitMetric(Metric.RETRIEVE_SEGMENT_IDS_DURATION_MILLIS, retrieveDuration.millisElapsed());
  }

  private <T> T query(Function<SqlSegmentsMetadataQuery, T> sqlFunction)
  {
    return inReadOnlyTransaction(
        (handle, status) -> sqlFunction.apply(
            SqlSegmentsMetadataQuery
                .forHandle(handle, connector, tablesConfig, jsonMapper)
        )
    );
  }

  private <T> T inReadOnlyTransaction(TransactionCallback<T> callback)
  {
    return connector.retryReadOnlyTransaction(callback, SQL_QUIET_RETRIES, SQL_MAX_RETRIES);
  }

  /**
   * Retrieves the payloads of required used segments from the metadata store
   * and updates the cache. A segment needs to be refreshed only if
   * {@link HeapMemoryDatasourceSegmentCache.SegmentsInInterval#shouldRefreshSegment}
   * returns true for it.
   */
  private void retrieveRequiredUsedSegments(
      String dataSource,
      DatasourceSegmentSummary summary
  )
  {
    final Set<SegmentId> segmentIdsToRefresh = summary.usedSegmentIdsToRefresh;
    if (segmentIdsToRefresh.isEmpty()) {
      return;
    }

    inReadOnlyTransaction((handle, status) -> {
      try (
          CloseableIterator<DataSegmentPlus> iterator =
              SqlSegmentsMetadataQuery
                  .forHandle(handle, connector, tablesConfig, jsonMapper)
                  .retrieveSegmentsByIdIterator(dataSource, segmentIdsToRefresh, useSchemaCache)
      ) {
        iterator.forEachRemaining(summary.usedSegments::add);
        return 0;
      }
    });
  }

  /**
   * Updates the cache of each datasource with the segment IDs fetched from the
   * metadata store in {@link #retrieveUsedSegmentIds}. The update done on each
   * datasource cache is atomic. Also identifies the segment IDs which have been
   * updated in the metadata store and need to be refreshed in the cache.
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
   * {@link HeapMemoryDatasourceSegmentCache.SegmentsInInterval#shouldRefreshSegment}
   * returns true for it.
   */
  private void retrieveUsedSegmentPayloads(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final Stopwatch retrieveDuration = Stopwatch.createStarted();
    datasourceToSummary.forEach(this::retrieveRequiredUsedSegments);
    emitMetric(Metric.RETRIEVE_SEGMENT_PAYLOADS_DURATION_MILLIS, retrieveDuration.millisElapsed());
  }

  /**
   * Retrieves all used segments from the metadata store.
   * This method is called only on the first full sync.
   */
  private void retrieveAllUsedSegments(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final Stopwatch retrieveDuration = Stopwatch.createStarted();
    final String sql;
    if (useSchemaCache) {
      sql = StringUtils.format(
          "SELECT id, payload, created_date, used_status_last_updated, compaction_state_fingerprint, schema_fingerprint, num_rows"
          + " FROM %s WHERE used = true",
          tablesConfig.getSegmentsTable()
      );
    } else {
      sql = StringUtils.format(
          "SELECT id, payload, created_date, used_status_last_updated, compaction_state_fingerprint"
          + " FROM %s WHERE used = true",
          tablesConfig.getSegmentsTable()
      );
    }

    final int numSkippedSegments = inReadOnlyTransaction((handle, status) -> {
      try (
          ResultIterator<DataSegmentPlus> iterator =
              handle.createQuery(sql)
                    .setFetchSize(connector.getStreamingFetchSize())
                    .map((index, r, ctx) -> mapToSegmentPlus(r))
                    .iterator()
      ) {
        int skippedRecords = 0;
        while (iterator.hasNext()) {
          final DataSegmentPlus segment = iterator.next();
          if (segment == null) {
            ++skippedRecords;
          } else {
            datasourceToSummary.computeIfAbsent(
                segment.getDataSegment().getDataSource(),
                ds -> new DatasourceSegmentSummary()
            ).usedSegments.add(segment);
          }
        }

        return skippedRecords;
      }
    });

    // Emit metrics
    if (numSkippedSegments > 0) {
      emitMetric(Metric.SKIPPED_SEGMENTS, numSkippedSegments);
    }
    datasourceToSummary.forEach(
        (dataSource, summary) ->
            emitMetric(dataSource, Metric.PERSISTED_USED_SEGMENTS, summary.usedSegments.size())
    );
    emitMetric(Metric.RETRIEVE_SEGMENT_PAYLOADS_DURATION_MILLIS, retrieveDuration.millisElapsed());
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
                log.error(
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

  /**
   * Retrieves required used segment schemas from the metadata store and resets
   * them in the {@link SegmentSchemaCache}. If this is the first sync, all used
   * schemas are retrieved from the metadata store. If this is a delta sync,
   * first only the fingerprints of all used schemas are retrieved. Payloads are
   * then fetched for only the fingerprints which are not present in the cache.
   */
  private void retrieveAndResetUsedSegmentSchemas(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final Stopwatch schemaSyncDuration = Stopwatch.createStarted();

    // Reset the SegmentSchemaCache with latest schemas and metadata
    final Map<String, SchemaPayload> schemaFingerprintToPayload;
    if (syncFinishTime.get() == null) {
      schemaFingerprintToPayload = buildSchemaFingerprintToPayloadMapForFullSync();
    } else {
      schemaFingerprintToPayload = buildSchemaFingerprintToPayloadMapForDeltaSync();
    }

    segmentSchemaCache.resetSchemaForPublishedSegments(
        buildSegmentIdToMetadataMapForSync(datasourceToSummary),
        schemaFingerprintToPayload
    );

    // Emit metrics for the current contents of the cache
    segmentSchemaCache.getStats().forEach(this::emitMetric);
    emitMetric(Metric.RETRIEVE_SEGMENT_SCHEMAS_DURATION_MILLIS, schemaSyncDuration.millisElapsed());
  }

  /**
   * Retrieves all used segment schemas from the metadata store and builds a
   * fresh map from schema fingerprint to payload.
   */
  private Map<String, SchemaPayload> buildSchemaFingerprintToPayloadMapForFullSync()
  {
    final List<SegmentSchemaRecord> records = query(
        SqlSegmentsMetadataQuery::retrieveAllUsedSegmentSchemas
    );

    return records.stream().collect(
        Collectors.toMap(
            SegmentSchemaRecord::getFingerprint,
            SegmentSchemaRecord::getPayload
        )
    );
  }

  /**
   * Retrieves segment schemas from the metadata store if they are not present
   * in the cache or have been recently updated in the metadata store. These
   * segment schemas along with those already present in the cache are used to
   * build a complete udpated map from schema fingerprint to payload.
   *
   * @return Complete updated map from schema fingerprint to payload for all
   * used segment schemas currently persisted in the metadata store.
   */
  private Map<String, SchemaPayload> buildSchemaFingerprintToPayloadMapForDeltaSync()
  {
    // Identify fingerprints in the cache and in the metadata store
    final Map<String, SchemaPayload> schemaFingerprintToPayload = new HashMap<>(
        segmentSchemaCache.getPublishedSchemaPayloadMap()
    );
    final Set<String> cachedFingerprints = Set.copyOf(schemaFingerprintToPayload.keySet());
    final Set<String> persistedFingerprints = query(
        SqlSegmentsMetadataQuery::retrieveAllUsedSegmentSchemaFingerprints
    );

    // Remove entry for schemas that have been deleted from the metadata store
    final Set<String> deletedFingerprints = Sets.difference(cachedFingerprints, persistedFingerprints);
    deletedFingerprints.forEach(schemaFingerprintToPayload::remove);

    // Retrieve and add entry for schemas that have been added to the metadata store
    final Set<String> addedFingerprints = Sets.difference(persistedFingerprints, cachedFingerprints);
    final List<SegmentSchemaRecord> addedSegmentSchemaRecords = query(
        sql -> sql.retrieveUsedSegmentSchemasForFingerprints(addedFingerprints)
    );
    if (addedSegmentSchemaRecords.size() < addedFingerprints.size()) {
      emitMetric(Metric.SKIPPED_SEGMENT_SCHEMAS, addedFingerprints.size() - addedSegmentSchemaRecords.size());
    }
    addedSegmentSchemaRecords.forEach(
        schema -> schemaFingerprintToPayload.put(schema.getFingerprint(), schema.getPayload())
    );

    return schemaFingerprintToPayload;
  }

  /**
   * Builds a map from {@link SegmentId} to {@link SegmentMetadata} for all used
   * segments currently present in the metadata store based on the current sync.
   */
  private Map<SegmentId, SegmentMetadata> buildSegmentIdToMetadataMapForSync(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final Map<SegmentId, SegmentMetadata> cachedSegmentIdToMetadata =
        segmentSchemaCache.getPublishedSegmentMetadataMap();

    final Map<SegmentId, SegmentMetadata> syncedSegmentIdToMetadataMap = new HashMap<>(
        cachedSegmentIdToMetadata
    );

    // Remove entry for segments not present in datasource cache (now synced with the metadata store)
    cachedSegmentIdToMetadata.keySet().forEach(segmentId -> {
      final DataSegment cachedSegment =
          getCacheForDatasource(segmentId.getDataSource()).findUsedSegment(segmentId);
      if (cachedSegment == null) {
        syncedSegmentIdToMetadataMap.remove(segmentId);
      }
    });

    // Add entry for segments that have been added to the datasource cache
    datasourceToSummary.values().forEach(summary -> {
      summary.usedSegments.forEach(segment -> {
        if (segment.getNumRows() != null && segment.getSchemaFingerprint() != null) {
          syncedSegmentIdToMetadataMap.put(
              segment.getDataSegment().getId(),
              new SegmentMetadata(segment.getNumRows(), segment.getSchemaFingerprint())
          );
        }
      });
    });

    return syncedSegmentIdToMetadataMap;
  }

  /**
   * Tries to parse the fields of the result set into a {@link DataSegmentPlus}.
   *
   * @return null if an error occurred while parsing the result
   */
  @Nullable
  private DataSegmentPlus mapToSegmentPlus(ResultSet resultSet)
  {
    String segmentId = null;
    try {
      segmentId = resultSet.getString(1);
      return new DataSegmentPlus(
          JacksonUtils.readValue(jsonMapper, resultSet.getBytes(2), DataSegment.class),
          DateTimes.of(resultSet.getString(3)),
          SqlSegmentsMetadataQuery.nullAndEmptySafeDate(resultSet.getString(4)),
          true,
          useSchemaCache ? resultSet.getString(6) : null,
          useSchemaCache ? (Long) resultSet.getObject(7) : null,
          null,
          resultSet.getString(5)
      );
    }
    catch (Throwable t) {
      log.error(t, "Could not read segment with ID[%s]", segmentId);
      return null;
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
   * Retrieves required used compaction states from the metadata store and resets
   * them in the {@link CompactionStateCache}. If this is the first sync, all used
   * compaction states are retrieved from the metadata store. If this is a delta sync,
   * first only the fingerprints of all used compaction states are retrieved. Payloads are
   * then fetched for only the fingerprints which are not present in the cache.
   */
  private void retrieveAndResetUsedCompactionStates()
  {
    final Stopwatch compactionStateSyncDuration = Stopwatch.createStarted();

    // Reset the CompactionStateCache with latest compaction states
    final Map<String, CompactionState> fingerprintToStateMap;
    if (syncFinishTime.get() == null) {
      fingerprintToStateMap = buildFingerprintToStateMapForFullSync();
    } else {
      fingerprintToStateMap = buildFingerprintToStateMapForDeltaSync();
    }

    compactionStateCache.resetCompactionStatesForPublishedSegments(fingerprintToStateMap);

    // Emit metrics for the current contents of the cache
    compactionStateCache.getAndResetStats().forEach(this::emitMetric);
    emitMetric(Metric.RETRIEVE_COMPACTION_STATES_DURATION_MILLIS, compactionStateSyncDuration.millisElapsed());
  }

  /**
   * Retrieves all used compaction states from the metadata store and builds a
   * fresh map from compaction state fingerprint to state.
   */
  private Map<String, CompactionState> buildFingerprintToStateMapForFullSync()
  {
    final List<CompactionStateRecord> records = query(
        SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStates
    );

    return records.stream().collect(
        Collectors.toMap(
            CompactionStateRecord::getFingerprint,
            CompactionStateRecord::getState
        )
    );
  }

  /**
   * Retrieves compaction states from the metadata store if they are not present
   * in the cache or have been recently updated in the metadata store. These
   * compaction states along with those already present in the cache are used to
   * build a complete updated map from compaction state fingerprint to state.
   *
   * @return Complete updated map from compaction state fingerprint to state for all
   * used compaction states currently persisted in the metadata store.
   */
  private Map<String, CompactionState> buildFingerprintToStateMapForDeltaSync()
  {
    // Identify fingerprints in the cache and in the metadata store
    final Map<String, CompactionState> fingerprintToStateMap = new HashMap<>(
        compactionStateCache.getPublishedCompactionStateMap()
    );
    final Set<String> cachedFingerprints = Set.copyOf(fingerprintToStateMap.keySet());
    final Set<String> persistedFingerprints = query(
        SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStateFingerprints
    );

    // Remove entry for compaction states that have been deleted from the metadata store
    final Set<String> deletedFingerprints = Sets.difference(cachedFingerprints, persistedFingerprints);
    deletedFingerprints.forEach(fingerprintToStateMap::remove);
    emitMetric(Metric.DELETED_COMPACTION_STATES, deletedFingerprints.size());

    // Retrieve and add entry for compaction states that have been added to the metadata store
    final Set<String> addedFingerprints = Sets.difference(persistedFingerprints, cachedFingerprints);
    final List<CompactionStateRecord> addedCompactionStateRecords = query(
        sql -> sql.retrieveCompactionStatesForFingerprints(addedFingerprints)
    );
    if (addedCompactionStateRecords.size() < addedFingerprints.size()) {
      emitMetric(Metric.SKIPPED_COMPACTION_STATES, addedFingerprints.size() - addedCompactionStateRecords.size());
    }
    addedCompactionStateRecords.forEach(
        record -> fingerprintToStateMap.put(record.getFingerprint(), record.getState())
    );
    emitMetric(Metric.ADDED_COMPACTION_STATES, addedCompactionStateRecords.size());

    return fingerprintToStateMap;
  }

  /**
   * Summary of segments currently present in the metadata store for a single
   * datasource.
   */
  private static class DatasourceSegmentSummary
  {
    final List<SegmentRecord> persistedSegments = new ArrayList<>();
    final List<PendingSegmentRecord> persistedPendingSegments = new ArrayList<>();

    final Set<SegmentId> usedSegmentIdsToRefresh = new HashSet<>();
    final Set<DataSegmentPlus> usedSegments = new HashSet<>();

    private void addSegmentRecord(SegmentRecord record)
    {
      persistedSegments.add(record);
    }

    private void addPendingSegmentRecord(PendingSegmentRecord record)
    {
      persistedPendingSegments.add(record);
    }
  }
}
