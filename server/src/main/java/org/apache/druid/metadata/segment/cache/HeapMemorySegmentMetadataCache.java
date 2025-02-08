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

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory implementation of {@link SegmentMetadataCache}.
 *
 * TODO: cancel the future of poll after becoming leader.
 */
@ThreadSafe
public class HeapMemorySegmentMetadataCache implements SegmentMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(HeapMemorySegmentMetadataCache.class);
  private static final String METRIC_PREFIX = "segment/metadataCache/";

  /**
   * Maximum time to wait for cache to be ready.
   */
  private static final int READY_TIMEOUT_MILLIS = 5 * 60_000;

  private enum CacheState
  {
    STOPPED, FOLLOWER, LEADER_FIRST_SYNC_PENDING, LEADER_FIRST_SYNC_STARTED, LEADER_READY
  }

  private final ObjectMapper jsonMapper;
  private final Duration pollDuration;
  private final boolean isCacheEnabled;
  private final MetadataStorageTablesConfig tablesConfig;
  private final SQLMetadataConnector connector;

  private final ScheduledExecutorService pollExecutor;
  private final ServiceEmitter emitter;

  private final Object cacheStateLock = new Object();

  @GuardedBy("cacheStateLock")
  private CacheState currentCacheState = CacheState.STOPPED;

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
    this.pollExecutor = isCacheEnabled ? executorFactory.create(1, "SegmentMetadataCache-%s") : null;
    this.emitter = emitter;
  }


  @Override
  @LifecycleStart
  public void start()
  {
    synchronized (cacheStateLock) {
      if (isCacheEnabled && currentCacheState == CacheState.STOPPED) {
        updateCacheState(CacheState.FOLLOWER, "Starting sync with metadata store");
        pollExecutor.schedule(this::syncWithMetadataStore, pollDuration.getMillis(), TimeUnit.MILLISECONDS);
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
   */
  private void syncWithMetadataStore()
  {
    final DateTime pollStartTime = DateTimes.nowUtc();
    final Stopwatch sincePollStart = Stopwatch.createStarted();
    try {
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

      final long pollDurationMillis = sincePollStart.millisElapsed();
      emitMetric("sync/time", pollDurationMillis);
      syncFinishTime.set(DateTimes.nowUtc());

      synchronized (cacheStateLock) {
        if (currentCacheState == CacheState.LEADER_FIRST_SYNC_STARTED) {
          updateCacheState(CacheState.LEADER_READY, "Cache is synced with metadata store");
        }
      }
    }
    catch (Throwable t) {
      log.makeAlert(t, "Error occurred while polling metadata store").emit();
    }
    finally {
      // Schedule the next sync
      final long nextSyncDelay = Math.max(pollDuration.getMillis() - sincePollStart.millisElapsed(), 0);
      pollExecutor.schedule(this::syncWithMetadataStore, nextSyncDelay, TimeUnit.MILLISECONDS);
    }
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

    connector.inReadOnlyTransaction((handle, status) -> {
      try (
          ResultIterator<SegmentRecord> iterator =
              handle.createQuery(sql)
                    .map((index, r, ctx) -> SegmentRecord.fromResultSet(r))
                    .iterator()
      ) {
        while (iterator.hasNext()) {
          final SegmentRecord record = iterator.next();
          final SegmentId segmentId = record.segmentId;
          final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(record.dataSource);
          final DatasourceSegmentSummary summary = datasourceToSummary
              .computeIfAbsent(record.dataSource, ds -> new DatasourceSegmentSummary());

          // Refresh used segments if required
          if (record.isUsed && cache.shouldRefreshUsedSegment(segmentId, record.lastUpdatedTime)) {
            summary.usedSegmentIdsToRefresh.add(record.segmentId.toString());
          }

          // Track max partition number of unused segment if needed
          if (!record.isUsed) {
            if (cache.addUnusedSegmentId(segmentId, record.lastUpdatedTime)) {
              summary.numUnusedSegmentsRefreshed++;
            }

            final int partitionNum = segmentId.getPartitionNum();
            summary
                .intervalVersionToMaxUnusedPartition
                .computeIfAbsent(segmentId.getInterval(), i -> new HashMap<>())
                .merge(segmentId.getVersion(), partitionNum, Math::max);
          }

          summary.persistedSegmentIds.add(segmentId);
        }

        return 0;
      }
      catch (Exception e) {
        log.makeAlert(e, "Error while retrieving segment IDs from metadata store.");
        return 1;
      }
    });
  }

  private void emitSummaryMetrics(String dataSource, DatasourceSegmentSummary summary)
  {
    emitDatasourceMetric(dataSource, "polled/total", summary.persistedSegmentIds.size());
    emitDatasourceMetric(dataSource, "polled/pending", summary.persistedPendingSegmentIds.size());
    emitDatasourceMetric(dataSource, "stale/used", summary.usedSegmentIdsToRefresh.size());

    emitDatasourceMetric(dataSource, "deleted/total", summary.numSegmentsRemoved);
    emitDatasourceMetric(dataSource, "deleted/pending", summary.numPendingSegmentsRemoved);

    emitDatasourceMetric(dataSource, "updated/used", summary.numUsedSegmentsRefreshed);
    emitDatasourceMetric(dataSource, "updated/unused", summary.numUnusedSegmentsRefreshed);
    emitDatasourceMetric(dataSource, "updated/pending", summary.numPendingSegmentsUpdated);

    final boolean updated =
        summary.numSegmentsRemoved > 0
        || summary.numPendingSegmentsRemoved > 0
        || summary.numUsedSegmentsRefreshed > 0
        || summary.numUnusedSegmentsRefreshed > 0
        || summary.numPendingSegmentsUpdated > 0;
    if (updated) {
      log.info(
          "Refreshed segments for datasource[%s] in cache."
          + " Persisted in metadata store = segments[%d], pending segments[%d]."
          + " Removed from cache = segments[%d], pending segments[%d]."
          + " Updated in cache = used segments[%d], unused segments[%d], pending segments[%d].",
          dataSource,
          summary.persistedSegmentIds.size(), summary.persistedPendingSegmentIds.size(),
          summary.numSegmentsRemoved, summary.numPendingSegmentsRemoved,
          summary.numUsedSegmentsRefreshed, summary.numUnusedSegmentsRefreshed,
          summary.numPendingSegmentsUpdated
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

    summary.numUsedSegmentsRefreshed = connector.inReadOnlyTransaction((handle, status) -> {
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
      catch (IOException e) {
        log.error(e, "Error retrieving segments");
        log.makeAlert(e, "Error retrieving segments for datasource[%s] from metadata store.", dataSource)
           .emit();
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
        "SELECT payload, sequence_name, sequence_prev_id,"
        + " upgraded_from_segment_id, task_allocator_id, created_date FROM %1$s",
        tablesConfig.getPendingSegmentsTable()
    );

    connector.inReadOnlyTransaction(
        (handle, status) -> handle
            .createQuery(sql)
            .setFetchSize(connector.getStreamingFetchSize())
            .map((index, r, ctx) -> {
              try {
                final PendingSegmentRecord record = PendingSegmentRecord.fromResultSet(r, jsonMapper);
                final String dataSource = record.getId().getDataSource();

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
                log.makeAlert(e, "Error retrieving pending segments from metadata store.").emit();
                return 0;
              }

              return 0;
            }).list()
    );
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
        ServiceMetricEvent.builder().setMetric(METRIC_PREFIX + metric, value)
    );
  }

  private void emitDatasourceMetric(String datasource, String metric, long value)
  {
    emitter.emit(
        ServiceMetricEvent.builder()
                          .setDimension(DruidMetrics.DATASOURCE, datasource)
                          .setMetric(METRIC_PREFIX + metric, value)
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

    @Nullable
    static SegmentRecord fromResultSet(ResultSet r)
    {
      try {
        final String serializedId = r.getString("id");
        final boolean isUsed = r.getBoolean("used");
        final String dataSource = r.getString("dataSource");
        final DateTime lastUpdatedTime = nullSafeDate(r.getString("used_status_last_updated"));

        final SegmentId segmentId = SegmentId.tryParse(dataSource, serializedId);
        if (segmentId == null) {
          return null;
        } else {
          return new SegmentRecord(segmentId, dataSource, isUsed, lastUpdatedTime);
        }
      }
      catch (SQLException e) {
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

    int numUsedSegmentsRefreshed;
    int numUnusedSegmentsRefreshed = 0;
    int numSegmentsRemoved = 0;
    int numPendingSegmentsRemoved = 0;
    int numPendingSegmentsUpdated = 0;

    @Override
    public String toString()
    {
      return "DatasourceSegmentSummary{" +
             "persistedSegmentIds=" + persistedSegmentIds +
             ", segmentIdsToRefresh=" + usedSegmentIdsToRefresh +
             ", intervalVersionToMaxUnusedPartition=" + intervalVersionToMaxUnusedPartition +
             '}';
    }
  }

}
