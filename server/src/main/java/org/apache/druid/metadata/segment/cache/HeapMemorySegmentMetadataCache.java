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
 */
@ThreadSafe
public class HeapMemorySegmentMetadataCache implements SegmentMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(HeapMemorySegmentMetadataCache.class);
  private static final String METRIC_PREFIX = "segment/metadataCache/";

  private enum CacheState
  {
    STOPPED, STANDBY, SYNC_PENDING, SYNC_STARTED, READY
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
  private volatile CacheState currentCacheState = CacheState.STOPPED;

  private final ConcurrentHashMap<String, HeapMemoryDatasourceSegmentCache>
      datasourceToSegmentCache = new ConcurrentHashMap<>();
  private final AtomicReference<DateTime> pollFinishTime = new AtomicReference<>();

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
        log.info("Starting poll of metadata store. Cache is now in STANDBY mode.");
        pollExecutor.schedule(this::pollMetadataStore, pollDuration.getMillis(), TimeUnit.MILLISECONDS);
        currentCacheState = CacheState.STANDBY;
      }
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    synchronized (cacheStateLock) {
      if (isCacheEnabled) {
        log.info("Stopping poll of metadata store. Cache is now STOPPED.");
        currentCacheState = CacheState.STOPPED;
        pollExecutor.shutdownNow();
        tearDown();
      }
    }
  }

  @Override
  public void becomeLeader()
  {
    synchronized (cacheStateLock) {
      if (isCacheEnabled) {
        log.info("We are now leader. Waiting to sync latest updates from metadata store.");
        currentCacheState = CacheState.SYNC_PENDING;
      }
    }
  }

  @Override
  public void stopBeingLeader()
  {
    synchronized (cacheStateLock) {
      if (isCacheEnabled) {
        log.info("Not leader anymore. Cache is now in STANDBY mode.");
        currentCacheState = CacheState.STANDBY;
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
    verifyCacheIsReady();
    return getCacheForDatasource(dataSource);
  }

  private HeapMemoryDatasourceSegmentCache getCacheForDatasource(String dataSource)
  {
    return datasourceToSegmentCache.computeIfAbsent(dataSource, HeapMemoryDatasourceSegmentCache::new);
  }

  private void verifyCacheIsReady()
  {
    synchronized (cacheStateLock) {
      switch (currentCacheState) {
        case STOPPED:
        case STANDBY:
          throw DruidException.defensive("Segment metadata cache has not been started.");
        case SYNC_PENDING:
        case SYNC_STARTED:
          waitForCacheToFinishSync();
          verifyCacheIsReady();
        case READY:
          // Cache is now ready for use
      }
    }
  }

  private void waitForCacheToFinishSync()
  {
    synchronized (cacheStateLock) {
      log.info("Waiting for cache to finish sync with metadata store.");
      while (currentCacheState == CacheState.SYNC_PENDING
             || currentCacheState == CacheState.SYNC_STARTED) {
        try {
          cacheStateLock.wait();
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      log.info("Wait complete. Cache is now in state[%s].", currentCacheState);
    }
  }

  private void markCacheAsReadyIfLeader()
  {
    synchronized (cacheStateLock) {
      if (currentCacheState == CacheState.SYNC_STARTED) {
        log.info("Sync has finished. Cache is now READY to serve requests.");
        currentCacheState = CacheState.READY;

        // State has changed from STARTING to READY, notify waiting threads
        cacheStateLock.notifyAll();
      }
    }
  }

  private void tearDown()
  {
    datasourceToSegmentCache.forEach((datasource, state) -> state.clear());
    datasourceToSegmentCache.clear();
  }

  private void pollMetadataStore()
  {
    final DateTime pollStartTime = DateTimes.nowUtc();
    final Stopwatch sincePollStart = Stopwatch.createStarted();
    try {
      synchronized (cacheStateLock) {
        if (currentCacheState == CacheState.SYNC_PENDING) {
          log.info("Started sync of latest updates from metadata store.");
          currentCacheState = CacheState.SYNC_STARTED;
        }
      }

      final Map<String, DatasourceSegmentSummary> datasourceToSummary = new HashMap<>();

      retrieveAllSegmentIds(datasourceToSummary);

      datasourceToSummary.forEach(
          (datasource, summary) ->
              removeUnknownSegmentsFromCache(datasource, summary, pollStartTime)
      );
      datasourceToSummary.forEach(
          (datasource, summary) ->
              getCacheForDatasource(datasource)
                  .resetMaxUnusedIds(summary.intervalVersionToMaxUnusedPartition)
      );
      datasourceToSummary.forEach(this::retrieveAndRefreshUsedSegments);

      retrieveAndRefreshAllPendingSegments(datasourceToSummary);
      datasourceToSummary.forEach(
          (datasource, summary) ->
              removeUnknownPendingSegmentsFromCache(datasource, summary, pollStartTime)
      );

      final long pollDurationMillis = sincePollStart.millisElapsed();
      emitMetric("poll/time", pollDurationMillis);
      pollFinishTime.set(DateTimes.nowUtc());

      markCacheAsReadyIfLeader();
    }
    catch (Throwable t) {
      log.error(t, "Error occurred while polling metadata store");
      log.makeAlert(t, "Error occurred while polling metadata store");
    }
    finally {
      // Schedule the next poll
      final long nextPollDelay = Math.max(pollDuration.getMillis() - sincePollStart.millisElapsed(), 0);
      pollExecutor.schedule(this::pollMetadataStore, nextPollDelay, TimeUnit.MILLISECONDS);
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
          final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(record.dataSource);
          final DatasourceSegmentSummary summary = datasourceToSummary
              .computeIfAbsent(record.dataSource, ds -> new DatasourceSegmentSummary());

          // Refresh used segments if required
          if (record.isUsed && cache.shouldRefreshUsedSegment(record.segmentId, record.lastUpdatedTime)) {
            summary.segmentIdsToRefresh.add(record.segmentId);
          }

          // Track max partition number of unused segment if needed
          if (!record.isUsed) {
            final SegmentId segmentId = SegmentId.tryParse(record.dataSource, record.segmentId);

            if (segmentId != null) {
              if (cache.addUnusedSegmentId(segmentId, record.lastUpdatedTime)) {
                summary.numUnusedSegmentsRefreshed++;
              }

              final int partitionNum = segmentId.getPartitionNum();
              summary
                  .intervalVersionToMaxUnusedPartition
                  .computeIfAbsent(segmentId.getInterval(), i -> new HashMap<>())
                  .merge(segmentId.getVersion(), partitionNum, Math::max);
            }
          }

          summary.persistedSegmentIds.add(record.segmentId);
        }

        return 0;
      }
      catch (Exception e) {
        log.makeAlert(e, "Error while retrieving segment IDs from metadata store.");
        return 1;
      }
    });
  }

  private void retrieveAndRefreshUsedSegments(
      String dataSource,
      DatasourceSegmentSummary summary
  )
  {
    if (summary.segmentIdsToRefresh.isEmpty()) {
      return;
    }

    final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);

    final int numUpdatedUsedSegments = connector.inReadOnlyTransaction((handle, status) -> {
      int updatedCount = 0;
      try (
          CloseableIterator<DataSegmentPlus> iterator =
              SqlSegmentsMetadataQuery
                  .forHandle(handle, connector, tablesConfig, jsonMapper)
                  .retrieveSegmentsByIdIterator(dataSource, summary.segmentIdsToRefresh)
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

    emitDatasourceMetric(dataSource, "refresh/used", numUpdatedUsedSegments);
    if (numUpdatedUsedSegments > 0) {
      log.info(
          "Refreshed [%d] used segments for datasource[%s] from metadata store.",
          numUpdatedUsedSegments, dataSource
      );
    }
  }

  private void retrieveAndRefreshAllPendingSegments(
      Map<String, DatasourceSegmentSummary> datasourceToSummary
  )
  {
    final String sql = StringUtils.format(
        "SELECT payload, sequence_name, sequence_prev_id,"
        + " upgraded_from_segment_id, task_allocator_id, created_date FROM %1$s",
        tablesConfig.getPendingSegmentsTable()
    );

    final Map<String, Integer> datasourceToUpdatedCount = new HashMap<>();
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

                datasourceToUpdatedCount.merge(
                    dataSource,
                    updated ? 1 : 0,
                    Integer::sum
                );
                datasourceToSummary
                    .computeIfAbsent(dataSource, ds -> new DatasourceSegmentSummary())
                    .persistedPendingSegmentIds
                    .add(record.getId().toString());
              }
              catch (Exception e) {
                log.makeAlert(e, "Error retrieving pending segments from metadata store.").emit();
                return 0;
              }

              return 0;
            }).list()
    );
    datasourceToUpdatedCount.forEach((dataSource, updatedCount) -> {
      if (updatedCount > 0) {
        log.info("Refreshed [%d] pending segments for datasource[%s].", updatedCount, dataSource);
      }
    });
  }

  private void removeUnknownPendingSegmentsFromCache(
      final String dataSource,
      final DatasourceSegmentSummary summary,
      final DateTime pollStartTime
  )
  {
    final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);
    final int numSegmentsRemoved = cache.removeUnpersistedPendingSegments(
        summary.persistedPendingSegmentIds,
        pollStartTime
    );
    if (numSegmentsRemoved > 0) {
      log.info(
          "Removed [%d] unknown pending segments from cache of datasource[%s].",
          numSegmentsRemoved, dataSource
      );
      emitDatasourceMetric(dataSource, "deleted/unknown", numSegmentsRemoved);
    }
  }

  private void removeUnknownSegmentsFromCache(
      final String dataSource,
      final DatasourceSegmentSummary summary,
      final DateTime pollStartTime
  )
  {
    final HeapMemoryDatasourceSegmentCache cache = getCacheForDatasource(dataSource);
    final int numSegmentsRemoved = cache.removeUnpersistedSegments(summary.persistedSegmentIds, pollStartTime);
    if (numSegmentsRemoved > 0) {
      log.info(
          "Removed [%d] unknown segment IDs from cache of datasource[%s].",
          numSegmentsRemoved, dataSource
      );
      emitDatasourceMetric(dataSource, "deleted/unknown", numSegmentsRemoved);
    }
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

  private static class SegmentRecord
  {
    private final String segmentId;
    private final String dataSource;
    private final boolean isUsed;
    private final DateTime lastUpdatedTime;

    SegmentRecord(String segmentId, String dataSource, boolean isUsed, DateTime lastUpdatedTime)
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
        final String segmentId = r.getString("id");
        final boolean isUsed = r.getBoolean("used");
        final String dataSource = r.getString("dataSource");
        final DateTime lastUpdatedTime = nullSafeDate(r.getString("used_status_last_updated"));

        return new SegmentRecord(segmentId, dataSource, isUsed, lastUpdatedTime);
      }
      catch (SQLException e) {
        return null;
      }
    }
  }

  /**
   * Summary of segments of a datasource currently present in the metadata store.
   */
  private static class DatasourceSegmentSummary
  {
    final Set<String> persistedSegmentIds = new HashSet<>();
    final Set<String> persistedPendingSegmentIds = new HashSet<>();

    final Set<String> segmentIdsToRefresh = new HashSet<>();
    final Map<Interval, Map<String, Integer>> intervalVersionToMaxUnusedPartition = new HashMap<>();
    int numUnusedSegmentsRefreshed = 0;

    @Override
    public String toString()
    {
      return "DatasourceSegmentSummary{" +
             "persistedSegmentIds=" + persistedSegmentIds +
             ", segmentIdsToRefresh=" + segmentIdsToRefresh +
             ", intervalVersionToMaxUnusedPartition=" + intervalVersionToMaxUnusedPartition +
             '}';
    }
  }

}
