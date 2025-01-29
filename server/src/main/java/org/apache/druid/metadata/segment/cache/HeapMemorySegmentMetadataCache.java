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
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
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
import java.util.stream.Collectors;

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

  private final ConcurrentHashMap<String, DatasourceSegmentCache>
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
  public DataSource getDatasource(String dataSource)
  {
    verifyCacheIsReady();
    return getCacheForDatasource(dataSource);
  }

  private DatasourceSegmentCache getCacheForDatasource(String dataSource)
  {
    return datasourceToSegmentCache.computeIfAbsent(dataSource, DatasourceSegmentCache::new);
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
          waitForCacheToFinishWarmup();
          verifyCacheIsReady();
        case READY:
          // Cache is now ready for use
      }
    }
  }

  private void waitForCacheToFinishWarmup()
  {
    synchronized (cacheStateLock) {
      while (currentCacheState == CacheState.SYNC_PENDING
             || currentCacheState == CacheState.SYNC_STARTED) {
        try {
          cacheStateLock.wait();
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
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
    final Stopwatch sincePollStart = Stopwatch.createStarted();

    synchronized (cacheStateLock) {
      if (currentCacheState == CacheState.SYNC_PENDING) {
        log.info("Started sync of latest updates from metadata store.");
        currentCacheState = CacheState.SYNC_STARTED;
      }
    }

    final Map<String, DatasourceSegmentSummary> datasourceToSummary = retrieveAllSegmentIds();
    log.info("Found segments for datasources: %s", datasourceToSummary);

    removeUnknownDatasources(datasourceToSummary);
    datasourceToSummary.forEach(this::removeUnknownSegmentIdsFromCache);

    datasourceToSummary.forEach(
        (datasource, summary) ->
            getCacheForDatasource(datasource)
                .resetMaxUnusedIds(summary.intervalVersionToMaxUnusedPartition)
    );

    datasourceToSummary.forEach(this::retrieveAndRefreshUsedSegments);

    retrieveAndRefreshAllPendingSegments();

    emitMetric("poll/time", sincePollStart.millisElapsed());
    pollFinishTime.set(DateTimes.nowUtc());

    markCacheAsReadyIfLeader();

    // Schedule the next poll
    final long nextPollDelay = Math.max(pollDuration.getMillis() - sincePollStart.millisElapsed(), 0);
    pollExecutor.schedule(this::pollMetadataStore, nextPollDelay, TimeUnit.MILLISECONDS);
  }

  /**
   * Retrieves all the segment IDs (used and unused) from the metadata store.
   *
   * @return Map from datasource name to segment summary.
   */
  private Map<String, DatasourceSegmentSummary> retrieveAllSegmentIds()
  {
    final Map<String, DatasourceSegmentSummary> datasourceToSummary = new HashMap<>();

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
          final DatasourceSegmentCache cache = getCacheForDatasource(record.dataSource);
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
              if (cache.addUnusedSegmentId(segmentId)) {
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

    return datasourceToSummary;
  }

  private void retrieveAndRefreshUsedSegments(
      String dataSource,
      DatasourceSegmentSummary summary
  )
  {
    if (summary.segmentIdsToRefresh.isEmpty()) {
      return;
    }

    final DatasourceSegmentCache cache = getCacheForDatasource(dataSource);
    int numUpdatedUsedSegments = 0;

    try (
        CloseableIterator<DataSegmentPlus> iterator = connector.inReadOnlyTransaction(
            (handle, status) -> SqlSegmentsMetadataQuery
                .forHandle(handle, connector, tablesConfig, jsonMapper)
                .retrieveSegmentsByIdIterator(dataSource, summary.segmentIdsToRefresh)
        )
    ) {
      while (iterator.hasNext()) {
        if (cache.addSegment(iterator.next())) {
          ++numUpdatedUsedSegments;
        }
      }
    }
    catch (IOException e) {
      log.makeAlert(e, "Error retrieving segments for datasource[%s] from metadata store.", dataSource)
         .emit();
    }

    emitDatasourceMetric(dataSource, "refresh/used", numUpdatedUsedSegments);
    if (numUpdatedUsedSegments > 0) {
      log.info(
          "Refreshed [%d] used segments for datasource[%s] from metadata store.",
          numUpdatedUsedSegments, dataSource
      );
    }
  }

  private void retrieveAndRefreshAllPendingSegments()
  {
    final String sql = StringUtils.format(
        "SELECT payload, sequence_name, sequence_prev_id, upgraded_from_segment_id"
        + " task_allocator_id, created_date FROM %1$s",
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
                final DatasourceSegmentCache cache = getCacheForDatasource(record.getId().getDataSource());

                if (cache.shouldRefreshPendingSegment(record)
                    && cache.insertPendingSegment(record, false)) {
                  datasourceToUpdatedCount.merge(record.getId().getDataSource(), 1, Integer::sum);
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "Error retrieving pending segments from metadata store.").emit();
                return 0;
              }

              return 0;
            }).list()
    );
    datasourceToUpdatedCount.forEach(
        (dataSource, updatedCount) ->
            log.info("Refreshed [%d] pending segments for datasource[%s].", updatedCount, dataSource)
    );
  }

  private void removeUnknownDatasources(Map<String, DatasourceSegmentSummary> datasourceToSummary)
  {
    final Set<String> datasourcesNotInMetadataStore =
        datasourceToSegmentCache.keySet()
                                .stream()
                                .filter(ds -> !datasourceToSummary.containsKey(ds))
                                .collect(Collectors.toSet());

    if (!datasourcesNotInMetadataStore.isEmpty()) {
      datasourcesNotInMetadataStore.forEach(datasourceToSegmentCache::remove);
      log.info("Removed unknown datasources[%s] from cache.", datasourcesNotInMetadataStore);
    }
  }

  /**
   * This is safe to do since updates are always made first to metadata store
   * and then to cache.
   */
  private void removeUnknownSegmentIdsFromCache(
      String dataSource,
      DatasourceSegmentSummary summary
  )
  {
    final DatasourceSegmentCache cache = getCacheForDatasource(dataSource);
    final Set<String> unknownSegmentIds = cache.getSegmentIdsNotIn(summary.persistedSegmentIds);
    final int numSegmentsRemoved = cache.removeSegmentIds(unknownSegmentIds);
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
