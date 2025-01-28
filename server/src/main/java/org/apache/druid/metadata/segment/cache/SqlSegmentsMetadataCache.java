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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataCache implements SegmentsMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(SqlSegmentsMetadataCache.class);
  private static final String METRIC_PREFIX = "segment/metadataCache/";

  private enum CacheState
  {
    STOPPED, STARTING, READY
  }

  private final ObjectMapper jsonMapper;
  private final Duration pollDuration;
  private final boolean isCacheEnabled;
  private final Supplier<MetadataStorageTablesConfig> tablesConfig;
  private final SQLMetadataConnector connector;

  private final ScheduledExecutorService pollExecutor;
  private final ServiceEmitter emitter;

  private final AtomicReference<CacheState> currentCacheState
      = new AtomicReference<>(CacheState.STOPPED);

  private final ConcurrentHashMap<String, DatasourceSegmentCache>
      datasourceToSegmentCache = new ConcurrentHashMap<>();
  private final AtomicReference<DateTime> pollFinishTime = new AtomicReference<>();

  @Inject
  public SqlSegmentsMetadataCache(
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
    this.tablesConfig = tablesConfig;
    this.connector = connector;
    this.pollExecutor = isCacheEnabled ? executorFactory.create(1, "SegmentsMetadataCache-%s") : null;
    this.emitter = emitter;
  }


  @Override
  @LifecycleStart
  public synchronized void start()
  {
    if (isCacheEnabled && currentCacheState.compareAndSet(CacheState.STOPPED, CacheState.STARTING)) {
      // Clean up any stray entries in the cache left over due to race conditions
      tearDown();
      pollExecutor.schedule(this::pollMetadataStore, pollDuration.getMillis(), TimeUnit.MILLISECONDS);
    }
  }

  /**
   * This method is called only when leadership is lost or when the service is
   * being stopped. Any transaction that is in progress when this method is
   * invoked will fail.
   */
  @Override
  @LifecycleStop
  public synchronized void stop()
  {
    if (isCacheEnabled) {
      currentCacheState.set(CacheState.STOPPED);
      tearDown();
    }
  }

  @Override
  public boolean isReady()
  {
    return currentCacheState.get() == CacheState.READY;
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
    if (!isReady()) {
      throw DruidException.defensive("Segment metadata cache is not ready yet.");
    }
  }

  private boolean isStopped()
  {
    return currentCacheState.get() == CacheState.STOPPED;
  }

  private void tearDown()
  {
    datasourceToSegmentCache.forEach((datasource, state) -> state.clear());
    datasourceToSegmentCache.clear();
  }

  private void pollMetadataStore()
  {
    final Stopwatch sincePollStart = Stopwatch.createStarted();
    if (isStopped()) {
      tearDown();
      return;
    }

    final Map<String, DatasourceSegmentSummary> datasourceToSummary = retrieveAllSegmentIds();

    if (isStopped()) {
      tearDown();
      return;
    }

    removeUnknownDatasources(datasourceToSummary);
    datasourceToSummary.forEach(this::removeUnknownSegmentIdsFromCache);
    datasourceToSummary.forEach(
        (datasource, summary) ->
            getCacheForDatasource(datasource)
                .resetMaxUnusedIds(summary.intervalVersionToMaxUnusedPartition)
    );

    if (isStopped()) {
      tearDown();
      return;
    }

    final int countOfRefreshedUsedSegments = datasourceToSummary.entrySet().stream().mapToInt(
        entry -> retrieveAndRefreshUsedSegments(
            entry.getKey(),
            entry.getValue().segmentIdsToRefresh
        )
    ).sum();
    if (countOfRefreshedUsedSegments > 0) {
      log.info(
          "Refreshed total [%d] used segments from metadata store.",
          countOfRefreshedUsedSegments
      );
    }

    if (isStopped()) {
      tearDown();
      return;
    }

    retrieveAndRefreshAllPendingSegments();

    emitMetric("poll/time", sincePollStart.millisElapsed());
    pollFinishTime.set(DateTimes.nowUtc());

    if (isStopped()) {
      tearDown();
    } else {
      currentCacheState.compareAndSet(CacheState.STARTING, CacheState.READY);

      // Schedule the next poll
      final long nextPollDelay = Math.max(pollDuration.getMillis() - sincePollStart.millisElapsed(), 0);
      pollExecutor.schedule(this::pollMetadataStore, nextPollDelay, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Retrieves all the segment IDs (used and unused) from the metadata store.
   *
   * @return Map from datasource name to segment summary.
   */
  private Map<String, DatasourceSegmentSummary> retrieveAllSegmentIds()
  {
    final Map<String, DatasourceSegmentSummary> datasourceToSummary = new HashMap<>();
    final AtomicInteger countOfRefreshedUnusedSegments = new AtomicInteger(0);

    // TODO: Consider improving this because the number of unused segments can be very large
    //  Instead of polling all segments, we could just poll the used segments
    //  and then fire a smarter query to determine the max unused ID or something
    //  But it might be tricky

    final String sql = StringUtils.format(
        "SELECT id, dataSource, used, used_status_last_updated FROM %s",
        getSegmentsTable()
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
                countOfRefreshedUnusedSegments.incrementAndGet();
                emitDatasourceMetric(record.dataSource, "refreshed/unused", 1);
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

    if (countOfRefreshedUnusedSegments.get() > 0) {
      log.info("Refreshed total [%d] unused segments from metadata store.", countOfRefreshedUnusedSegments.get());
    }

    return datasourceToSummary;
  }

  private int retrieveAndRefreshUsedSegments(
      String dataSource,
      Set<String> segmentIdsToRefresh
  )
  {
    final DatasourceSegmentCache cache = getCacheForDatasource(dataSource);
    int numUpdatedUsedSegments = 0;
    try (
        CloseableIterator<DataSegmentPlus> iterator = connector.inReadOnlyTransaction(
            (handle, status) -> SqlSegmentsMetadataQuery
                .forHandle(handle, connector, tablesConfig.get(), jsonMapper)
                .retrieveSegmentsByIdIterator(dataSource, segmentIdsToRefresh)
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
    return numUpdatedUsedSegments;
  }

  private void retrieveAndRefreshAllPendingSegments()
  {
    final String sql = StringUtils.format(
        "SELECT payload, sequence_name, sequence_prev_id, upgraded_from_segment_id"
        + " task_allocator_id, created_date FROM %1$s",
        tablesConfig.get().getPendingSegmentsTable()
    );

    connector.inReadOnlyTransaction(
        (handle, status) -> handle
            .createQuery(sql)
            .setFetchSize(connector.getStreamingFetchSize())
            .map((index, r, ctx) -> {
              try {
                final PendingSegmentRecord record = PendingSegmentRecord.fromResultSet(r, jsonMapper);
                final DatasourceSegmentCache cache = getCacheForDatasource(record.getId().getDataSource());

                if (cache.shouldRefreshPendingSegment(record)) {
                  cache.insertPendingSegment(record, false);
                }

                return 0;
              }
              catch (Exception e) {
                return 1;
              }
            })
    );
  }

  private void removeUnknownDatasources(Map<String, DatasourceSegmentSummary> datasourceToSummary)
  {
    final Set<String> datasourcesNotInMetadataStore =
        datasourceToSegmentCache.keySet()
                                .stream()
                                .filter(ds -> !datasourceToSummary.containsKey(ds))
                                .collect(Collectors.toSet());

    datasourcesNotInMetadataStore.forEach(datasourceToSegmentCache::remove);
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

  private String getSegmentsTable()
  {
    return tablesConfig.get().getSegmentsTable();
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
  }

}
