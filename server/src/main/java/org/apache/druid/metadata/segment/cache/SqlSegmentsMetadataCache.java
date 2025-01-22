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
import org.apache.druid.metadata.segment.DatasourceSegmentMetadataReader;
import org.apache.druid.metadata.segment.DatasourceSegmentMetadataWriter;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.http.DataSegmentPlus;
import org.joda.time.DateTime;
import org.joda.time.Duration;
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

  @Override
  @LifecycleStop
  public synchronized void stop()
  {
    if (isCacheEnabled) {
      currentCacheState.set(CacheState.STOPPED);
      tearDown();
    }

    // TODO: Handle race conditions
    // T1: sees cache as ready
    // T2: stops the cache
    // T1: tries to read some value from the cache and fails

    // Should start-stop wait on everything else?
    // When does stop happen?
    // 1. Leadership changes: If leadership has changed, no point continuing the operation?
    //    In the current implementation, a task action would continue executing even if leadership has been lost?
    //    Yes, I do think so.
    //    Solution: If leadership has changed, transaction would fail, we wouldn't need to read or write anymore

    // 2. Service start-stop. Again no point worrying about the cache
  }

  @Override
  public boolean isReady()
  {
    return currentCacheState.get() == CacheState.READY;
  }

  @Override
  public DatasourceSegmentMetadataReader readerForDatasource(String dataSource)
  {
    verifyCacheIsReady();
    return datasourceToSegmentCache.getOrDefault(dataSource, DatasourceSegmentCache.empty());
  }

  @Override
  public DatasourceSegmentMetadataWriter writerForDatasource(String dataSource)
  {
    verifyCacheIsReady();
    return datasourceToSegmentCache.computeIfAbsent(dataSource, ds -> new DatasourceSegmentCache());
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

    final Map<String, Set<String>> datasourceToRefreshSegmentIds = new HashMap<>();
    final Map<String, Set<String>> datasourceToKnownSegmentIds
        = retrieveAllSegmentIds(datasourceToRefreshSegmentIds);

    // TODO: handle changes made to the metadata store between these two database calls
    //    there doesn't seem to be much point to lock the cache during this period
    //    so go and fetch the segments and then refresh them
    //    it is possible that the cache is now updated and the refresh is not needed after all
    //    so the refresh should be idempotent
    if (isStopped()) {
      tearDown();
      return;
    }

    removeUnknownSegmentIdsFromCache(datasourceToKnownSegmentIds);

    if (isStopped()) {
      tearDown();
      return;
    }

    retrieveAndRefreshUsedSegmentsForIds(datasourceToRefreshSegmentIds);

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
   * @return Map from datasource name to set of all segment IDs present in the
   * metadata store for that datasource.
   */
  private Map<String, Set<String>> retrieveAllSegmentIds(
      Map<String, Set<String>> datasourceToRefreshSegmentIds
  )
  {
    final Map<String, Set<String>> datasourceToKnownSegmentIds = new HashMap<>();
    final AtomicInteger countOfRefreshedUnusedSegments = new AtomicInteger(0);

    // TODO: should we poll all segments here or just poll used
    //  and then separately poll only the required stuff for unused segments
    //  because the number of unused segments can be very large

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
          final DatasourceSegmentCache cache = datasourceToSegmentCache.computeIfAbsent(
              record.dataSource,
              ds -> new DatasourceSegmentCache()
          );

          if (cache.shouldRefreshSegment(record.segmentId, record.state)) {
            if (record.state.isUsed()) {
              datasourceToRefreshSegmentIds.computeIfAbsent(record.dataSource, ds -> new HashSet<>())
                                           .add(record.segmentId);
            } else if (cache.refreshUnusedSegment(record.segmentId, record.state)) {
              countOfRefreshedUnusedSegments.incrementAndGet();
              emitDatasourceMetric(record.dataSource, "refreshed/unused", 1);
            }
          }

          datasourceToKnownSegmentIds.computeIfAbsent(record.dataSource, ds -> new HashSet<>())
                                     .add(record.segmentId);
        }

        return 0;
      } catch (Exception e) {
        log.makeAlert(e, "Error while retrieving segment IDs from metadata store.");
        return 1;
      }
    });

    if (countOfRefreshedUnusedSegments.get() > 0) {
      log.info("Refreshed total [%d] unused segments from metadata store.", countOfRefreshedUnusedSegments.get());
    }

    return datasourceToKnownSegmentIds;
  }

  private void retrieveAndRefreshUsedSegmentsForIds(
      Map<String, Set<String>> datasourceToRefreshSegmentIds
  )
  {
    final AtomicInteger countOfRefreshedUsedSegments = new AtomicInteger(0);
    datasourceToRefreshSegmentIds.forEach((dataSource, segmentIds) -> {
      final DatasourceSegmentCache cache
          = datasourceToSegmentCache.computeIfAbsent(dataSource, ds -> new DatasourceSegmentCache());

      int numUpdatedUsedSegments = 0;
      try (
          CloseableIterator<DataSegmentPlus> iterator = connector.inReadOnlyTransaction(
              (handle, status) -> SqlSegmentsMetadataQuery
                  .forHandle(handle, connector, tablesConfig.get(), jsonMapper)
                  .retrieveSegmentsByIdIterator(dataSource, segmentIds)
          )
      ) {
        while (iterator.hasNext()) {
          if (cache.refreshUsedSegment(iterator.next())) {
            ++numUpdatedUsedSegments;
          }
        }
      }
      catch (IOException e) {
        log.makeAlert(e, "Error retrieving segments for datasource[%s] from metadata store.", dataSource)
           .emit();
      }

      emitDatasourceMetric(dataSource, "refresh/used", numUpdatedUsedSegments);
      countOfRefreshedUsedSegments.addAndGet(numUpdatedUsedSegments);
    });

    if (countOfRefreshedUsedSegments.get() > 0) {
      log.info(
          "Refreshed total [%d] used segments from metadata store.",
          countOfRefreshedUsedSegments.get()
      );
    }
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
                final DateTime createdDate = nullSafeDate(r.getString("created_date"));

                // TODO: use the created date

                final DatasourceSegmentCache cache = datasourceToSegmentCache.computeIfAbsent(
                    record.getId().getDataSource(),
                    ds -> new DatasourceSegmentCache()
                );

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

  /**
   * This is safe to do since updates are always made first to metadata store
   * and then to cache.
   */
  private void removeUnknownSegmentIdsFromCache(Map<String, Set<String>> datasourceToKnownSegmentIds)
  {
    datasourceToSegmentCache.forEach((dataSource, cache) -> {
      final Set<String> unknownSegmentIds = cache.getSegmentIdsNotIn(
          datasourceToKnownSegmentIds.getOrDefault(dataSource, Set.of())
      );
      final int numSegmentsRemoved = cache.removeSegmentIds(unknownSegmentIds);
      if (numSegmentsRemoved > 0) {
        log.info(
            "Removed [%d] unknown segment IDs from cache of datasource[%s].",
            numSegmentsRemoved, dataSource
        );
        emitDatasourceMetric(dataSource, "deleted/unknown", numSegmentsRemoved);
      }
    });
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
    private final SegmentState state;

    SegmentRecord(String segmentId, String dataSource, SegmentState state)
    {
      this.segmentId = segmentId;
      this.dataSource = dataSource;
      this.state = state;
    }

    @Nullable
    static SegmentRecord fromResultSet(ResultSet r)
    {
      try {
        final String segmentId = r.getString("id");
        final boolean isUsed = r.getBoolean("used");
        final String dataSource = r.getString("dataSource");
        final DateTime lastUpdatedTime = nullSafeDate(r.getString("used_status_last_updated"));

        final SegmentState storedState = new SegmentState(isUsed, lastUpdatedTime);

        return new SegmentRecord(segmentId, dataSource, storedState);
      } catch (SQLException e) {
        return null;
      }
    }
  }

}
