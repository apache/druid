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

package org.apache.druid.metadata.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TODO:
 * -[x] Handle all cases of cache vs metadata store
 * -[ ] Perform read writes to the cache only if it is READY
 * -[ ] Add APIs in cache to read/update
 * -[ ] Wire up cache in IndexerSQLMetadataStorageCoordinator
 * -[ ] Wire up cache in OverlordCompactionScheduler
 * -[ ] Poll pending segments too
 * -[ ] Write unit tests
 * -[ ] Write integration tests
 * -[ ] Write a benchmark
 */
public class SqlSegmentsMetadataCache implements SegmentsMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(SqlSegmentsMetadataCache.class);

  private enum CacheState
  {
    STOPPED, STARTING, READY
  }

  private final ObjectMapper jsonMapper;
  private final Duration pollDuration;
  private final Supplier<MetadataStorageTablesConfig> tablesConfig;
  private final SQLMetadataConnector connector;

  private final ScheduledExecutorService pollExecutor;

  private final AtomicReference<CacheState> currentCacheState
      = new AtomicReference<>(CacheState.STOPPED);

  private final ConcurrentHashMap<String, DatasourceSegmentCache> datasourceToSegmentCache
      = new ConcurrentHashMap<>();
  private final AtomicReference<DateTime> pollFinishTime = new AtomicReference<>();

  @Inject
  public SqlSegmentsMetadataCache(
      ObjectMapper jsonMapper,
      Supplier<SegmentsMetadataManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> tablesConfig,
      SQLMetadataConnector connector,
      ScheduledExecutorFactory executorFactory
  )
  {
    this.jsonMapper = jsonMapper;
    this.pollDuration = config.get().getPollDuration().toStandardDuration();
    this.tablesConfig = tablesConfig;
    this.connector = connector;
    this.pollExecutor = executorFactory.create(1, "SegmentsMetadataCache-%s");
  }


  @Override
  public void startPollingDatabase()
  {
    if (currentCacheState.compareAndSet(CacheState.STOPPED, CacheState.STARTING)) {
      tearDown();
      pollExecutor.schedule(this::pollSegments, pollDuration.getMillis(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void stopPollingDatabase()
  {
    currentCacheState.set(CacheState.STOPPED);
    tearDown();

    // TODO: Handle race conditions
    // T1: sees cache as READY and starts to write something to it
    // T2: stops the cache and cleans it up
    // T1: updates the cache but does not do clean up afterwards
    // I guess it is okay to have the stray entries as long as we clean up everything when we start

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
  public Map<String, SegmentTimeline> getDataSourceToUsedSegmentTimeline()
  {
    return Map.of();
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

  private void pollSegments()
  {
    final Stopwatch sincePollStart = Stopwatch.createStarted();
    if (isStopped()) {
      tearDown();
      return;
    }

    final Map<String, Set<String>> datasourceToKnownSegmentIds = new HashMap<>();
    final Map<String, Set<String>> datasourceToRefreshSegmentIds = new HashMap<>();

    final AtomicInteger countOfRefreshedUnusedSegments = new AtomicInteger(0);

    final String getAllIdsSql = "SELECT id, dataSource, used, used_status_last_updated FROM %s";
    connector.inReadOnlyTransaction(
        (handle, status) -> handle
            .createQuery(StringUtils.format(getAllIdsSql, getSegmentsTable()))
            .setFetchSize(connector.getStreamingFetchSize())
            .map((index, r, ctx) -> {
              try {
                final String segmentId = r.getString("id");
                final boolean isUsed = r.getBoolean("used");
                final String dataSource = r.getString("dataSource");
                final String updatedColValue = r.getString("used_status_last_updated");
                final DateTime lastUpdatedTime
                    = updatedColValue == null ? null : DateTimes.of(updatedColValue);

                final SegmentState metadataState
                    = new SegmentState(segmentId, dataSource, isUsed, lastUpdatedTime);

                final DatasourceSegmentCache cache
                    = datasourceToSegmentCache.computeIfAbsent(dataSource, ds -> new DatasourceSegmentCache());

                if (cache.shouldRefreshSegment(metadataState)) {
                  if (metadataState.isUsed()) {
                    datasourceToRefreshSegmentIds.computeIfAbsent(dataSource, ds -> new HashSet<>())
                                                 .add(segmentId);
                  } else if (cache.refreshUnusedSegment(metadataState)) {
                    countOfRefreshedUnusedSegments.incrementAndGet();
                  }
                }

                datasourceToKnownSegmentIds.computeIfAbsent(dataSource, ds -> new HashSet<>())
                                           .add(segmentId);
                return 0;
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to read segments from metadata store.").emit();
                // Do not throw an exception so that polling continues
                return 1;
              }
            })
    );

    if (countOfRefreshedUnusedSegments.get() > 0) {
      log.info("Refreshed [%d] unused segments from metadata store.", countOfRefreshedUnusedSegments.get());
      // TODO: emit a metric here
    }

    // Remove unknown segment IDs from cache
    // This is safe to do since updates are always made first to metadata store and then to cache
    datasourceToSegmentCache.forEach((dataSource, cache) -> {
      final Set<String> unknownSegmentIds = cache.getUnknownSegmentIds(
          datasourceToKnownSegmentIds.getOrDefault(dataSource, Set.of())
      );
      final int numSegmentsRemoved = cache.removeSegmentIds(unknownSegmentIds);
      if (numSegmentsRemoved > 0) {
        log.info(
            "Removed [%d] unknown segment IDs from cache of datasource[%s].",
            numSegmentsRemoved, dataSource
        );
      }
    });

    if (isStopped()) {
      tearDown();
      return;
    }

    final AtomicInteger countOfRefreshedUsedSegments = new AtomicInteger(0);
    datasourceToRefreshSegmentIds.forEach((dataSource, segmentIds) -> {
      long numUpdatedUsedSegments = connector.inReadOnlyTransaction((handle, status) -> {
        final DatasourceSegmentCache cache
            = datasourceToSegmentCache.computeIfAbsent(dataSource, ds -> new DatasourceSegmentCache());

        return SqlSegmentsMetadataQuery
            .forHandle(handle, connector, tablesConfig.get(), jsonMapper)
            .retrieveSegmentsById(dataSource, segmentIds)
            .stream()
            .map(cache::refreshUsedSegment)
            .filter(updated -> updated)
            .count();
      });

      // TODO: emit metric here
      countOfRefreshedUsedSegments.addAndGet((int) numUpdatedUsedSegments);
    });

    if (countOfRefreshedUsedSegments.get() > 0) {
      log.info("Refreshed [%d] used segments from metadata store.", countOfRefreshedUnusedSegments.get());
      // TODO: emit a metric here
    }

    // TODO: poll pending segments
    // TODO: emit poll duration metric

    pollFinishTime.set(DateTimes.nowUtc());

    if (isStopped()) {
      tearDown();
    } else {
      // Schedule the next poll
      final long nextPollDelay = Math.max(pollDuration.getMillis() - sincePollStart.millisElapsed(), 0);
      pollExecutor.schedule(this::pollSegments, nextPollDelay, TimeUnit.MILLISECONDS);
    }
  }

  private String getSegmentsTable()
  {
    return tablesConfig.get().getSegmentsTable();
  }

}
