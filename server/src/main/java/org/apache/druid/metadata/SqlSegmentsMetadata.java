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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.skife.jdbi.v2.BaseResultSetMapper;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class SqlSegmentsMetadata implements SegmentsMetadata
{
  private static final EmittingLogger log = new EmittingLogger(SqlSegmentsMetadata.class);

  /**
   * Use to synchronize {@link #start()}, {@link #stop()}, {@link #poll()}, and {@link #isStarted()}. These methods
   * should be synchronized to prevent from being called at the same time if two different threads are calling them.
   * This might be possible if a druid coordinator gets and drops leadership repeatedly in quick succession.
   */
  private final ReentrantReadWriteLock startStopLock = new ReentrantReadWriteLock();

  /**
   * Used to ensure that {@link #poll()} is never run concurrently. It should already be so (at least in production
   * code), where {@link #poll()} is called only from the task created in {@link #createPollTaskForStartOrder} and is
   * scheduled in a single-threaded {@link #exec}, so this lock is an additional safety net in case there are bugs in
   * the code, and for tests, where {@link #poll()} is called from the outside code.
   *
   * Not using {@link #startStopLock}.writeLock() in order to still be able to run {@link #poll()} concurrently with
   * {@link #isStarted()}.
   */
  private final Object pollLock = new Object();

  private final ObjectMapper jsonMapper;
  private final Supplier<SegmentsMetadataConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;

  /**
   * Volatile since this reference is reassigned in {@link #poll} and then read from in other threads. Starts null so we
   * can differentiate "never polled" (null) from "polled, but empty" (empty map). Note that this is not simply a
   * lazy-initialized variable: it starts off as null, and may transition between null and non-null multiple times as
   * {@link #stop} and {@link #start} are called.
   */
  private @MonotonicNonNull ConcurrentHashMap<String, DruidDataSource> dataSources = null;

  private volatile @MonotonicNonNull CompletableFuture<Void> firstPollFutureSinceLastStart = null;
  /** The latch to be used to read a non-null object from {@link #firstPollFutureSinceLastStart}. */
  private final CountDownLatch everStartedLatch = new CountDownLatch(1);

  /** The number of times this SqlSegmentsMetadata was started. */
  private long startCount = 0;
  /**
   * Equal to the current {@link #startCount} value if the SqlSegmentsMetadata is currently started; -1 if
   * currently stopped.
   *
   * This field is used to implement a simple stamp mechanism instead of just a boolean "started" flag to prevent
   * the theoretical situation of two or more tasks scheduled in {@link #start()} calling {@link #isStarted()} and
   * {@link #poll()} concurrently, if the sequence of {@link #start()} - {@link #stop()} - {@link #start()} actions
   * occurs quickly.
   *
   * {@link SQLMetadataRuleManager} also has a similar issue.
   */
  private long currentStartOrder = -1;
  private ScheduledExecutorService exec = null;

  @Inject
  public SqlSegmentsMetadata(
      ObjectMapper jsonMapper,
      Supplier<SegmentsMetadataConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      SQLMetadataConnector connector
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.connector = connector;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    doStart();
  }

  public CompletableFuture<Void> doStart()
  {
    ReentrantReadWriteLock.WriteLock lock = startStopLock.writeLock();
    lock.lock();
    try {
      if (isStarted()) {
        return firstPollFutureSinceLastStart;
      }

      firstPollFutureSinceLastStart = new CompletableFuture<>();
      startCount++;
      currentStartOrder = startCount;
      final long localStartOrder = currentStartOrder;

      exec = Execs.scheduledSingleThreaded(getClass().getName() + "-Exec--%d");

      final Duration delay = config.get().getPollDuration().toStandardDuration();
      exec.scheduleWithFixedDelay(
          createPollTaskForStartOrder(localStartOrder),
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
      everStartedLatch.countDown();
      return firstPollFutureSinceLastStart;
    }
    finally {
      lock.unlock();
    }
  }

  private Runnable createPollTaskForStartOrder(long startOrder)
  {
    return () -> {
      // poll() is synchronized together with start(), stop() and isStarted() to ensure that when stop() exits, poll()
      // won't actually run anymore after that (it could only enter the synchronized section and exit immediately
      // because the localStartedOrder doesn't match the new currentStartOrder). It's needed to avoid flakiness in
      // SqlSegmentsMetadataTest. See https://github.com/apache/incubator-druid/issues/6028
      ReentrantReadWriteLock.ReadLock lock = startStopLock.readLock();
      lock.lock();
      try {
        if (startOrder == currentStartOrder) {
          poll();
          firstPollFutureSinceLastStart.complete(null);
        } else {
          log.debug("startOrder = currentStartOrder = %d, skipping poll()", startOrder);
        }
      }
      catch (Throwable t) {
        log.makeAlert(t, "Uncaught exception in " + getClass().getName() + "'s polling thread").emit();
        // Swallow the exception, so that scheduled polling goes on. Leave firstPollFutureSinceLastStart uncompleted
        // for now, so that it may be completed during the next poll.
        if (!(t instanceof Exception)) {
          // Don't try to swallow a Throwable which is not an Exception (that is, a Error).
          firstPollFutureSinceLastStart.completeExceptionally(t);
          throw t;
        }
      }
      finally {
        lock.unlock();
      }
    };
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    ReentrantReadWriteLock.WriteLock lock = startStopLock.writeLock();
    lock.lock();
    try {
      if (!isStarted()) {
        return;
      }

      // NOT nulling dataSources, allowing to query the latest polled data even when this SegmentsMetadata object is
      // stopped.

      currentStartOrder = -1;
      exec.shutdownNow();
      exec = null;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public int markAsUsedAllSegmentsInDataSource(final String dataSource)
  {
    try {
      final IDBI dbi = connector.getDBI();
      VersionedIntervalTimeline<String, DataSegment> segmentTimeline = connector.inReadOnlyTransaction(
          (handle, status) -> VersionedIntervalTimeline.forSegments(
              Iterators.transform(
                  handle
                      .createQuery(
                          StringUtils.format(
                              "SELECT payload FROM %s WHERE dataSource = :dataSource",
                              getSegmentsTable()
                          )
                      )
                      .setFetchSize(connector.getStreamingFetchSize())
                      .bind("dataSource", dataSource)
                      .map(ByteArrayMapper.FIRST)
                      .iterator(),
                  payload -> {
                    try {
                      return jsonMapper.readValue(payload, DataSegment.class);
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
              )

          )
      );

      final List<DataSegment> segments = new ArrayList<>();
      List<TimelineObjectHolder<String, DataSegment>> timelineObjectHolders =
          segmentTimeline.lookup(Intervals.ETERNITY);
      for (TimelineObjectHolder<String, DataSegment> objectHolder : timelineObjectHolders) {
        for (PartitionChunk<DataSegment> partitionChunk : objectHolder.getObject()) {
          segments.add(partitionChunk.getObject());
        }
      }

      if (segments.isEmpty()) {
        log.info("No segments found in the database for data source [%s]", dataSource);
        return 0;
      }

      int numChangedSegments = dbi.withHandle(
          (Handle handle) -> {
            Batch batch = handle.createBatch();

            for (DataSegment segment : segments) {
              batch.add(
                  StringUtils.format(
                      "UPDATE %s SET used=true WHERE id = '%s'",
                      getSegmentsTable(),
                      segment.getId()
                  )
              );
            }
            int[] segmentChanges = batch.execute();
            return Arrays.stream(segmentChanges).sum();
          }
      );
      return numChangedSegments;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking all segments as used in data source [%s]", dataSource);
      throw e;
    }
  }

  @Override
  public boolean markSegmentAsUsed(final String segmentId)
  {
    try {
      int numUpdatedDatabaseEntries = connector.getDBI().withHandle(
          (Handle handle) -> handle
              .createStatement(StringUtils.format("UPDATE %s SET used=true WHERE id = :id", getSegmentsTable()))
              .bind("id", segmentId)
              .execute()
      );
      return numUpdatedDatabaseEntries > 0;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking segment %s as used", segmentId);
      throw e;
    }
  }

  @Override
  public int markAsUnusedAllSegmentsInDataSource(final String dataSource)
  {
    try {
      final int numUpdatedDatabaseEntries = connector.getDBI().withHandle(
          (Handle handle) -> handle
              .createStatement(
                  StringUtils.format("UPDATE %s SET used=false WHERE dataSource = :dataSource", getSegmentsTable())
              )
              .bind("dataSource", dataSource)
              .execute()
      );

      @MonotonicNonNull ConcurrentHashMap<String, DruidDataSource> dataSourcesSnapshot = this.dataSources;
      if (dataSourcesSnapshot != null) {
        dataSourcesSnapshot.remove(dataSource);
      }

      return numUpdatedDatabaseEntries;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking all segments as unused in data source [%s]", dataSource);
      throw e;
    }
  }

  @Override
  public boolean markSegmentAsUnused(String dataSourceName, final String segmentId)
  {
    try {
      boolean segmentStateChanged = markSegmentAsUnusedInDatabase(segmentId);

      // Call iteratePossibleParsingsWithDataSource() outside of dataSources.computeIfPresent() because the former is a
      // potentially expensive operation, while lambda to be passed into computeIfPresent() should preferably run fast.
      List<SegmentId> possibleSegmentIds = SegmentId.iteratePossibleParsingsWithDataSource(dataSourceName, segmentId);
      @MonotonicNonNull ConcurrentHashMap<String, DruidDataSource> dataSourcesSnapshot = this.dataSources;
      if (dataSourcesSnapshot != null) {
        dataSourcesSnapshot.computeIfPresent(
            dataSourceName,
            (dsName, dataSource) -> {
              for (SegmentId possibleSegmentId : possibleSegmentIds) {
                if (dataSource.removeSegment(possibleSegmentId) != null) {
                  break;
                }
              }
              // Returning null from the lambda here makes the ConcurrentHashMap to remove the current entry.
              return dataSource.isEmpty() ? null : dataSource;
            }
        );
      }
      return segmentStateChanged;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking segment [%s] as unused", segmentId);
      throw e;
    }
  }

  @Override
  public boolean markSegmentAsUnused(SegmentId segmentId)
  {
    try {
      final boolean segmentStateChanged = markSegmentAsUnusedInDatabase(segmentId.toString());
      @MonotonicNonNull ConcurrentHashMap<String, DruidDataSource> dataSourcesSnapshot = this.dataSources;
      if (dataSourcesSnapshot != null) {
        dataSourcesSnapshot.computeIfPresent(
            segmentId.getDataSource(),
            (dsName, dataSource) -> {
              dataSource.removeSegment(segmentId);
              // Returning null from the lambda here makes the ConcurrentHashMap to remove the current entry.
              return dataSource.isEmpty() ? null : dataSource;
            }
        );
      }
      return segmentStateChanged;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking segment [%s] as unused", segmentId);
      throw e;
    }
  }

  private boolean markSegmentAsUnusedInDatabase(String segmentId)
  {
    final int numUpdatedDatabaseEntries = connector.getDBI().withHandle(
        handle -> handle
            .createStatement(StringUtils.format("UPDATE %s SET used=false WHERE id = :segmentID", getSegmentsTable()))
            .bind("segmentID", segmentId)
            .execute()
    );
    return numUpdatedDatabaseEntries > 0;
  }

  @Override
  public boolean isStarted()
  {
    // isStarted() is synchronized together with start(), stop() and poll() to ensure that the latest currentStartOrder
    // is always visible. readLock should be used to avoid unexpected performance degradation of DruidCoordinator.
    ReentrantReadWriteLock.ReadLock lock = startStopLock.readLock();
    lock.lock();
    try {
      return currentStartOrder >= 0;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public @Nullable ImmutableDruidDataSource prepareImmutableDataSourceWithUsedSegments(String dataSourceName)
  {
    Uninterruptibles.awaitUninterruptibly(everStartedLatch);
    Futures.getUnchecked(firstPollFutureSinceLastStart);
    final DruidDataSource dataSource = dataSources.get(dataSourceName);
    return dataSource == null ? null : dataSource.toImmutableDruidDataSource();
  }

  @Override
  public @Nullable DruidDataSource getDataSourceWithUsedSegments(String dataSource)
  {
    Uninterruptibles.awaitUninterruptibly(everStartedLatch);
    Futures.getUnchecked(firstPollFutureSinceLastStart);
    return dataSources.get(dataSource);
  }

  @Override
  public Collection<ImmutableDruidDataSource> prepareImmutableDataSourcesWithAllUsedSegments()
  {
    Uninterruptibles.awaitUninterruptibly(everStartedLatch);
    Futures.getUnchecked(firstPollFutureSinceLastStart);

    return Optional.ofNullable(dataSources)
                   .map(m ->
                            m.values()
                             .stream()
                             .map(DruidDataSource::toImmutableDruidDataSource)
                             .collect(Collectors.toList())
                   )
                   .orElse(null);
  }

  @Override
  public @Nullable Iterable<DataSegment> iterateAllUsedSegments()
  {
    final ConcurrentHashMap<String, DruidDataSource> dataSourcesSnapshot = dataSources;
    if (dataSourcesSnapshot == null) {
      return null;
    }

    return () -> dataSourcesSnapshot.values()
                                    .stream()
                                    .flatMap(dataSource -> dataSource.getSegments().stream())
                                    .iterator();
  }

  @Override
  public Collection<String> retrieveAllDataSourceNames()
  {
    return connector.getDBI().withHandle(
        handle -> handle
            .createQuery(StringUtils.format("SELECT DISTINCT(datasource) FROM %s", getSegmentsTable()))
            .fold(
                new ArrayList<>(),
                (List<String> druidDataSources,
                 Map<String, Object> stringObjectMap,
                 FoldController foldController,
                 StatementContext statementContext) -> {
                  druidDataSources.add(MapUtils.getString(stringObjectMap, "datasource"));
                  return druidDataSources;
                }
            )
    );
  }

  @Override
  public void poll()
  {
    // See the comment to the pollLock field, explaining this synchronized block
    synchronized (pollLock) {
      doPoll();
    }
  }

  /** This method is extracted from {@link #poll()} solely to reduce code nesting. */
  @GuardedBy("pollLock")
  private void doPoll()
  {
    log.debug("Starting polling of segment table");

    // some databases such as PostgreSQL require auto-commit turned off
    // to stream results back, enabling transactions disables auto-commit
    //
    // setting connection to read-only will allow some database such as MySQL
    // to automatically use read-only transaction mode, further optimizing the query
    final List<DataSegment> segments = connector.inReadOnlyTransaction(
        new TransactionCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> inTransaction(Handle handle, TransactionStatus status)
          {
            return handle
                .createQuery(StringUtils.format("SELECT payload FROM %s WHERE used=true", getSegmentsTable()))
                .setFetchSize(connector.getStreamingFetchSize())
                .map(
                    new ResultSetMapper<DataSegment>()
                    {
                      @Override
                      public DataSegment map(int index, ResultSet r, StatementContext ctx) throws SQLException
                      {
                        try {
                          DataSegment segment = jsonMapper.readValue(r.getBytes("payload"), DataSegment.class);
                          return replaceWithExistingSegmentIfPresent(segment);
                        }
                        catch (IOException e) {
                          log.makeAlert(e, "Failed to read segment from db.").emit();
                          throw new RuntimeException(e);
                        }
                      }
                    }
                )
                .list();
          }
        }
    );

    if (segments == null || segments.isEmpty()) {
      log.info("No segments found in the database!");
      return;
    }

    log.info("Polled and found %,d segments in the database", segments.size());

    ConcurrentHashMap<String, DruidDataSource> newDataSources = new ConcurrentHashMap<>();

    ImmutableMap<String, String> dataSourceProperties = ImmutableMap.of("created", DateTimes.nowUtc().toString());
    segments
        .stream()
        .filter(Objects::nonNull)
        .forEach(segment -> {
          newDataSources
              .computeIfAbsent(segment.getDataSource(), dsName -> new DruidDataSource(dsName, dataSourceProperties))
              .addSegmentIfAbsent(segment);
        });

    // Replace dataSources atomically.
    dataSources = newDataSources;
  }

  /**
   * For the garbage collector in Java, it's better to keep new objects short-living, but once they are old enough
   * (i. e. promoted to old generation), try to keep them alive. In {@link #poll()}, we fetch and deserialize all
   * existing segments each time, and then replace them in {@link #dataSources}. This method allows to use already
   * existing (old) segments when possible, effectively interning them a-la {@link String#intern} or {@link
   * com.google.common.collect.Interner}, aiming to make the majority of {@link DataSegment} objects garbage soon after
   * they are deserialized and to die in young generation. It allows to avoid fragmentation of the old generation and
   * full GCs.
   */
  private DataSegment replaceWithExistingSegmentIfPresent(DataSegment segment)
  {
    DruidDataSource dataSource = Optional.ofNullable(dataSources).map(m -> m.get(segment.getDataSource())).orElse(null);
    if (dataSource == null) {
      return segment;
    }
    DataSegment alreadyExistingSegment = dataSource.getSegment(segment.getId());
    return alreadyExistingSegment != null ? alreadyExistingSegment : segment;
  }

  private String getSegmentsTable()
  {
    return dbTables.get().getSegmentsTable();
  }

  @Override
  public List<Interval> getUnusedSegmentIntervals(final String dataSource, final DateTime maxEndTime, final int limit)
  {
    return connector.inReadOnlyTransaction(
        new TransactionCallback<List<Interval>>()
        {
          @Override
          public List<Interval> inTransaction(Handle handle, TransactionStatus status)
          {
            Iterator<Interval> iter = handle
                .createQuery(
                    StringUtils.format(
                        "SELECT start, %2$send%2$s FROM %1$s WHERE dataSource = :dataSource AND "
                        + "%2$send%2$s <= :end AND used = false ORDER BY start, %2$send%2$s",
                        getSegmentsTable(),
                        connector.getQuoteString()
                    )
                )
                .setFetchSize(connector.getStreamingFetchSize())
                .setMaxRows(limit)
                .bind("dataSource", dataSource)
                .bind("end", maxEndTime.toString())
                .map(
                    new BaseResultSetMapper<Interval>()
                    {
                      @Override
                      protected Interval mapInternal(int index, Map<String, Object> row)
                      {
                        return new Interval(
                            DateTimes.of((String) row.get("start")),
                            DateTimes.of((String) row.get("end"))
                        );
                      }
                    }
                )
                .iterator();


            List<Interval> result = Lists.newArrayListWithCapacity(limit);
            for (int i = 0; i < limit && iter.hasNext(); i++) {
              try {
                result.add(iter.next());
              }
              catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            return result;
          }
        }
    );
  }
}
