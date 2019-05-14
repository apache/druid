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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.skife.jdbi.v2.BaseResultSetMapper;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 *
 */
@ManageLifecycle
public class SQLMetadataSegmentManager implements MetadataSegmentManager
{
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataSegmentManager.class);

  /**
   * Marker interface for objects stored in {@link #latestDatabasePoll}. See the comment for that field for details.
   */
  private interface DatabasePoll
  {}

  /** Represents periodic {@link #poll}s happening from {@link #exec}. */
  private static class PeriodicDatabasePoll implements DatabasePoll
  {
    /**
     * This future allows to wait until {@link #dataSources} is initialized in the first {@link #poll()} happening since
     * {@link #startPollingDatabasePeriodically()} is called for the first time, or since the last visible (in
     * happens-before terms) call to {@link #startPollingDatabasePeriodically()} in case of Coordinator's leadership
     * changes.
     */
    final CompletableFuture<Void> firstPollCompletionFuture = new CompletableFuture<>();
  }

  /**
   * Represents on-demand {@link #poll} initiated at periods of time when SqlSegmentsMetadata doesn't poll the database
   * periodically.
   */
  private static class OnDemandDatabasePoll implements DatabasePoll
  {
    final long initiationTimeNanos = System.nanoTime();
    final CompletableFuture<Void> pollCompletionFuture = new CompletableFuture<>();

    long nanosElapsedFromInitiation()
    {
      return System.nanoTime() - initiationTimeNanos;
    }
  }

  /**
   * Use to synchronize {@link #startPollingDatabasePeriodically}, {@link #stopPollingDatabasePeriodically}, {@link
   * #poll}, and {@link #isPollingDatabasePeriodically}. These methods should be synchronized to prevent from being
   * called at the same time if two different threads are calling them. This might be possible if Coordinator gets and
   * drops leadership repeatedly in quick succession.
   *
   * This lock is also used to synchronize {@link #awaitOrPerformDatabasePoll} for times when SqlSegmentsMetadata
   * is not polling the database periodically (in other words, when the Coordinator is not the leader).
   */
  private final ReentrantReadWriteLock startStopPollLock = new ReentrantReadWriteLock();

  /**
   * Used to ensure that {@link #poll()} is never run concurrently. It should already be so (at least in production
   * code), where {@link #poll()} is called only from the task created in {@link #createPollTaskForStartOrder} and is
   * scheduled in a single-threaded {@link #exec}, so this lock is an additional safety net in case there are bugs in
   * the code, and for tests, where {@link #poll()} is called from the outside code.
   *
   * Not using {@link #startStopPollLock}.writeLock() in order to still be able to run {@link #poll()} concurrently
   * with {@link #isPollingDatabasePeriodically()}.
   */
  private final Object pollLock = new Object();

  private final ObjectMapper jsonMapper;
  private final Duration periodicPollDelay;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;

  /**
   * This field is made volatile to avoid "ghost secondary reads" that may result in NPE, see
   * https://github.com/code-review-checklists/java-concurrency#safe-local-dcl (note that dataSources resembles a lazily
   * initialized field). Alternative is to always read the field in a snapshot local variable, but it's too easy to
   * forget to do.
   *
   * This field may be updated from {@link #exec}, or from whatever thread calling {@link #doOnDemandPoll} via {@link
   * #awaitOrPerformDatabasePoll()} via one of the public methods of SqlSegmentsMetadata.
   */
  private volatile @MonotonicNonNull ConcurrentHashMap<String, DruidDataSource> dataSources = null;

  /**
   * The latest {@link DatabasePoll} represent {@link #poll()} calls which update {@link #dataSources}, either
   * periodically (see {@link PeriodicDatabasePoll}, {@link #startPollingDatabasePeriodically}, {@link
   * #stopPollingDatabasePeriodically}) or "on demand" (see {@link OnDemandDatabasePoll}), when one of the methods that
   * accesses {@link #dataSources} state (such as {@link #prepareImmutableDataSourceWithUsedSegments}) is called when
   * the Coordinator is not the leader and therefore SqlSegmentsMetadata isn't polling the database periodically.
   *
   * The notion and the complexity of "on demand" database polls was introduced to simplify the interface of {@link
   * MetadataSegmentManager} and guarantee that it always returns consistent and relatively up-to-date data from methods like
   * {@link #prepareImmutableDataSourceWithUsedSegments}, while avoiding excessive repetitive polls. The last part is
   * achieved via "hooking on" other polls by awaiting on {@link PeriodicDatabasePoll#firstPollCompletionFuture} or
   * {@link OnDemandDatabasePoll#pollCompletionFuture}, see {@link #awaitOrPerformDatabasePoll} method
   * implementation for details.
   *
   * Note: the overall implementation of periodic/on-demand polls is not completely optimal: for example, when the
   * Coordinator just stopped leading, the latest periodic {@link #poll} (which is still "fresh") is not considered
   * and a new on-demand poll is always initiated. This is done to simplify the implementation, while the efficiency
   * during Coordinator leadership switches is not a priority.
   *
   * This field is {@code volatile} because it's checked and updated in a double-checked locking manner in {@link
   * #awaitOrPerformDatabasePoll()}.
   */
  private volatile @Nullable DatabasePoll latestDatabasePoll = null;

  /** Used to cancel periodic poll task in {@link #stopPollingDatabasePeriodically}. */
  @GuardedBy("startStopPollLock")
  private @Nullable Future<?> periodicPollTaskFuture = null;

  /** The number of times {@link #startPollingDatabasePeriodically} was called. */
  private long startPollingCount = 0;

  /**
   * Equal to the current {@link #startPollingCount} value if the SqlSegmentsMetadata is currently started; -1 if
   * currently stopped.
   *
   * This field is used to implement a simple stamp mechanism instead of just a boolean "started" flag to prevent
   * the theoretical situation of two or more tasks scheduled in {@link #startPollingDatabasePeriodically()} calling
   * {@link #isPollingDatabasePeriodically()} and {@link #poll()} concurrently, if the sequence of {@link
   * #startPollingDatabasePeriodically()} - {@link #stopPollingDatabasePeriodically()} - {@link
   * #startPollingDatabasePeriodically()} actions occurs quickly.
   *
   * {@link SQLMetadataRuleManager} also has a similar issue.
   */
  private long currentStartPollingOrder = -1;

  private ScheduledExecutorService exec = null;

  @Inject
  public SQLMetadataSegmentManager(
      ObjectMapper jsonMapper,
      Supplier<MetadataSegmentManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      SQLMetadataConnector connector
  )
  {
    this.jsonMapper = jsonMapper;
    this.periodicPollDelay = config.get().getPollDuration().toStandardDuration();
    this.dbTables = dbTables;
    this.connector = connector;
  }

  /**
   * Don't confuse this method with {@link #startPollingDatabasePeriodically}. This is a lifecycle starting method to
   * be executed just once for an instance of SqlSegmentsMetadata.
   */
  @LifecycleStart
  public void start()
  {
    ReentrantReadWriteLock.WriteLock lock = startStopPollLock.writeLock();
    lock.lock();
    try {
      exec = Execs.scheduledSingleThreaded(getClass().getName() + "-Exec--%d");
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * Don't confuse this method with {@link #stopPollingDatabasePeriodically}. This is a lifecycle stopping method to
   * be executed just once for an instance of SqlSegmentsMetadata.
   */
  @LifecycleStop
  public void stop()
  {
    ReentrantReadWriteLock.WriteLock lock = startStopPollLock.writeLock();
    lock.lock();
    try {
      exec.shutdownNow();
      exec = null;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public void startPollingDatabasePeriodically()
  {
    ReentrantReadWriteLock.WriteLock lock = startStopPollLock.writeLock();
    lock.lock();
    try {
      if (exec == null) {
        throw new IllegalStateException(getClass().getName() + " is not started");
      }
      if (isPollingDatabasePeriodically()) {
        return;
      }

      PeriodicDatabasePoll periodicPollUpdate = new PeriodicDatabasePoll();
      latestDatabasePoll = periodicPollUpdate;

      startPollingCount++;
      currentStartPollingOrder = startPollingCount;
      final long localStartOrder = currentStartPollingOrder;

      periodicPollTaskFuture = exec.scheduleWithFixedDelay(
          createPollTaskForStartOrder(localStartOrder, periodicPollUpdate),
          0,
          periodicPollDelay.getMillis(),
          TimeUnit.MILLISECONDS
      );
    }
    finally {
      lock.unlock();
    }
  }

  private Runnable createPollTaskForStartOrder(long startOrder, PeriodicDatabasePoll periodicPollUpdate)
  {
    return () -> {
      // poll() is synchronized together with startPollingDatabasePeriodically(), stopPollingDatabasePeriodically() and
      // isPollingDatabasePeriodically() to ensure that when stopPollingDatabasePeriodically() exits, poll() won't
      // actually run anymore after that (it could only enter the synchronized section and exit immediately because the
      // localStartedOrder doesn't match the new currentStartPollingOrder). It's needed to avoid flakiness in
      // SqlSegmentsMetadataTest. See https://github.com/apache/incubator-druid/issues/6028
      ReentrantReadWriteLock.ReadLock lock = startStopPollLock.readLock();
      lock.lock();
      try {
        if (startOrder == currentStartPollingOrder) {
          poll();
          periodicPollUpdate.firstPollCompletionFuture.complete(null);
        } else {
          log.debug("startOrder = currentStartPollingOrder = %d, skipping poll()", startOrder);
        }
      }
      catch (Throwable t) {
        log.makeAlert(t, "Uncaught exception in " + getClass().getName() + "'s polling thread").emit();
        // Swallow the exception, so that scheduled polling goes on. Leave firstPollFutureSinceLastStart uncompleted
        // for now, so that it may be completed during the next poll.
        if (!(t instanceof Exception)) {
          // Don't try to swallow a Throwable which is not an Exception (that is, a Error).
          periodicPollUpdate.firstPollCompletionFuture.completeExceptionally(t);
          throw t;
        }
      }
      finally {
        lock.unlock();
      }
    };
  }

  @Override
  public void stopPollingDatabasePeriodically()
  {
    ReentrantReadWriteLock.WriteLock lock = startStopPollLock.writeLock();
    lock.lock();
    try {
      if (!isPollingDatabasePeriodically()) {
        return;
      }

      periodicPollTaskFuture.cancel(false);
      latestDatabasePoll = null;

      // NOT nulling dataSources, allowing to query the latest polled data even when this SegmentsMetadata object is
      // stopped.

      currentStartPollingOrder = -1;
    }
    finally {
      lock.unlock();
    }
  }

  private void awaitOrPerformDatabasePoll()
  {
    // Double-checked locking with awaitPeriodicOrFreshOnDemandDatabasePoll() call playing the role of the "check".
    if (awaitPeriodicOrFreshOnDemandDatabasePoll()) {
      return;
    }
    ReentrantReadWriteLock.WriteLock lock = startStopPollLock.writeLock();
    lock.lock();
    try {
      if (awaitPeriodicOrFreshOnDemandDatabasePoll()) {
        return;
      }
      OnDemandDatabasePoll newOnDemandUpdate = new OnDemandDatabasePoll();
      this.latestDatabasePoll = newOnDemandUpdate;
      doOnDemandPoll(newOnDemandUpdate);
    }
    finally {
      lock.unlock();
    }
  }

  private boolean awaitPeriodicOrFreshOnDemandDatabasePoll()
  {
    DatabasePoll latestDatabasePoll = this.latestDatabasePoll;
    if (latestDatabasePoll instanceof PeriodicDatabasePoll) {
      Futures.getUnchecked(((PeriodicDatabasePoll) latestDatabasePoll).firstPollCompletionFuture);
      return true;
    }
    if (latestDatabasePoll instanceof OnDemandDatabasePoll) {
      long periodicPollDelayNanos = TimeUnit.MILLISECONDS.toNanos(periodicPollDelay.getMillis());
      OnDemandDatabasePoll latestOnDemandUpdate = (OnDemandDatabasePoll) latestDatabasePoll;
      boolean latestUpdateIsFresh = latestOnDemandUpdate.nanosElapsedFromInitiation() < periodicPollDelayNanos;
      if (latestUpdateIsFresh) {
        Futures.getUnchecked(latestOnDemandUpdate.pollCompletionFuture);
        return true;
      }
      // Latest on-demand update is not fresh. Fall through to return false from this method.
    } else {
      assert latestDatabasePoll == null;
    }
    return false;
  }

  private void doOnDemandPoll(OnDemandDatabasePoll onDemandPoll)
  {
    try {
      poll();
      onDemandPoll.pollCompletionFuture.complete(null);
    }
    catch (Throwable t) {
      onDemandPoll.pollCompletionFuture.completeExceptionally(t);
      throw t;
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
      // Unlike bulk markAsUsed methods: markAsUsedAllNonOvershadowedSegmentsInDataSource(),
      // markAsUsedNonOvershadowedSegmentsInInterval(), and markAsUsedNonOvershadowedSegments() we don't put the marked
      // segment into the respective data source, because we don't have it fetched from the database. It's probably not
      // worth complicating the implementation and making two database queries just to add the segment because it will
      // be anyway fetched during the next poll(). Segment putting that is done in the bulk markAsUsed methods is a nice
      // to have thing, but doesn't formally affects the external guarantees of SegmentsMetadata class.
      return numUpdatedDatabaseEntries > 0;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking segment %s as used", segmentId);
      throw e;
    }
  }

  @Override
  public int markAsUsedAllNonOvershadowedSegmentsInDataSource(final String dataSource)
  {
    return doMarkAsUsedNonOvershadowedSegments(dataSource, null);
  }

  @Override
  public int markAsUsedNonOvershadowedSegmentsInInterval(final String dataSource, final Interval interval)
  {
    Preconditions.checkNotNull(interval);
    return doMarkAsUsedNonOvershadowedSegments(dataSource, interval);
  }

  /**
   * Implementation for both {@link #markAsUsedAllNonOvershadowedSegmentsInDataSource} (if the given interval is null)
   * and {@link #markAsUsedNonOvershadowedSegmentsInInterval}.
   */
  private int doMarkAsUsedNonOvershadowedSegments(String dataSourceName, @Nullable Interval interval)
  {
    List<DataSegment> usedSegmentsOverlappingInterval = new ArrayList<>();
    List<DataSegment> unusedSegmentsInInterval = new ArrayList<>();
    connector.inReadOnlyTransaction(
        (handle, status) -> {
          String queryString =
              StringUtils.format("SELECT used, payload FROM %1$s WHERE dataSource = :dataSource", getSegmentsTable());
          if (interval != null) {
            queryString += StringUtils.format(" AND start < :end AND %1$send%1$s > :start", connector.getQuoteString());
          }
          Query<?> query = handle
              .createQuery(queryString)
              .setFetchSize(connector.getStreamingFetchSize())
              .bind("dataSource", dataSourceName);
          if (interval != null) {
            query = query
                .bind("start", interval.getStart().toString())
                .bind("end", interval.getEnd().toString());
          }
          query = query
              .map((int index, ResultSet resultSet, StatementContext context) -> {
                try {
                  DataSegment segment = jsonMapper.readValue(resultSet.getBytes("payload"), DataSegment.class);
                  if (resultSet.getBoolean("used")) {
                    usedSegmentsOverlappingInterval.add(segment);
                  } else {
                    if (interval == null || interval.contains(segment.getInterval())) {
                      unusedSegmentsInInterval.add(segment);
                    }
                  }
                  return null;
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
          // Consume the query results to ensure usedSegmentsOverlappingInterval and unusedSegmentsInInterval are
          // populated.
          consume(query.iterator());
          return null;
        }
    );

    VersionedIntervalTimeline<String, DataSegment> versionedIntervalTimeline = VersionedIntervalTimeline.forSegments(
        Iterators.concat(usedSegmentsOverlappingInterval.iterator(), unusedSegmentsInInterval.iterator())
    );

    return markNonOvershadowedSegmentsAsUsed(dataSourceName, unusedSegmentsInInterval, versionedIntervalTimeline);
  }

  private static void consume(Iterator<?> iterator)
  {
    while (iterator.hasNext()) {
      iterator.next();
    }
  }

  /** Also puts non-overshadowed segments into {@link #dataSources}. */
  private int markNonOvershadowedSegmentsAsUsed(
      String dataSourceName,
      List<DataSegment> unusedSegments,
      VersionedIntervalTimeline<String, DataSegment> timeline
  )
  {
    @Nullable
    DruidDataSource dataSource = null;
    if (dataSources != null) {
      dataSource = dataSources.computeIfAbsent(
          dataSourceName,
          dsName -> new DruidDataSource(dsName, createDefaultDataSourceProperties())
      );
    }
    List<String> segmentIdsToMarkAsUsed = new ArrayList<>();
    for (DataSegment segment : unusedSegments) {
      if (timeline.isOvershadowed(segment.getInterval(), segment.getVersion())) {
        continue;
      }
      if (dataSource != null) {
        dataSource.addSegment(segment);
      }
      String s = segment.getId().toString();
      segmentIdsToMarkAsUsed.add(s);
    }

    return markSegmentsAsUsed(segmentIdsToMarkAsUsed);
  }

  @Override
  public int markAsUsedNonOvershadowedSegments(final String dataSource, final Set<String> segmentIds)
      throws UnknownSegmentIdException
  {
    try {
      Pair<List<DataSegment>, VersionedIntervalTimeline<String, DataSegment>> unusedSegmentsAndTimeline = connector
          .inReadOnlyTransaction(
              (handle, status) -> {
                List<DataSegment> unusedSegments = retreiveUnusedSegments(dataSource, segmentIds, handle);
                List<Interval> unusedSegmentsIntervals = JodaUtils.condenseIntervals(
                    unusedSegments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
                );
                Iterator<DataSegment> usedSegmentsOverlappingUnusedSegmentsIntervals =
                    retreiveUsedSegmentsOverlappingIntervals(dataSource, unusedSegmentsIntervals, handle);
                VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(
                    Iterators.concat(usedSegmentsOverlappingUnusedSegmentsIntervals, unusedSegments.iterator())
                );
                return new Pair<>(unusedSegments, timeline);
              }
          );

      List<DataSegment> unusedSegments = unusedSegmentsAndTimeline.lhs;
      VersionedIntervalTimeline<String, DataSegment> timeline = unusedSegmentsAndTimeline.rhs;
      return markNonOvershadowedSegmentsAsUsed(dataSource, unusedSegments, timeline);
    }
    catch (Exception e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof UnknownSegmentIdException) {
        throw (UnknownSegmentIdException) rootCause;
      } else {
        throw e;
      }
    }
  }

  private List<DataSegment> retreiveUnusedSegments(
      final String dataSource,
      final Set<String> segmentIds,
      final Handle handle
  ) throws UnknownSegmentIdException
  {
    List<String> unknownSegmentIds = new ArrayList<>();
    List<DataSegment> segments = segmentIds
        .stream()
        .map(
            segmentId -> {
              Iterator<DataSegment> segmentResultIterator = handle
                  .createQuery(
                      StringUtils.format(
                          "SELECT used, payload FROM %1$s WHERE dataSource = :dataSource AND id = :id",
                          getSegmentsTable()
                      )
                  )
                  .bind("dataSource", dataSource)
                  .bind("id", segmentId)
                  .map((int index, ResultSet resultSet, StatementContext context) -> {
                    try {
                      if (!resultSet.getBoolean("used")) {
                        return jsonMapper.readValue(resultSet.getBytes("payload"), DataSegment.class);
                      } else {
                        // We emit nulls for used segments. They are filtered out below in this method.
                        return null;
                      }
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
                  .iterator();
              if (!segmentResultIterator.hasNext()) {
                unknownSegmentIds.add(segmentId);
                return null;
              } else {
                @Nullable DataSegment segment = segmentResultIterator.next();
                if (segmentResultIterator.hasNext()) {
                  log.error(
                      "There is more than one row corresponding to segment id [%s] in data source [%s] in the database",
                      segmentId,
                      dataSource
                  );
                }
                return segment;
              }
            }
        )
        .filter(Objects::nonNull) // Filter nulls corresponding to used segments.
        .collect(Collectors.toList());
    if (!unknownSegmentIds.isEmpty()) {
      throw new UnknownSegmentIdException(unknownSegmentIds);
    }
    return segments;
  }

  private Iterator<DataSegment> retreiveUsedSegmentsOverlappingIntervals(
      final String dataSource,
      final Collection<Interval> intervals,
      final Handle handle
  )
  {
    return intervals
        .stream()
        .flatMap(interval -> {
          Iterable<DataSegment> segmentResultIterable = () -> handle
              .createQuery(
                  StringUtils.format(
                      "SELECT payload FROM %1$s "
                      + "WHERE dataSource = :dataSource AND start < :end AND %2$send%2$s > :start AND used = true",
                      getSegmentsTable(),
                      connector.getQuoteString()
                  )
              )
              .setFetchSize(connector.getStreamingFetchSize())
              .bind("dataSource", dataSource)
              .bind("start", interval.getStart().toString())
              .bind("end", interval.getEnd().toString())
              .map((int index, ResultSet resultSet, StatementContext context) -> {
                try {
                  return jsonMapper.readValue(resultSet.getBytes("payload"), DataSegment.class);
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              })
              .iterator();
          return StreamSupport.stream(segmentResultIterable.spliterator(), false);
        })
        .iterator();
  }

  private int markSegmentsAsUsed(final List<String> segmentIds)
  {
    if (segmentIds.isEmpty()) {
      log.info("No segments found to update!");
      return 0;
    }

    return connector.getDBI().withHandle(handle -> {
      Batch batch = handle.createBatch();
      segmentIds.forEach(segmentId -> batch.add(
          StringUtils.format("UPDATE %s SET used=true WHERE id = '%s'", getSegmentsTable(), segmentId)
      ));
      int[] segmentChanges = batch.execute();
      return computeNumChangedSegments(segmentIds, segmentChanges);
    });
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

      if (dataSources != null) {
        dataSources.remove(dataSource);
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
      if (dataSources != null) {
        removeSegmentFromPolledDataSources(dataSourceName, segmentId, dataSources);
      }
      return segmentStateChanged;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking segment [%s] as unused", segmentId);
      throw e;
    }
  }

  private static void removeSegmentFromPolledDataSources(
      String dataSourceName,
      String segmentId,
      ConcurrentHashMap<String, DruidDataSource> dataSourcesSnapshot
  )
  {
    // Call iteratePossibleParsingsWithDataSource() outside of dataSources.computeIfPresent() because the former is
    // a potentially expensive operation, while lambda to be passed into computeIfPresent() should preferably run
    // fast.
    List<SegmentId> possibleSegmentIds = SegmentId.iteratePossibleParsingsWithDataSource(dataSourceName, segmentId);
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

  @Override
  public boolean markSegmentAsUnused(SegmentId segmentId)
  {
    try {
      final boolean segmentStateChanged = markSegmentAsUnusedInDatabase(segmentId.toString());
      if (dataSources != null) {
        dataSources.computeIfPresent(
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

  @Override
  public int markSegmentsAsUnused(String dataSourceName, Set<String> segmentIds)
  {
    if (segmentIds.isEmpty()) {
      return 0;
    }
    final List<String> segmentIdList = new ArrayList<>(segmentIds);
    try {
      return connector.getDBI().withHandle(handle -> {
        Batch batch = handle.createBatch();
        segmentIdList.forEach(segmentId -> batch.add(
            StringUtils.format(
                "UPDATE %s SET used=false WHERE datasource = '%s' AND id = '%s'",
                getSegmentsTable(),
                dataSourceName,
                segmentId
            )
        ));
        final int[] segmentChanges = batch.execute();
        int numChangedSegments = computeNumChangedSegments(segmentIdList, segmentChanges);

        // Also remove segments from polled dataSources.
        // Cache dataSourcesSnapshot locally to make sure that we do all updates to a single map, not to two different
        // maps if poll() happens concurrently.
        @MonotonicNonNull ConcurrentHashMap<String, DruidDataSource> dataSourcesSnapshot = this.dataSources;
        if (dataSourcesSnapshot != null) {
          for (String segmentId : segmentIdList) {
            removeSegmentFromPolledDataSources(dataSourceName, segmentId, dataSourcesSnapshot);
          }
        }
        return numChangedSegments;
      });
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int markAsUnusedSegmentsInInterval(String dataSourceName, Interval interval)
  {
    try {
      Integer numUpdatedDatabaseEntries = connector.getDBI().withHandle(
          handle -> handle
              .createStatement(
                  StringUtils
                      .format(
                          "UPDATE %s SET used=false WHERE datasource = :datasource "
                          + "AND start >= :start AND %2$send%2$s <= :end",
                          getSegmentsTable(),
                          connector.getQuoteString()
                      ))
              .bind("datasource", dataSourceName)
              .bind("start", interval.getStart().toString())
              .bind("end", interval.getEnd().toString())
              .execute()
      );
      if (dataSources != null) {
        dataSources.computeIfPresent(dataSourceName, (dsName, dataSource) -> {
          dataSource.removeSegmentsIf(segment -> interval.contains(segment.getInterval()));
          // Returning null from the lambda here makes the ConcurrentHashMap to remove the current entry.
          return dataSource.isEmpty() ? null : dataSource;
        });
      }
      return numUpdatedDatabaseEntries;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean markSegmentAsUnusedInDatabase(String segmentId)
  {
    final int numUpdatedRows = connector.getDBI().withHandle(
        handle -> handle
            .createStatement(StringUtils.format("UPDATE %s SET used=false WHERE id = :segmentID", getSegmentsTable()))
            .bind("segmentID", segmentId)
            .execute()
    );
    if (numUpdatedRows < 0) {
      log.assertionError(
          "Negative number of rows updated for segment id [%s]: %d",
          segmentId,
          numUpdatedRows
      );
    } else if (numUpdatedRows > 1) {
      log.error(
          "More than one row updated for segment id [%s]: %d, "
          + "there may be more than one row for the segment id in the database",
          segmentId,
          numUpdatedRows
      );
    }
    return numUpdatedRows > 0;
  }

  private static int computeNumChangedSegments(List<String> segmentIds, int[] segmentChanges)
  {
    int numChangedSegments = 0;
    for (int i = 0; i < segmentChanges.length; i++) {
      int numUpdatedRows = segmentChanges[i];
      if (numUpdatedRows < 0) {
        log.assertionError(
            "Negative number of rows updated for segment id [%s]: %d",
            segmentIds.get(i),
            numUpdatedRows
        );
      } else if (numUpdatedRows > 1) {
        log.error(
            "More than one row updated for segment id [%s]: %d, "
            + "there may be more than one row for the segment id in the database",
            segmentIds.get(i),
            numUpdatedRows
        );
      }
      if (numUpdatedRows > 0) {
        numChangedSegments += 1;
      }
    }
    return numChangedSegments;
  }

  @Override
  public boolean isPollingDatabasePeriodically()
  {
    // isPollingDatabasePeriodically() is synchronized together with startPollingDatabasePeriodically(),
    // stopPollingDatabasePeriodically() and poll() to ensure that the latest currentStartPollingOrder is always
    // visible. readLock should be used to avoid unexpected performance degradation of DruidCoordinator.
    ReentrantReadWriteLock.ReadLock lock = startStopPollLock.readLock();
    lock.lock();
    try {
      return currentStartPollingOrder >= 0;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public @Nullable ImmutableDruidDataSource prepareImmutableDataSourceWithUsedSegments(String dataSourceName)
  {
    awaitOrPerformDatabasePoll();
    final DruidDataSource dataSource = dataSources.get(dataSourceName);
    return dataSource == null || dataSource.isEmpty() ? null : dataSource.toImmutableDruidDataSource();
  }

  @Override
  public @Nullable DruidDataSource getDataSourceWithUsedSegments(String dataSource)
  {
    awaitOrPerformDatabasePoll();
    return dataSources.get(dataSource);
  }

  @Override
  public Collection<ImmutableDruidDataSource> prepareImmutableDataSourcesWithAllUsedSegments()
  {
    awaitOrPerformDatabasePoll();
    return dataSources
        .values()
        .stream()
        .map(DruidDataSource::toImmutableDruidDataSource)
        .collect(Collectors.toList());
  }

  @Override
  public Iterable<DataSegment> iterateAllUsedSegments()
  {
    awaitOrPerformDatabasePoll();
    return () -> dataSources
        .values()
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
                          // If one entry is database is corrupted, doPoll() should continue to work overall. See
                          // .filter(Objects::nonNull) below in this method.
                          return null;
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

    ImmutableMap<String, String> dataSourceProperties = createDefaultDataSourceProperties();
    segments
        .stream()
        .filter(Objects::nonNull) // Filter corrupted entries (see above in this method).
        .forEach(segment -> {
          newDataSources
              .computeIfAbsent(segment.getDataSource(), dsName -> new DruidDataSource(dsName, dataSourceProperties))
              .addSegmentIfAbsent(segment);
        });

    // Replace dataSources atomically.
    dataSources = newDataSources;
  }

  private static ImmutableMap<String, String> createDefaultDataSourceProperties()
  {
    return ImmutableMap.of("created", DateTimes.nowUtc().toString());
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
    if (dataSources == null) {
      return segment;
    }
    @Nullable DruidDataSource dataSource = dataSources.get(segment.getDataSource());
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
