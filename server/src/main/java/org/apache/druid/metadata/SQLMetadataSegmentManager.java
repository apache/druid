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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.client.DataSourcesSnapshot;
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
     * This future allows to wait until {@link #dataSourcesSnapshot} is initialized in the first {@link #poll()}
     * happening since {@link #startPollingDatabasePeriodically()} is called for the first time, or since the last
     * visible (in happens-before terms) call to {@link #startPollingDatabasePeriodically()} in case of Coordinator's
     * leadership changes.
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
   * https://github.com/code-review-checklists/java-concurrency#safe-local-dcl (note that dataSourcesSnapshot resembles
   * a lazily initialized field). Alternative is to always read the field in a snapshot local variable, but it's too
   * easy to forget to do.
   *
   * This field may be updated from {@link #exec}, or from whatever thread calling {@link #doOnDemandPoll} via {@link
   * #awaitOrPerformDatabasePoll()} via one of the public methods of SqlSegmentsMetadata.
   */
  private volatile @MonotonicNonNull DataSourcesSnapshot dataSourcesSnapshot = null;

  /**
   * The latest {@link DatabasePoll} represent {@link #poll()} calls which update {@link #dataSourcesSnapshot}, either
   * periodically (see {@link PeriodicDatabasePoll}, {@link #startPollingDatabasePeriodically}, {@link
   * #stopPollingDatabasePeriodically}) or "on demand" (see {@link OnDemandDatabasePoll}), when one of the methods that
   * accesses {@link #dataSourcesSnapshot}'s state (such as {@link #getImmutableDataSourceWithUsedSegments}) is
   * called when the Coordinator is not the leader and therefore SqlSegmentsMetadata isn't polling the database
   * periodically.
   *
   * Note that if there is a happens-before relationship between a call to {@link #startPollingDatabasePeriodically()}
   * (on Coordinators' leadership change) and one of the methods accessing the {@link #dataSourcesSnapshot}'s state in
   * this class the latter is guaranteed to await for the initiated periodic poll. This is because when the latter
   * method calls to {@link #awaitLatestDatabasePoll()} via {@link #awaitOrPerformDatabasePoll}, they will
   * see the latest {@link PeriodicDatabasePoll} value (stored in this field, latestDatabasePoll, in {@link
   * #startPollingDatabasePeriodically()}) and to await on its {@link PeriodicDatabasePoll#firstPollCompletionFuture}.
   *
   * However, the guarantee explained above doesn't make any actual semantic difference, because on both periodic and
   * on-demand database polls the same invariant is maintained that the results not older than {@link
   * #periodicPollDelay} are used. The main difference is in performance: since on-demand polls are irregular and happen
   * in the context of the thread wanting to access the {@link #dataSourcesSnapshot}, that may cause delays in the
   * logic. On the other hand, periodic polls are decoupled into {@link #exec} and {@link
   * #dataSourcesSnapshot}-accessing methods should be generally "wait free" for database polls.
   *
   * The notion and the complexity of "on demand" database polls was introduced to simplify the interface of {@link
   * MetadataSegmentManager} and guarantee that it always returns consistent and relatively up-to-date data from methods
   * like {@link #getImmutableDataSourceWithUsedSegments}, while avoiding excessive repetitive polls. The last part
   * is achieved via "hooking on" other polls by awaiting on {@link PeriodicDatabasePoll#firstPollCompletionFuture} or
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
  @GuardedBy("startStopPollLock")
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
  @GuardedBy("startStopPollLock")
  private long currentStartPollingOrder = -1;

  @GuardedBy("startStopPollLock")
  private @Nullable ScheduledExecutorService exec = null;

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
      if (exec != null) {
        return; // Already started
      }
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
        log.makeAlert(t, "Uncaught exception in %s's polling thread", SQLMetadataSegmentManager.class).emit();
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

      // NOT nulling dataSourcesSnapshot, allowing to query the latest polled data even when this SegmentsMetadata
      // object is stopped.

      currentStartPollingOrder = -1;
    }
    finally {
      lock.unlock();
    }
  }

  private void awaitOrPerformDatabasePoll()
  {
    // Double-checked locking with awaitLatestDatabasePoll() call playing the role of the "check".
    if (awaitLatestDatabasePoll()) {
      return;
    }
    ReentrantReadWriteLock.WriteLock lock = startStopPollLock.writeLock();
    lock.lock();
    try {
      if (awaitLatestDatabasePoll()) {
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

  /**
   * If the latest {@link DatabasePoll} is a {@link PeriodicDatabasePoll}, or an {@link OnDemandDatabasePoll} that is
   * made not longer than {@link #periodicPollDelay} from now, awaits for it and returns true; returns false otherwise,
   * meaning that a new on-demand database poll should be initiated.
   */
  private boolean awaitLatestDatabasePoll()
  {
    DatabasePoll latestDatabasePoll = this.latestDatabasePoll;
    if (latestDatabasePoll instanceof PeriodicDatabasePoll) {
      Futures.getUnchecked(((PeriodicDatabasePoll) latestDatabasePoll).firstPollCompletionFuture);
      return true;
    }
    if (latestDatabasePoll instanceof OnDemandDatabasePoll) {
      long periodicPollDelayNanos = TimeUnit.MILLISECONDS.toNanos(periodicPollDelay.getMillis());
      OnDemandDatabasePoll latestOnDemandPoll = (OnDemandDatabasePoll) latestDatabasePoll;
      boolean latestUpdateIsFresh = latestOnDemandPoll.nanosElapsedFromInitiation() < periodicPollDelayNanos;
      if (latestUpdateIsFresh) {
        Futures.getUnchecked(latestOnDemandPoll.pollCompletionFuture);
        return true;
      }
      // Latest on-demand update is not fresh. Fall through to return false from this method.
    } else {
      assert latestDatabasePoll == null;
      // No periodic updates and no on-demand database poll have been done yet, nothing to await for.
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

    return markNonOvershadowedSegmentsAsUsed(unusedSegmentsInInterval, versionedIntervalTimeline);
  }

  private static void consume(Iterator<?> iterator)
  {
    while (iterator.hasNext()) {
      iterator.next();
    }
  }

  private int markNonOvershadowedSegmentsAsUsed(
      List<DataSegment> unusedSegments,
      VersionedIntervalTimeline<String, DataSegment> timeline
  )
  {
    List<String> segmentIdsToMarkAsUsed = new ArrayList<>();
    for (DataSegment segment : unusedSegments) {
      if (timeline.isOvershadowed(segment.getInterval(), segment.getVersion(), segment)) {
        continue;
      }
      segmentIdsToMarkAsUsed.add(segment.getId().toString());
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
                List<DataSegment> unusedSegments = retrieveUnusedSegments(dataSource, segmentIds, handle);
                List<Interval> unusedSegmentsIntervals = JodaUtils.condenseIntervals(
                    unusedSegments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
                );
                Iterator<DataSegment> usedSegmentsOverlappingUnusedSegmentsIntervals =
                    retrieveUsedSegmentsOverlappingIntervals(dataSource, unusedSegmentsIntervals, handle);
                VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(
                    Iterators.concat(usedSegmentsOverlappingUnusedSegmentsIntervals, unusedSegments.iterator())
                );
                return new Pair<>(unusedSegments, timeline);
              }
          );

      List<DataSegment> unusedSegments = unusedSegmentsAndTimeline.lhs;
      VersionedIntervalTimeline<String, DataSegment> timeline = unusedSegmentsAndTimeline.rhs;
      return markNonOvershadowedSegmentsAsUsed(unusedSegments, timeline);
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

  private List<DataSegment> retrieveUnusedSegments(
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

  private Iterator<DataSegment> retrieveUsedSegmentsOverlappingIntervals(
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

      return numUpdatedDatabaseEntries;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking all segments as unused in data source [%s]", dataSource);
      throw e;
    }
  }

  /**
   * This method does not update {@link #dataSourcesSnapshot}, see the comments in {@link #doPoll()} about
   * snapshot update. The update of the segment's state will be reflected after the next {@link DatabasePoll}.
   */
  @Override
  public boolean markSegmentAsUnused(final String segmentId)
  {
    try {
      return markSegmentAsUnusedInDatabase(segmentId);
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
        return computeNumChangedSegments(segmentIdList, segmentChanges);
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
  public @Nullable ImmutableDruidDataSource getImmutableDataSourceWithUsedSegments(String dataSourceName)
  {
    return getSnapshotOfDataSourcesWithAllUsedSegments().getDataSource(dataSourceName);
  }

  @Override
  public Collection<ImmutableDruidDataSource> getImmutableDataSourcesWithAllUsedSegments()
  {
    return getSnapshotOfDataSourcesWithAllUsedSegments().getDataSourcesWithAllUsedSegments();
  }

  @Override
  public Set<SegmentId> getOvershadowedSegments()
  {
    return getSnapshotOfDataSourcesWithAllUsedSegments().getOvershadowedSegments();
  }

  @Override
  public DataSourcesSnapshot getSnapshotOfDataSourcesWithAllUsedSegments()
  {
    awaitOrPerformDatabasePoll();
    return dataSourcesSnapshot;
  }

  @Override
  public Iterable<DataSegment> iterateAllUsedSegments()
  {
    awaitOrPerformDatabasePoll();
    return () -> dataSourcesSnapshot
        .getDataSourcesWithAllUsedSegments()
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
                          // If one entry in database is corrupted doPoll() should continue to work overall. See
                          // filter by `Objects::nonNull` below in this method.
                          return null;
                        }
                      }
                    }
                )
                .list();
          }
        }
    );

    Preconditions.checkNotNull(
        segments,
        "Unexpected 'null' when polling segments from the db, aborting snapshot update."
    );

    // dataSourcesSnapshot is updated only here and the DataSourcesSnapshot object is immutable. If data sources or
    // segments are marked as used or unused directly (via markAs...() methods in MetadataSegmentManager), the
    // dataSourcesSnapshot can become invalid until the next database poll.
    // DataSourcesSnapshot computes the overshadowed segments, which makes it an expensive operation if the
    // snapshot was invalidated on each segment mark as unused or used, especially if a user issues a lot of single
    // segment mark calls in rapid succession. So the snapshot update is not done outside of database poll at this time.
    // Updates outside of database polls were primarily for the user experience, so users would immediately see the
    // effect of a segment mark call reflected in MetadataResource API calls.

    ImmutableMap<String, String> dataSourceProperties = createDefaultDataSourceProperties();
    if (segments.isEmpty()) {
      log.info("No segments found in the database!");
    } else {
      log.info("Polled and found %,d segments in the database", segments.size());
    }
    dataSourcesSnapshot = DataSourcesSnapshot.fromUsedSegments(
        Iterables.filter(segments, Objects::nonNull), // Filter corrupted entries (see above in this method).
        dataSourceProperties
    );
  }

  private static ImmutableMap<String, String> createDefaultDataSourceProperties()
  {
    return ImmutableMap.of("created", DateTimes.nowUtc().toString());
  }

  /**
   * For the garbage collector in Java, it's better to keep new objects short-living, but once they are old enough
   * (i. e. promoted to old generation), try to keep them alive. In {@link #poll()}, we fetch and deserialize all
   * existing segments each time, and then replace them in {@link #dataSourcesSnapshot}. This method allows to use
   * already existing (old) segments when possible, effectively interning them a-la {@link String#intern} or {@link
   * com.google.common.collect.Interner}, aiming to make the majority of {@link DataSegment} objects garbage soon after
   * they are deserialized and to die in young generation. It allows to avoid fragmentation of the old generation and
   * full GCs.
   */
  private DataSegment replaceWithExistingSegmentIfPresent(DataSegment segment)
  {
    @MonotonicNonNull DataSourcesSnapshot dataSourcesSnapshot = this.dataSourcesSnapshot;
    if (dataSourcesSnapshot == null) {
      return segment;
    }
    @Nullable ImmutableDruidDataSource dataSource = dataSourcesSnapshot.getDataSource(segment.getDataSource());
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
