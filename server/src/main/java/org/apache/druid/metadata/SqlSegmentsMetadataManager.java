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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
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
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class SqlSegmentsMetadataManager implements SegmentsMetadataManager
{
  private static final EmittingLogger log = new EmittingLogger(SqlSegmentsMetadataManager.class);

  /**
   * Marker interface for objects stored in {@link #latestDatabasePoll}. See the comment for that field for details.
   */
  private interface DatabasePoll
  {}

  /** Represents periodic {@link #poll}s happening from {@link #exec}. */
  @VisibleForTesting
  static class PeriodicDatabasePoll implements DatabasePoll
  {
    /**
     * This future allows to wait until {@link #dataSourcesSnapshot} is initialized in the first {@link #poll()}
     * happening since {@link #startPollingDatabasePeriodically()} is called for the first time, or since the last
     * visible (in happens-before terms) call to {@link #startPollingDatabasePeriodically()} in case of Coordinator's
     * leadership changes.
     */
    final CompletableFuture<Void> firstPollCompletionFuture = new CompletableFuture<>();
    long lastPollStartTimestampInMs = -1;
  }

  /**
   * Represents on-demand {@link #poll} initiated at periods of time when SqlSegmentsMetadataManager doesn't poll the database
   * periodically.
   */
  @VisibleForTesting
  static class OnDemandDatabasePoll implements DatabasePoll
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
   * This lock is also used to synchronize {@link #useLatestIfWithinDelayOrPerformNewDatabasePoll} for times when SqlSegmentsMetadataManager
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
   * #useLatestIfWithinDelayOrPerformNewDatabasePoll()} via one of the public methods of SqlSegmentsMetadataManager.
   */
  private volatile @MonotonicNonNull DataSourcesSnapshot dataSourcesSnapshot = null;

  /**
   * The latest {@link DatabasePoll} represent {@link #poll()} calls which update {@link #dataSourcesSnapshot}, either
   * periodically (see {@link PeriodicDatabasePoll}, {@link #startPollingDatabasePeriodically}, {@link
   * #stopPollingDatabasePeriodically}) or "on demand" (see {@link OnDemandDatabasePoll}), when one of the methods that
   * accesses {@link #dataSourcesSnapshot}'s state (such as {@link #getImmutableDataSourceWithUsedSegments}) is
   * called when the Coordinator is not the leader and therefore SqlSegmentsMetadataManager isn't polling the database
   * periodically.
   *
   * Note that if there is a happens-before relationship between a call to {@link #startPollingDatabasePeriodically()}
   * (on Coordinators' leadership change) and one of the methods accessing the {@link #dataSourcesSnapshot}'s state in
   * this class the latter is guaranteed to await for the initiated periodic poll. This is because when the latter
   * method calls to {@link #useLatestSnapshotIfWithinDelay()} via {@link #useLatestIfWithinDelayOrPerformNewDatabasePoll}, they will
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
   * SegmentsMetadataManager} and guarantee that it always returns consistent and relatively up-to-date data from methods
   * like {@link #getImmutableDataSourceWithUsedSegments}, while avoiding excessive repetitive polls. The last part
   * is achieved via "hooking on" other polls by awaiting on {@link PeriodicDatabasePoll#firstPollCompletionFuture} or
   * {@link OnDemandDatabasePoll#pollCompletionFuture}, see {@link #useLatestIfWithinDelayOrPerformNewDatabasePoll} method
   * implementation for details.
   *
   * Note: the overall implementation of periodic/on-demand polls is not completely optimal: for example, when the
   * Coordinator just stopped leading, the latest periodic {@link #poll} (which is still "fresh") is not considered
   * and a new on-demand poll is always initiated. This is done to simplify the implementation, while the efficiency
   * during Coordinator leadership switches is not a priority.
   *
   * This field is {@code volatile} because it's checked and updated in a double-checked locking manner in {@link
   * #useLatestIfWithinDelayOrPerformNewDatabasePoll()}.
   */
  private volatile @Nullable DatabasePoll latestDatabasePoll = null;

  /** Used to cancel periodic poll task in {@link #stopPollingDatabasePeriodically}. */
  @GuardedBy("startStopPollLock")
  private @Nullable Future<?> periodicPollTaskFuture = null;

  /** The number of times {@link #startPollingDatabasePeriodically} was called. */
  @GuardedBy("startStopPollLock")
  private long startPollingCount = 0;

  /**
   * Equal to the current {@link #startPollingCount} value if the SqlSegmentsMetadataManager is currently started; -1 if
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

  private Future<?> usedFlagLastUpdatedPopulationFuture;

  @Inject
  public SqlSegmentsMetadataManager(
      ObjectMapper jsonMapper,
      Supplier<SegmentsMetadataManagerConfig> config,
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
   * be executed just once for an instance of SqlSegmentsMetadataManager.
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
      exec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(getClass().getName()) + "-Exec--%d");
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * Don't confuse this method with {@link #stopPollingDatabasePeriodically}. This is a lifecycle stopping method to
   * be executed just once for an instance of SqlSegmentsMetadataManager.
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

      PeriodicDatabasePoll periodicDatabasePoll = new PeriodicDatabasePoll();
      latestDatabasePoll = periodicDatabasePoll;

      startPollingCount++;
      currentStartPollingOrder = startPollingCount;
      final long localStartOrder = currentStartPollingOrder;

      periodicPollTaskFuture = exec.scheduleWithFixedDelay(
          createPollTaskForStartOrder(localStartOrder, periodicDatabasePoll),
          0,
          periodicPollDelay.getMillis(),
          TimeUnit.MILLISECONDS
      );
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public void stopAsyncUsedFlagLastUpdatedUpdate()
  {
    if (!usedFlagLastUpdatedPopulationFuture.isDone() && !usedFlagLastUpdatedPopulationFuture.isCancelled()) {
      usedFlagLastUpdatedPopulationFuture.cancel(true);
    }
  }

  @Override
  public void populateUsedFlagLastUpdatedAsync()
  {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    usedFlagLastUpdatedPopulationFuture = executorService.submit(
        () -> populateUsedFlagLastUpdated()
    );
  }

  /**
   * Populate used_status_last_updated for unused segments whose current value for said column is NULL
   *
   * The updates are made incrementally.
   */
  @VisibleForTesting
  void populateUsedFlagLastUpdated()
  {
    String segmentsTable = getSegmentsTable();
    log.info(
        "Populating used_status_last_updated with non-NULL values for unused segments in [%s]",
        segmentsTable
    );

    int limit = 100;
    int totalUpdatedEntries = 0;

    while (true) {
      List<String> segmentsToUpdate = new ArrayList<>(100);
      try {
        connector.retryWithHandle(
            new HandleCallback<Void>()
            {
              @Override
              public Void withHandle(Handle handle)
              {
                segmentsToUpdate.addAll(handle.createQuery(
                    StringUtils.format(
                        "SELECT id FROM %1$s WHERE used_status_last_updated IS NULL and used = :used %2$s",
                        segmentsTable,
                        connector.limitClause(limit)
                    )
                ).bind("used", false).mapTo(String.class).list());
                return null;
              }
            }
        );

        if (segmentsToUpdate.isEmpty()) {
          // We have no segments to process
          break;
        }

        connector.retryWithHandle(
            new HandleCallback<Void>()
            {
              @Override
              public Void withHandle(Handle handle)
              {
                Batch updateBatch = handle.createBatch();
                String sql = "UPDATE %1$s SET used_status_last_updated = '%2$s' WHERE id = '%3$s'";
                String now = DateTimes.nowUtc().toString();
                for (String id : segmentsToUpdate) {
                  updateBatch.add(StringUtils.format(sql, segmentsTable, now, id));
                }
                updateBatch.execute();
                return null;
              }
            }
        );
      }
      catch (Exception e) {
        log.warn(e, "Population of used_status_last_updated in [%s] has failed. There may be unused segments with"
                    + " NULL values for used_status_last_updated that won't be killed!", segmentsTable);
        return;
      }

      totalUpdatedEntries += segmentsToUpdate.size();
      log.info("Updated a batch of %d rows in [%s] with a valid used_status_last_updated date",
               segmentsToUpdate.size(),
               segmentsTable
      );
      try {
        Thread.sleep(10000);
      }
      catch (InterruptedException e) {
        log.info("Interrupted, exiting!");
        Thread.currentThread().interrupt();
      }
    }
    log.info(
        "Finished updating [%s] with a valid used_status_last_updated date. %d rows updated",
        segmentsTable,
        totalUpdatedEntries
    );
  }

  private Runnable createPollTaskForStartOrder(long startOrder, PeriodicDatabasePoll periodicDatabasePoll)
  {
    return () -> {
      // If latest poll was an OnDemandDatabasePoll that started less than periodicPollDelay,
      // We will wait for (periodicPollDelay - currentTime - LatestOnDemandDatabasePollStartTime) then check again.
      try {
        long periodicPollDelayNanos = TimeUnit.MILLISECONDS.toNanos(periodicPollDelay.getMillis());
        while (latestDatabasePoll != null
               && latestDatabasePoll instanceof OnDemandDatabasePoll
               && ((OnDemandDatabasePoll) latestDatabasePoll).nanosElapsedFromInitiation() < periodicPollDelayNanos) {
          long sleepNano = periodicPollDelayNanos
                           - ((OnDemandDatabasePoll) latestDatabasePoll).nanosElapsedFromInitiation();
          TimeUnit.NANOSECONDS.sleep(sleepNano);
        }
      }
      catch (Exception e) {
        log.debug(e, "Exception found while waiting for next periodic poll");
      }

      // poll() is synchronized together with startPollingDatabasePeriodically(), stopPollingDatabasePeriodically() and
      // isPollingDatabasePeriodically() to ensure that when stopPollingDatabasePeriodically() exits, poll() won't
      // actually run anymore after that (it could only enter the synchronized section and exit immediately because the
      // localStartedOrder doesn't match the new currentStartPollingOrder). It's needed to avoid flakiness in
      // SqlSegmentsMetadataManagerTest. See https://github.com/apache/druid/issues/6028
      ReentrantReadWriteLock.ReadLock lock = startStopPollLock.readLock();
      lock.lock();
      try {
        if (startOrder == currentStartPollingOrder) {
          periodicDatabasePoll.lastPollStartTimestampInMs = System.currentTimeMillis();
          poll();
          periodicDatabasePoll.firstPollCompletionFuture.complete(null);
          latestDatabasePoll = periodicDatabasePoll;
        } else {
          log.debug("startOrder = currentStartPollingOrder = %d, skipping poll()", startOrder);
        }
      }
      catch (Throwable t) {
        log.makeAlert(t, "Uncaught exception in %s's polling thread", SqlSegmentsMetadataManager.class).emit();
        // Swallow the exception, so that scheduled polling goes on. Leave firstPollFutureSinceLastStart uncompleted
        // for now, so that it may be completed during the next poll.
        if (!(t instanceof Exception)) {
          // Don't try to swallow a Throwable which is not an Exception (that is, a Error).
          periodicDatabasePoll.firstPollCompletionFuture.completeExceptionally(t);
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

      // NOT nulling dataSourcesSnapshot, allowing to query the latest polled data even when this SegmentsMetadataManager
      // object is stopped.

      currentStartPollingOrder = -1;
    }
    finally {
      lock.unlock();
    }
  }

  private void useLatestIfWithinDelayOrPerformNewDatabasePoll()
  {
    // Double-checked locking with useLatestSnapshotIfWithinDelay() call playing the role of the "check".
    if (useLatestSnapshotIfWithinDelay()) {
      return;
    }
    ReentrantReadWriteLock.WriteLock lock = startStopPollLock.writeLock();
    lock.lock();
    try {
      if (useLatestSnapshotIfWithinDelay()) {
        return;
      }
      OnDemandDatabasePoll onDemandDatabasePoll = new OnDemandDatabasePoll();
      this.latestDatabasePoll = onDemandDatabasePoll;
      doOnDemandPoll(onDemandDatabasePoll);
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * This method returns true without waiting for database poll if the latest {@link DatabasePoll} is a
   * {@link PeriodicDatabasePoll} that has completed it's first poll, or an {@link OnDemandDatabasePoll} that is
   * made not longer than {@link #periodicPollDelay} from current time.
   * This method does wait untill completion for if the latest {@link DatabasePoll} is a
   * {@link PeriodicDatabasePoll} that has not completed it's first poll, or an {@link OnDemandDatabasePoll} that is
   * already in the process of polling the database.
   * This means that any method using this check can read from snapshot that is
   * up to {@link SqlSegmentsMetadataManager#periodicPollDelay} old.
   */
  @VisibleForTesting
  boolean useLatestSnapshotIfWithinDelay()
  {
    DatabasePoll latestDatabasePoll = this.latestDatabasePoll;
    if (latestDatabasePoll instanceof PeriodicDatabasePoll) {
      Futures.getUnchecked(((PeriodicDatabasePoll) latestDatabasePoll).firstPollCompletionFuture);
      return true;
    }
    if (latestDatabasePoll instanceof OnDemandDatabasePoll) {
      long periodicPollDelayNanos = TimeUnit.MILLISECONDS.toNanos(periodicPollDelay.getMillis());
      OnDemandDatabasePoll latestOnDemandPoll = (OnDemandDatabasePoll) latestDatabasePoll;
      boolean latestDatabasePollIsFresh = latestOnDemandPoll.nanosElapsedFromInitiation() < periodicPollDelayNanos;
      if (latestDatabasePollIsFresh) {
        Futures.getUnchecked(latestOnDemandPoll.pollCompletionFuture);
        return true;
      }
      // Latest on-demand poll is not fresh. Fall through to return false from this method.
    } else {
      assert latestDatabasePoll == null;
      // No periodic database polls and no on-demand poll have been done yet, nothing to await for.
    }
    return false;
  }

  /**
   * This method will always force a database poll if there is no ongoing database poll. This method will then
   * waits for the new poll or the ongoing poll to completes before returning.
   * This means that any method using this check can be sure that the latest poll for the snapshot was completed after
   * this method was called.
   */
  @VisibleForTesting
  void forceOrWaitOngoingDatabasePoll()
  {
    long checkStartTime = System.currentTimeMillis();
    ReentrantReadWriteLock.WriteLock lock = startStopPollLock.writeLock();
    lock.lock();
    try {
      DatabasePoll latestDatabasePoll = this.latestDatabasePoll;
      try {
        //Verify if there was a periodic poll completed while we were waiting for the lock
        if (latestDatabasePoll instanceof PeriodicDatabasePoll
            && ((PeriodicDatabasePoll) latestDatabasePoll).lastPollStartTimestampInMs > checkStartTime) {
          return;
        }
        // Verify if there was a on-demand poll completed while we were waiting for the lock
        if (latestDatabasePoll instanceof OnDemandDatabasePoll) {
          long checkStartTimeNanos = TimeUnit.MILLISECONDS.toNanos(checkStartTime);
          OnDemandDatabasePoll latestOnDemandPoll = (OnDemandDatabasePoll) latestDatabasePoll;
          if (latestOnDemandPoll.initiationTimeNanos > checkStartTimeNanos) {
            return;
          }
        }
      }
      catch (Exception e) {
        // Latest poll was unsuccessful, try to do a new poll
        log.debug(e, "Latest poll was unsuccessful. Starting a new poll...");
      }
      // Force a database poll
      OnDemandDatabasePoll onDemandDatabasePoll = new OnDemandDatabasePoll();
      this.latestDatabasePoll = onDemandDatabasePoll;
      doOnDemandPoll(onDemandDatabasePoll);
    }
    finally {
      lock.unlock();
    }
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
              .createStatement(StringUtils.format("UPDATE %s SET used=true, used_status_last_updated = :used_status_last_updated WHERE id = :id", getSegmentsTable()))
              .bind("id", segmentId)
              .bind("used_status_last_updated", DateTimes.nowUtc().toString())
              .execute()
      );
      // Unlike bulk markAsUsed methods: markAsUsedAllNonOvershadowedSegmentsInDataSource(),
      // markAsUsedNonOvershadowedSegmentsInInterval(), and markAsUsedNonOvershadowedSegments() we don't put the marked
      // segment into the respective data source, because we don't have it fetched from the database. It's probably not
      // worth complicating the implementation and making two database queries just to add the segment because it will
      // be anyway fetched during the next poll(). Segment putting that is done in the bulk markAsUsed methods is a nice
      // to have thing, but doesn't formally affects the external guarantees of SegmentsMetadataManager class.
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
    final List<DataSegment> unusedSegments = new ArrayList<>();
    final SegmentTimeline timeline = new SegmentTimeline();

    connector.inReadOnlyTransaction(
        (handle, status) -> {
          final SqlSegmentsMetadataQuery queryTool =
              SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables.get(), jsonMapper);

          final List<Interval> intervals =
              interval == null ? Intervals.ONLY_ETERNITY : Collections.singletonList(interval);

          try (final CloseableIterator<DataSegment> iterator =
                   queryTool.retrieveUsedSegments(dataSourceName, intervals)) {
            timeline.addSegments(iterator);
          }

          try (final CloseableIterator<DataSegment> iterator =
                   queryTool.retrieveUnusedSegments(dataSourceName, intervals, null, null, null, null)) {
            while (iterator.hasNext()) {
              final DataSegment dataSegment = iterator.next();
              timeline.addSegments(Iterators.singletonIterator(dataSegment));
              unusedSegments.add(dataSegment);
            }
          }

          //noinspection ReturnOfNull: This consumer operates by side effects
          return null;
        }
    );

    return markNonOvershadowedSegmentsAsUsed(unusedSegments, timeline);
  }

  private int markNonOvershadowedSegmentsAsUsed(
      List<DataSegment> unusedSegments,
      SegmentTimeline timeline
  )
  {
    List<SegmentId> segmentIdsToMarkAsUsed = new ArrayList<>();
    for (DataSegment segment : unusedSegments) {
      if (!timeline.isOvershadowed(segment)) {
        segmentIdsToMarkAsUsed.add(segment.getId());
      }
    }

    return markSegmentsAsUsed(segmentIdsToMarkAsUsed);
  }

  @Override
  public int markAsUsedNonOvershadowedSegments(final String dataSource, final Set<String> segmentIds)
      throws UnknownSegmentIdsException
  {
    try {
      Pair<List<DataSegment>, SegmentTimeline> unusedSegmentsAndTimeline = connector
          .inReadOnlyTransaction(
              (handle, status) -> {
                List<DataSegment> unusedSegments = retrieveUnusedSegments(dataSource, segmentIds, handle);
                List<Interval> unusedSegmentsIntervals = JodaUtils.condenseIntervals(
                    unusedSegments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
                );
                try (CloseableIterator<DataSegment> usedSegmentsOverlappingUnusedSegmentsIntervals =
                         retrieveUsedSegmentsOverlappingIntervals(dataSource, unusedSegmentsIntervals, handle)) {
                  SegmentTimeline timeline = SegmentTimeline.forSegments(
                      Iterators.concat(usedSegmentsOverlappingUnusedSegmentsIntervals, unusedSegments.iterator())
                  );
                  return new Pair<>(unusedSegments, timeline);
                }
              }
          );

      List<DataSegment> unusedSegments = unusedSegmentsAndTimeline.lhs;
      SegmentTimeline timeline = unusedSegmentsAndTimeline.rhs;
      return markNonOvershadowedSegmentsAsUsed(unusedSegments, timeline);
    }
    catch (Exception e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof UnknownSegmentIdsException) {
        throw (UnknownSegmentIdsException) rootCause;
      } else {
        throw e;
      }
    }
  }

  private List<DataSegment> retrieveUnusedSegments(
      final String dataSource,
      final Set<String> segmentIds,
      final Handle handle
  ) throws UnknownSegmentIdsException
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
      throw new UnknownSegmentIdsException(unknownSegmentIds);
    }
    return segments;
  }

  private CloseableIterator<DataSegment> retrieveUsedSegmentsOverlappingIntervals(
      final String dataSource,
      final Collection<Interval> intervals,
      final Handle handle
  )
  {
    return SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables.get(), jsonMapper)
                                   .retrieveUsedSegments(dataSource, intervals);
  }

  private int markSegmentsAsUsed(final List<SegmentId> segmentIds)
  {
    if (segmentIds.isEmpty()) {
      log.info("No segments found to update!");
      return 0;
    }

    return connector.getDBI().withHandle(
        handle ->
            SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables.get(), jsonMapper)
                                    .markSegments(segmentIds, true)
    );
  }

  @Override
  public int markAsUnusedAllSegmentsInDataSource(final String dataSource)
  {
    try {
      return connector.getDBI().withHandle(
          handle ->
              SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables.get(), jsonMapper)
                                      .markSegmentsUnused(dataSource, Intervals.ETERNITY)
      );
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
  public boolean markSegmentAsUnused(final SegmentId segmentId)
  {
    try {
      final int numSegments = connector.getDBI().withHandle(
          handle ->
              SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables.get(), jsonMapper)
                                      .markSegments(Collections.singletonList(segmentId), false)
      );

      return numSegments > 0;
    }
    catch (RuntimeException e) {
      log.error(e, "Exception marking segment [%s] as unused", segmentId);
      throw e;
    }
  }

  @Override
  public int markSegmentsAsUnused(Set<SegmentId> segmentIds)
  {
    return connector.getDBI().withHandle(
        handle ->
            SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables.get(), jsonMapper)
                                    .markSegments(segmentIds, false)
    );
  }

  @Override
  public int markAsUnusedSegmentsInInterval(String dataSourceName, Interval interval)
  {
    try {
      return connector.getDBI().withHandle(
          handle ->
              SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables.get(), jsonMapper)
                                      .markSegmentsUnused(dataSourceName, interval)
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
  public DataSourcesSnapshot getSnapshotOfDataSourcesWithAllUsedSegments()
  {
    useLatestIfWithinDelayOrPerformNewDatabasePoll();
    return dataSourcesSnapshot;
  }

  @VisibleForTesting
  DataSourcesSnapshot getDataSourcesSnapshot()
  {
    return dataSourcesSnapshot;
  }

  @VisibleForTesting
  DatabasePoll getLatestDatabasePoll()
  {
    return latestDatabasePoll;
  }


  @Override
  public Iterable<DataSegment> iterateAllUsedSegments()
  {
    useLatestIfWithinDelayOrPerformNewDatabasePoll();
    return dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot();
  }

  @Override
  public Optional<Iterable<DataSegment>> iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
      String datasource,
      Interval interval,
      boolean requiresLatest
  )
  {
    if (requiresLatest) {
      forceOrWaitOngoingDatabasePoll();
    } else {
      useLatestIfWithinDelayOrPerformNewDatabasePoll();
    }
    SegmentTimeline usedSegmentsTimeline
        = dataSourcesSnapshot.getUsedSegmentsTimelinesPerDataSource().get(datasource);
    return Optional.fromNullable(usedSegmentsTimeline)
                   .transform(timeline -> timeline.findNonOvershadowedObjectsInInterval(interval, Partitions.ONLY_COMPLETE));
  }

  /**
   * Retrieves segments and their associated metadata for a given datasource that are marked unused and that are
   * *fully contained by* an optionally specified interval. If the interval specified is null, this method will
   * retrieve all unused segments.
   *
   * This call does not return any information about realtime segments.
   *
   * @param datasource      The name of the datasource
   * @param interval        an optional interval to search over.
   * @param limit           an optional maximum number of results to return. If none is specified, the results are
   *                        not limited.
   * @param lastSegmentId an optional last segment id from which to search for results. All segments returned are >
   *                      this segment lexigraphically if sortOrder is null or  {@link SortOrder#ASC}, or < this
   *                      segment lexigraphically if sortOrder is {@link SortOrder#DESC}. If none is specified, no
   *                      such filter is used.
   * @param sortOrder an optional order with which to return the matching segments by id, start time, end time. If
   *                  none is specified, the order of the results is not guarenteed.

   * Returns an iterable.
   */
  @Override
  public Iterable<DataSegmentPlus> iterateAllUnusedSegmentsForDatasource(
      final String datasource,
      @Nullable final Interval interval,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder
  )
  {
    return connector.inReadOnlyTransaction(
        (handle, status) -> {
          final SqlSegmentsMetadataQuery queryTool =
              SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables.get(), jsonMapper);

          final List<Interval> intervals =
              interval == null
                  ? Intervals.ONLY_ETERNITY
                  : Collections.singletonList(interval);
          try (final CloseableIterator<DataSegmentPlus> iterator =
                   queryTool.retrieveUnusedSegmentsPlus(datasource, intervals, limit, lastSegmentId, sortOrder, null)) {
            return ImmutableList.copyOf(iterator);
          }
        }
    );
  }

  @Override
  public Set<String> retrieveAllDataSourceNames()
  {
    return connector.getDBI().withHandle(
        handle -> handle
            .createQuery(StringUtils.format("SELECT DISTINCT(datasource) FROM %s", getSegmentsTable()))
            .fold(
                new HashSet<>(),
                (Set<String> druidDataSources,
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
    // segments are marked as used or unused directly (via markAs...() methods in SegmentsMetadataManager), the
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
  public List<Interval> getUnusedSegmentIntervals(
      final String dataSource,
      @Nullable final DateTime minStartTime,
      final DateTime maxEndTime,
      final int limit,
      DateTime maxUsedStatusLastUpdatedTime
  )
  {
    // Note that we handle the case where used_status_last_updated IS NULL here to allow smooth transition to Druid version that uses used_status_last_updated column
    return connector.inReadOnlyTransaction(
        new TransactionCallback<List<Interval>>()
        {
          @Override
          public List<Interval> inTransaction(Handle handle, TransactionStatus status)
          {
            final Query<Interval> sql = handle
                .createQuery(
                    StringUtils.format(
                        "SELECT start, %2$send%2$s FROM %1$s WHERE dataSource = :dataSource AND "
                        + "%2$send%2$s <= :end AND used = false AND used_status_last_updated IS NOT NULL AND used_status_last_updated <= :used_status_last_updated %3$s ORDER BY start, %2$send%2$s",
                        getSegmentsTable(),
                        connector.getQuoteString(),
                        null != minStartTime ? "AND start >= :start" : ""
                    )
                )
                .setFetchSize(connector.getStreamingFetchSize())
                .setMaxRows(limit)
                .bind("dataSource", dataSource)
                .bind("end", maxEndTime.toString())
                .bind("used_status_last_updated", maxUsedStatusLastUpdatedTime.toString())
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
                );
            if (null != minStartTime) {
              sql.bind("start", minStartTime.toString());
            }

            Iterator<Interval> iter = sql.iterator();

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
