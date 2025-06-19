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
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of {@link SegmentsMetadataManager}, that periodically polls
 * used segments from the metadata store to build a {@link DataSourcesSnapshot}.
 */
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
  private final ObjectReader segmentReader;
  private final Duration periodicPollDelay;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;
  private final SegmentSchemaCache segmentSchemaCache;
  private final ServiceEmitter serviceEmitter;
  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

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
   * accesses {@link #dataSourcesSnapshot}'s state is
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
   * SegmentsMetadataManager} and guarantee that it always returns consistent and relatively up-to-date data,
   * while avoiding excessive repetitive polls. The last part
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

  public SqlSegmentsMetadataManager(
      ObjectMapper jsonMapper,
      Supplier<SegmentsMetadataManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      SQLMetadataConnector connector,
      SegmentSchemaCache segmentSchemaCache,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig,
      ServiceEmitter serviceEmitter
  )
  {
    this.jsonMapper = jsonMapper;
    this.segmentReader = jsonMapper.readerFor(DataSegment.class);
    this.periodicPollDelay = config.get().getPollDuration().toStandardDuration();
    this.dbTables = dbTables;
    this.connector = connector;
    this.segmentSchemaCache = segmentSchemaCache;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
    this.serviceEmitter = serviceEmitter;
  }

  @Override
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

  @Override
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
        this::populateUsedFlagLastUpdated
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
    final String segmentsTable = getSegmentsTable();
    log.info(
        "Populating column 'used_status_last_updated' with non-NULL values for unused segments in table[%s].",
        segmentsTable
    );

    final int batchSize = 100;
    int totalUpdatedEntries = 0;

    // Update the rows in batches of size 100
    while (true) {
      final List<String> segmentsToUpdate = new ArrayList<>(batchSize);
      int numUpdatedRows;
      try {
        connector.retryWithHandle(
            handle -> {
              segmentsToUpdate.addAll(handle.createQuery(
                  StringUtils.format(
                      "SELECT id FROM %1$s WHERE used_status_last_updated IS NULL and used = :used %2$s",
                      segmentsTable,
                      connector.limitClause(batchSize)
                  )
              ).bind("used", false).mapTo(String.class).list());
              return null;
            }
        );

        if (segmentsToUpdate.isEmpty()) {
          break;
        }

        numUpdatedRows = connector.retryWithHandle(
            handle -> {
              final Batch updateBatch = handle.createBatch();
              final String sql = "UPDATE %1$s SET used_status_last_updated = '%2$s' WHERE id = '%3$s'";
              String now = DateTimes.nowUtc().toString();
              for (String id : segmentsToUpdate) {
                updateBatch.add(StringUtils.format(sql, segmentsTable, now, id));
              }
              int[] results = updateBatch.execute();
              return Arrays.stream(results).sum();
            }
        );
        totalUpdatedEntries += numUpdatedRows;
      }
      catch (Exception e) {
        log.warn(e, "Populating column 'used_status_last_updated' in table[%s] has failed. There may be unused segments with"
                    + " NULL values for 'used_status_last_updated' that won't be killed!", segmentsTable);
        return;
      }

      log.debug(
          "Updated a batch of [%d] rows in table[%s] with a valid used_status_last_updated date",
          segmentsToUpdate.size(), segmentsTable
      );

      // Do not wait if there are no more segments to update
      if (segmentsToUpdate.size() == numUpdatedRows && numUpdatedRows < batchSize) {
        break;
      }

      // Wait for some time before processing the next batch
      try {
        Thread.sleep(10000);
      }
      catch (InterruptedException e) {
        log.info("Interrupted, exiting!");
        Thread.currentThread().interrupt();
      }
    }
    log.info(
        "Populated column 'used_status_last_updated' in table[%s] in [%d] rows.",
        segmentsTable, totalUpdatedEntries
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
  public DataSourcesSnapshot getRecentDataSourcesSnapshot()
  {
    useLatestIfWithinDelayOrPerformNewDatabasePoll();
    return dataSourcesSnapshot;
  }

  @Override
  public DataSourcesSnapshot forceUpdateDataSourcesSnapshot()
  {
    forceOrWaitOngoingDatabasePoll();
    return dataSourcesSnapshot;
  }

  /**
   * Returns the last snapshot built by the manager. This method always returns
   * immediately, even if the last snapshot is older than the poll period.
   */
  @VisibleForTesting
  DataSourcesSnapshot getLatestDataSourcesSnapshot()
  {
    return dataSourcesSnapshot;
  }

  @VisibleForTesting
  DatabasePoll getLatestDatabasePoll()
  {
    return latestDatabasePoll;
  }

  @VisibleForTesting
  void poll()
  {
    // See the comment to the pollLock field, explaining this synchronized block
    synchronized (pollLock) {
      if (centralizedDatasourceSchemaConfig.isEnabled()) {
        pollSegmentAndSchema();
      } else {
        pollSegments();
      }
    }
  }

  private void pollSegments()
  {
    final DateTime startTime = DateTimes.nowUtc();
    final Stopwatch stopwatch = Stopwatch.createStarted();

    // Some databases such as PostgreSQL require auto-commit turned off
    // to stream results back, enabling transactions disables auto-commit
    // setting connection to read-only will allow some database such as MySQL
    // to automatically use read-only transaction mode, further optimizing the query
    final List<DataSegment> segments = connector.inReadOnlyTransaction(
        (handle, status) -> handle
            .createQuery(StringUtils.format("SELECT payload FROM %s WHERE used=true", getSegmentsTable()))
            .setFetchSize(connector.getStreamingFetchSize())
            .map((index, r, ctx) -> {
              try {
                DataSegment segment = segmentReader.readValue(r.getBytes("payload"));
                return replaceWithExistingSegmentIfPresent(segment);
              }
              catch (IOException e) {
                log.makeAlert(e, "Failed to read segment from db.").emit();
                // If one entry in database is corrupted doPoll() should continue to work overall. See
                // filter by `Objects::nonNull` below in this method.
                return null;
              }
            }).list()
    );

    Preconditions.checkNotNull(
        segments,
        "Unexpected 'null' when polling segments from the db, aborting snapshot update."
    );

    stopwatch.stop();
    emitMetric("segment/poll/time", stopwatch.millisElapsed());
    /*log.info(
        "Polled and found [%,d] segments in the database in [%,d]ms.",
        segments.size(), stopwatch.millisElapsed()
    );*/

    createDatasourcesSnapshot(startTime, segments);
  }

  private void pollSegmentAndSchema()
  {
    final DateTime startTime = DateTimes.nowUtc();
    final Stopwatch stopwatch = Stopwatch.createStarted();

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentMetadataBuilder = new ImmutableMap.Builder<>();

    // Collect and emit stats for the schema cache before every DB poll
    segmentSchemaCache.getStats().forEach(this::emitMetric);

    // some databases such as PostgreSQL require auto-commit turned off
    // to stream results back, enabling transactions disables auto-commit
    //
    // setting connection to read-only will allow some database such as MySQL
    // to automatically use read-only transaction mode, further optimizing the query
    final List<DataSegment> segments = connector.inReadOnlyTransaction(
        new TransactionCallback<>()
        {
          @Override
          public List<DataSegment> inTransaction(Handle handle, TransactionStatus status)
          {
            return handle
                .createQuery(StringUtils.format("SELECT payload, schema_fingerprint, num_rows FROM %s WHERE used=true", getSegmentsTable()))
                .setFetchSize(connector.getStreamingFetchSize())
                .map(
                    (index, r, ctx) -> {
                      try {
                        DataSegment segment = jsonMapper.readValue(r.getBytes("payload"), DataSegment.class);

                        Long numRows = (Long) r.getObject("num_rows");
                        String schemaFingerprint = r.getString("schema_fingerprint");

                        if (schemaFingerprint != null && numRows != null) {
                          segmentMetadataBuilder.put(
                              segment.getId(),
                              new SegmentMetadata(numRows, schemaFingerprint)
                          );
                        }
                        return replaceWithExistingSegmentIfPresent(segment);
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to read segment from db.").emit();
                        // If one entry in database is corrupted doPoll() should continue to work overall. See
                        // filter by `Objects::nonNull` below in this method.
                        return null;
                      }
                    }
                )
                .list();
          }
        }
    );

    ImmutableMap.Builder<String, SchemaPayload> schemaMapBuilder = new ImmutableMap.Builder<>();

    final String schemaPollQuery =
        StringUtils.format(
            "SELECT fingerprint, payload FROM %s WHERE version = %s",
            getSegmentSchemaTable(),
            CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
        );

    connector.inReadOnlyTransaction(
        (handle, status) -> {
          handle.createQuery(schemaPollQuery)
                .setFetchSize(connector.getStreamingFetchSize())
                .map((index, r, ctx) -> {
                  try {
                    schemaMapBuilder.put(
                        r.getString("fingerprint"),
                        jsonMapper.readValue(r.getBytes("payload"), SchemaPayload.class)
                    );
                  }
                  catch (IOException e) {
                    log.makeAlert(e, "Failed to read schema from db.").emit();
                  }
                  return null;
                }).list();

          return null;
        });

    ImmutableMap<String, SchemaPayload> schemaMap = schemaMapBuilder.build();
    segmentSchemaCache.resetSchemaForPublishedSegments(segmentMetadataBuilder.build(), schemaMap);

    Preconditions.checkNotNull(
        segments,
        "Unexpected 'null' when polling segments from the db, aborting snapshot update."
    );

    stopwatch.stop();
    emitMetric("segment/pollWithSchema/time", stopwatch.millisElapsed());
    log.info(
        "Polled and found [%,d] segments and [%,d] schemas in the database in [%,d]ms.",
        segments.size(), schemaMap.size(), stopwatch.millisElapsed()
    );

    createDatasourcesSnapshot(startTime, segments);
  }

  private void emitMetric(String metricName, long value)
  {
    serviceEmitter.emit(new ServiceMetricEvent.Builder().setMetric(metricName, value));
  }

  private void createDatasourcesSnapshot(DateTime snapshotTime, List<DataSegment> segments)
  {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    // dataSourcesSnapshot is updated only here and the DataSourcesSnapshot object is immutable. If data sources or
    // segments are marked as used or unused directly (via markAs...() methods in SegmentsMetadataManager), the
    // dataSourcesSnapshot can become invalid until the next database poll.
    // DataSourcesSnapshot computes the overshadowed segments, which makes it an expensive operation if the
    // snapshot was invalidated on each segment mark as unused or used, especially if a user issues a lot of single
    // segment mark calls in rapid succession. So the snapshot update is not done outside of database poll at this time.
    // Updates outside of database polls were primarily for the user experience, so users would immediately see the
    // effect of a segment mark call reflected in MetadataResource API calls.

    dataSourcesSnapshot = DataSourcesSnapshot.fromUsedSegments(
        Iterables.filter(segments, Objects::nonNull), // Filter corrupted entries (see above in this method).
        snapshotTime
    );
    emitMetric("segment/buildSnapshot/time", stopwatch.millisElapsed());
    log.debug(
        "Created snapshot from polled segments in [%d]ms. Found [%d] overshadowed segments.",
        stopwatch.millisElapsed(), dataSourcesSnapshot.getOvershadowedSegments().size()
    );
  }

  /**
   * For the garbage collector in Java, it's better to keep new objects short-living, but once they are old enough
   * (i.e. promoted to old generation), try to keep them alive. In {@link #poll()}, we fetch and deserialize all
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

  private String getSegmentSchemaTable()
  {
    return dbTables.get().getSegmentSchemasTable();
  }
}
