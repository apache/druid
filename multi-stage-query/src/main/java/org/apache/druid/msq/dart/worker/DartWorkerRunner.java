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

package org.apache.druid.msq.dart.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.messages.server.MessageRelayResource;
import org.apache.druid.messages.server.MessageRelayResource.PollListener;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.msq.dart.controller.DartWorkerSyncer;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.dart.guice.DartWorkerConfig;
import org.apache.druid.msq.dart.worker.http.DartWorkerInfo;
import org.apache.druid.msq.dart.worker.http.GetWorkersResponse;
import org.apache.druid.msq.dart.worker.http.SyncWorkersRequest;
import org.apache.druid.msq.dart.worker.http.SyncWorkersResponse;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerImpl;
import org.apache.druid.msq.exec.WorkerRunRef;
import org.apache.druid.msq.rpc.ResourcePermissionMapper;
import org.apache.druid.msq.rpc.WorkerResource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

@ManageLifecycle
public class DartWorkerRunner
{
  private static final Logger log = new Logger(DartWorkerRunner.class);

  /**
   * Time between {@link #reapControllersIfNeeded()} calls.
   */
  private static final long REAPER_PERIOD_MILLIS = 10_000;

  /**
   * How long {@link #stop()} waits for {@link #controllerReaperExec} to terminate.
   */
  private static final long REAPER_SHUTDOWN_TIMEOUT_MILLIS = 30_000;

  /**
   * Minimum value for {@link DartWorkerConfig#getControllerTimeout()}. Any shorter and we might end up reaping
   * controllers in between heartbeats.
   */
  private static final long MIN_CONTROLLER_TIMEOUT_MILLIS = 60_000;

  /**
   * Controller host -> {@link #clock} time of the most recent contact from that controller.
   */
  @GuardedBy("this")
  private final Map<String, Long> lastControllerHeartbeat = new HashMap<>();

  /**
   * Used to track the time since this runner has been idle.
   */
  private final Stopwatch sinceLastWorkerFinished = Stopwatch.createUnstarted();

  /**
   * Query ID -> Worker instance.
   */
  @GuardedBy("this")
  private final Map<String, WorkerHolder> workerMap = new HashMap<>();
  private final DartWorkerContextFactory workerFactory;
  private final ListeningExecutorService workerExec;
  private final Outbox<ControllerMessage> outbox;
  private final ResourcePermissionMapper permissionMapper;
  private final AuthorizerMapper authorizerMapper;
  private final long controllerTimeoutMillis;

  /**
   * Cancel workers after they have been missing from {@link #syncController} for this long. A grace period is
   * necessary because in-flight syncs may not include a worker that has just started.
   */
  private final long syncGraceMillis;

  private final File baseTempDir;
  private final Clock clock;
  private final ScheduledExecutorService controllerReaperExec;

  public DartWorkerRunner(
      final DartWorkerContextFactory workerFactory,
      final ListeningExecutorService workerExec,
      final Outbox<ControllerMessage> outbox,
      final ResourcePermissionMapper permissionMapper,
      final AuthorizerMapper authorizerMapper,
      final DartWorkerConfig workerConfig,
      final File baseTempDir,
      final Clock clock,
      final ScheduledExecutorService controllerReaperExec
  )
  {
    this.workerFactory = workerFactory;
    this.workerExec = workerExec;
    this.outbox = outbox;
    this.permissionMapper = permissionMapper;
    this.authorizerMapper = authorizerMapper;
    this.controllerTimeoutMillis = workerConfig.getControllerTimeout().toStandardDuration().getMillis();
    if (controllerTimeoutMillis < MIN_CONTROLLER_TIMEOUT_MILLIS) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "druid.msq.dart.worker.controllerTimeout[%s] must be at least [%,d] ms",
                              workerConfig.getControllerTimeout(),
                              MIN_CONTROLLER_TIMEOUT_MILLIS
                          );
    }
    this.syncGraceMillis = workerConfig.getSyncGracePeriod().toStandardDuration().getMillis();
    if (syncGraceMillis <= 0) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "druid.msq.dart.worker.syncGracePeriod[%s] must be positive",
                              workerConfig.getSyncGracePeriod()
                          );
    }
    this.baseTempDir = baseTempDir;
    this.clock = clock;
    this.controllerReaperExec = controllerReaperExec;
  }

  /**
   * Start a worker, creating a holder for it. If a worker with this query ID is already started, does nothing.
   * Returns the worker. Counts as contact from the controller, like {@link #controllerHeartbeat}.
   */
  public Worker startWorker(
      final String queryId,
      final String controllerHost,
      final QueryContext context
  )
  {
    final WorkerHolder holder;
    final boolean newHolder;

    synchronized (this) {
      lastControllerHeartbeat.put(controllerHost, clock.millis());

      final WorkerHolder existingHolder = workerMap.get(queryId);
      if (existingHolder != null) {
        holder = existingHolder;
        newHolder = false;
      } else {
        final WorkerContext workerContext = workerFactory.build(queryId, controllerHost, baseTempDir, context);
        final Worker worker = new WorkerImpl(null, workerContext);
        final WorkerResource resource = new WorkerResource(worker, permissionMapper, authorizerMapper);
        holder = new WorkerHolder(worker, workerContext, controllerHost, resource, DateTimes.nowUtc());
        workerMap.put(queryId, holder);
        this.notifyAll();
        newHolder = true;
      }
    }

    if (newHolder) {
      holder.runRef.run(holder.worker, workerExec).addListener(
          () -> {
            synchronized (this) {
              workerMap.remove(queryId, holder);
              CloseableUtils.closeAndSuppressExceptions(
                  holder,
                  e -> log.warn(e, "Failed to close worker[%s]", holder.worker.id())
              );
              resetOutboxIfReaped(holder.controllerHost);
              resetWorkerFinishTime();
              this.notifyAll();
            }
          },
          Execs.directExecutor()
      );
    }

    return holder.worker;
  }

  /**
   * Stops a worker.
   */
  public void stopWorker(final String queryId)
  {
    final WorkerHolder holder;

    synchronized (this) {
      holder = workerMap.get(queryId);
    }

    if (holder != null) {
      holder.runRef.cancel();
    }
  }

  /**
   * Handles a sync from a controller.
   *
   * @see DartWorkerSyncer the controller side
   */
  public SyncWorkersResponse syncController(final String controllerHost, final SyncWorkersRequest request)
  {
    final Set<String> queryIds = request.queryIds();
    final List<WorkerHolder> holdersToCancel = new ArrayList<>();
    final Set<String> runningQueryIds = new HashSet<>();

    synchronized (this) {
      final long now = clock.millis();
      lastControllerHeartbeat.put(controllerHost, now);

      for (final Map.Entry<String, WorkerHolder> entry : workerMap.entrySet()) {
        final WorkerHolder holder = entry.getValue();
        if (!holder.controllerHost.equals(controllerHost)) {
          continue;
        }

        // Report the query as running even if we cancel it below, since it has not exited yet.
        runningQueryIds.add(entry.getKey());

        if (queryIds.contains(entry.getKey())) {
          // Null out missingFromSyncSince, signifying that this query is meant to be active.
          holder.missingFromSyncSince = null;
        } else {
          // Add this query to workersToCancel if it has been missing long enough.
          final Long missingFromSyncSince = holder.missingFromSyncSince;
          if (missingFromSyncSince == null) {
            holder.missingFromSyncSince = now;
          } else if (now - missingFromSyncSince >= syncGraceMillis) {
            holdersToCancel.add(holder);
          }
        }
      }
    }

    for (final WorkerHolder holder : holdersToCancel) {
      log.info(
          "Canceling query[%s] because controller[%s] no longer lists it as running.",
          holder.workerContext.queryId(),
          controllerHost
      );
      holder.runRef.cancel();
    }

    return new SyncWorkersResponse(runningQueryIds);
  }

  /**
   * Get the worker resource handler for a query ID if it exists. Returns null if the worker is not running.
   */
  @Nullable
  public WorkerResource getWorkerResource(final String queryId)
  {
    synchronized (this) {
      final WorkerHolder holder = workerMap.get(queryId);
      if (holder != null) {
        return holder.resource;
      } else {
        return null;
      }
    }
  }

  /**
   * Returns a {@link GetWorkersResponse} with information about all active workers.
   */
  public GetWorkersResponse getWorkersResponse()
  {
    final List<DartWorkerInfo> infos = new ArrayList<>();

    final long idleDurationMillis;
    synchronized (this) {
      idleDurationMillis = workerMap.isEmpty() ? sinceLastWorkerFinished.millisElapsed() : 0L;
      for (final Map.Entry<String, WorkerHolder> entry : workerMap.entrySet()) {
        final String queryId = entry.getKey();
        final WorkerHolder workerHolder = entry.getValue();
        infos.add(
            new DartWorkerInfo(
                queryId,
                WorkerId.fromString(workerHolder.worker.id()),
                workerHolder.controllerHost,
                workerHolder.acceptTime
            )
        );
      }
    }

    return new GetWorkersResponse(infos, idleDurationMillis);
  }

  /**
   * Sets the finish time of the last worker to the current time. Used to track
   * the duration for which this runner has been idle.
   */
  public void resetWorkerFinishTime()
  {
    synchronized (this) {
      if (sinceLastWorkerFinished.isRunning()) {
        sinceLastWorkerFinished.restart();
      } else {
        sinceLastWorkerFinished.start();
      }
    }
  }

  @LifecycleStart
  public void start()
  {
    createAndCleanTempDirectory();

    final Duration reapDuration = Duration.millis(REAPER_PERIOD_MILLIS);
    ScheduledExecutors.scheduleWithFixedDelay(
        controllerReaperExec,
        reapDuration,
        reapDuration,
        this::reapControllersIfNeeded
    );
  }

  @LifecycleStop
  public void stop()
  {
    controllerReaperExec.shutdownNow();

    try {
      if (!controllerReaperExec.awaitTermination(REAPER_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        log.warn("Controller reaper did not terminate within [%,d] ms.", REAPER_SHUTDOWN_TIMEOUT_MILLIS);
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    synchronized (this) {
      final Collection<WorkerHolder> holders = workerMap.values();

      for (final WorkerHolder holder : holders) {
        holder.runRef.cancel();
      }

      for (final WorkerHolder holder : holders) {
        holder.runRef.awaitStop();
      }
    }
  }

  /**
   * Handles a heartbeat from a controller. Called via {@link PollListener#pollReceived(String)} by
   * {@link MessageRelayResource#httpGetMessagesFromOutbox}.
   */
  public void controllerHeartbeat(final String controllerHost)
  {
    synchronized (this) {
      lastControllerHeartbeat.put(controllerHost, clock.millis());
    }
  }

  /**
   * Cancels the {@link WorkerHolder} associated with controllers that have not made contact in
   * {@link DartWorkerConfig#getControllerTimeout()}. Called periodically in {@link #controllerReaperExec}.
   */
  void reapControllersIfNeeded()
  {
    final Set<String> controllersToReap = new HashSet<>();

    synchronized (this) {
      final long now = clock.millis();
      final Iterator<Map.Entry<String, Long>> it = lastControllerHeartbeat.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<String, Long> entry = it.next();
        if (now - entry.getValue() > controllerTimeoutMillis) {
          controllersToReap.add(entry.getKey());
          it.remove();
        }
      }

      for (final String controllerHost : controllersToReap) {
        log.warn(
            "Reaping controller[%s] that has not made contact in over [%,d] ms.",
            controllerHost,
            controllerTimeoutMillis
        );
      }

      for (final WorkerHolder holder : getWorkersForControllers(controllersToReap)) {
        holder.runRef.cancel();
      }

      for (final String controllerHost : controllersToReap) {
        outbox.resetOutbox(controllerHost);
      }
    }
  }

  /**
   * Method for testing. Waits for the set of queries to match a given predicate.
   */
  @VisibleForTesting
  void awaitQuerySet(Predicate<Set<String>> queryIdsPredicate) throws InterruptedException
  {
    synchronized (this) {
      while (!queryIdsPredicate.test(workerMap.keySet())) {
        wait();
      }
    }
  }

  /**
   * Creates the {@link #baseTempDir}, and removes any items in it that still exist.
   */
  void createAndCleanTempDirectory()
  {
    try {
      FileUtils.mkdirp(baseTempDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final File[] files = baseTempDir.listFiles();

    if (files != null) {
      for (final File file : files) {
        if (file.isDirectory()) {
          try {
            FileUtils.deleteDirectory(file);
            log.info("Removed stale query directory[%s].", file);
          }
          catch (Exception e) {
            log.noStackTrace().warn(e, "Could not remove stale query directory[%s], skipping.", file);
          }
        }
      }
    }
  }

  /**
   * Returns the {@link WorkerHolder}s associated with the given controllers.
   */
  @GuardedBy("this")
  private List<WorkerHolder> getWorkersForControllers(final Set<String> controllerHosts)
  {
    final List<WorkerHolder> holders = new ArrayList<>();
    for (final WorkerHolder holder : workerMap.values()) {
      if (controllerHosts.contains(holder.controllerHost)) {
        holders.add(holder);
      }
    }
    return holders;
  }

  /**
   * Resets the outbox of a controller reaped by {@link #reapControllersIfNeeded} once its last worker has exited.
   * Workers that were busy when canceled may send messages after the reap, which recreates the outbox, and nothing
   * else would remove it.
   */
  @GuardedBy("this")
  private void resetOutboxIfReaped(final String controllerHost)
  {
    if (!lastControllerHeartbeat.containsKey(controllerHost)
        && getWorkersForControllers(Set.of(controllerHost)).isEmpty()) {
      outbox.resetOutbox(controllerHost);
    }
  }

  private static class WorkerHolder implements Closeable
  {
    private final Worker worker;
    private final WorkerContext workerContext;
    private final WorkerResource resource;
    private final String controllerHost;
    private final DateTime acceptTime;
    private final WorkerRunRef runRef;

    /**
     * Epoch time of the first sync from {@link #controllerHost} that did not include this worker's queryId.
     * Null if the most recent sync did include it.
     */
    @Nullable
    private volatile Long missingFromSyncSince;

    public WorkerHolder(
        final Worker worker,
        final WorkerContext workerContext,
        final String controllerHost,
        final WorkerResource resource,
        final DateTime acceptTime
    )
    {
      this.worker = worker;
      this.workerContext = workerContext;
      this.resource = resource;
      this.controllerHost = controllerHost;
      this.acceptTime = acceptTime;
      this.runRef = new WorkerRunRef();
    }

    @Override
    public void close()
    {
      workerContext.close();
    }
  }
}
