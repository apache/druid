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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.SegmentRowCountDistribution;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Handles segment load and unload in both Curator-based and HTTP-based loading.
 * Curator-based loading uses only the method {@link #submitCuratorRequest}
 * whereas HTTP-based loading uses only {@link #submitRequestBatch}.
 */
@ManageLifecycle
public class SegmentLoadDropHandler
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoadDropHandler.class);

  /**
   * Synchronizes start/stop of the SegmentLoadDropHandler.
   */
  private final Object startStopLock = new Object();

  private final ObjectMapper jsonMapper;
  private final SegmentLoaderConfig config;
  private final DataSegmentAnnouncer announcer;
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentManager segmentManager;
  private final ScheduledExecutorService loadingExec;
  private final ServerTypeConfig serverTypeConfig;
  private final SegmentCacheManager segmentCacheManager;

  private volatile boolean started = false;

  /**
   * Set of segments to drop, maintained only for the purposes of monitoring.
   */
  private final ConcurrentSkipListSet<DataSegment> segmentsToDrop = new ConcurrentSkipListSet<>();

  private final AtomicBoolean hasUnhandledUpdates = new AtomicBoolean(false);

  @GuardedBy("taskQueueLock")
  private final Map<DataSegment, SegmentTaskPair> segmentToTasks = new LinkedHashMap<>();

  /**
   * Guards addition and removal from {@link #segmentToTasks}.
   */
  private final Object taskQueueLock = new Object();

  private final AtomicReference<LoadDropBatch> currentBatch = new AtomicReference<>();

  @Inject
  public SegmentLoadDropHandler(
      ObjectMapper jsonMapper,
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentManager segmentManager,
      SegmentCacheManager segmentCacheManager,
      ServerTypeConfig serverTypeConfig,
      ScheduledExecutorFactory executorFactory
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.announcer = announcer;
    this.serverAnnouncer = serverAnnouncer;
    this.segmentManager = segmentManager;
    this.segmentCacheManager = segmentCacheManager;
    this.serverTypeConfig = serverTypeConfig;
    this.loadingExec = executorFactory.create(config.getNumLoadingThreads(), "SegmentLoadDropHandler-%s");
  }

  @LifecycleStart
  public void start() throws IOException
  {
    synchronized (startStopLock) {
      if (started) {
        return;
      }

      final Stopwatch stopwatch = Stopwatch.createStarted();
      log.info("Starting SegmentLoadDropHandler...");
      try {
        if (!config.getLocations().isEmpty()) {
          loadLocalCache();
        }

        if (shouldAnnounceSelfAsDataServer()) {
          serverAnnouncer.announce();
        }
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
      }
      started = true;
      log.info("Started SegmentLoadDropHandler in [%d]ms.", stopwatch.millisElapsed());
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      if (!started) {
        return;
      }

      log.info("Stopping SegmentLoadDropHandler...");
      try {
        if (shouldAnnounceSelfAsDataServer()) {
          serverAnnouncer.unannounce();
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        started = false;
      }
      log.info("Stopped SegmentLoadDropHandler.");
    }
  }

  public boolean isStarted()
  {
    return started;
  }

  private void loadLocalCache() throws IOException
  {
    File baseDir = config.getInfoDir();
    FileUtils.mkdirp(baseDir);

    List<DataSegment> cachedSegments = new ArrayList<>();
    File[] segmentsToLoad = baseDir.listFiles();
    int ignored = 0;
    for (int i = 0; i < segmentsToLoad.length; i++) {
      File file = segmentsToLoad[i];
      log.info("Loading segment cache file [%d/%d][%s].", i + 1, segmentsToLoad.length, file);
      try {
        final DataSegment segment = jsonMapper.readValue(file, DataSegment.class);

        if (!segment.getId().toString().equals(file.getName())) {
          log.warn("Ignoring cache file[%s] for segment[%s].", file.getPath(), segment.getId());
          ignored++;
        } else if (segmentCacheManager.isSegmentCached(segment)) {
          cachedSegments.add(segment);
        } else {
          log.warn("Unable to find cache file for %s. Deleting lookup entry", segment.getId());

          File segmentInfoCacheFile = new File(baseDir, segment.getId().toString());
          if (!segmentInfoCacheFile.delete()) {
            log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
          }
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to load segment from segmentInfo file")
           .addData("file", file)
           .emit();
      }
    }

    if (ignored > 0) {
      log.makeAlert("Ignored misnamed segment cache files on startup.")
         .addData("numIgnored", ignored)
         .emit();
    }

    loadCachedSegments(cachedSegments);
  }

  /**
   * Downloads a single segment and creates a cache file for it in the info dir.
   * If the load fails at any step, {@link #cleanupFailedLoad} is called.
   *
   * @throws SegmentLoadingException if the load fails
   */
  private void loadSegment(
      DataSegment segment,
      boolean lazy,
      @Nullable ExecutorService loadSegmentIntoPageCacheExec
  ) throws SegmentLoadingException
  {
    final boolean loaded;
    try {
      loaded = segmentManager.loadSegment(
          segment,
          lazy,
          () -> cleanupFailedLoad(segment),
          loadSegmentIntoPageCacheExec
      );
    }
    catch (Exception e) {
      cleanupFailedLoad(segment);
      throw new SegmentLoadingException(e, "Could not load segment: %s", e.getMessage());
    }

    if (loaded) {
      File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getId().toString());
      if (!segmentInfoCacheFile.exists()) {
        try {
          jsonMapper.writeValue(segmentInfoCacheFile, segment);
        }
        catch (IOException e) {
          cleanupFailedLoad(segment);
          throw new SegmentLoadingException(
              e,
              "Failed to write to disk segment info cache file[%s]",
              segmentInfoCacheFile
          );
        }
      }
    }
  }

  public Map<String, Long> getAverageNumOfRowsPerSegmentForDatasource()
  {
    return segmentManager.getAverageRowCountForDatasource();
  }

  public Map<String, SegmentRowCountDistribution> getRowCountDistributionPerDatasource()
  {
    return segmentManager.getRowCountDistribution();
  }

  /**
   * Bulk adding segments during bootstrap
   */
  private void loadCachedSegments(Collection<DataSegment> segments)
  {
    // Start a temporary thread pool to load segments into page cache during bootstrap
    final Stopwatch stopwatch = Stopwatch.createStarted();
    ExecutorService loadingExecutor = null;
    ExecutorService loadSegmentsIntoPageCacheOnBootstrapExec =
        config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap() != 0 ?
        Execs.multiThreaded(
            config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap(),
            "Load-Segments-Into-Page-Cache-On-Bootstrap-%s"
        ) : null;
    try (final BackgroundSegmentAnnouncer backgroundSegmentAnnouncer =
             new BackgroundSegmentAnnouncer(announcer, loadingExec, config.getAnnounceIntervalMillis())) {

      backgroundSegmentAnnouncer.startAnnouncing();

      loadingExecutor = Execs.multiThreaded(config.getNumBootstrapThreads(), "Segment-Load-Startup-%s");

      final int numSegments = segments.size();
      final CountDownLatch latch = new CountDownLatch(numSegments);
      final AtomicInteger counter = new AtomicInteger(0);
      final CopyOnWriteArrayList<DataSegment> failedSegments = new CopyOnWriteArrayList<>();
      for (final DataSegment segment : segments) {
        loadingExecutor.submit(
            () -> {
              try {
                log.info("Loading segment[%d/%d][%s]", counter.incrementAndGet(), numSegments, segment.getId());
                loadSegment(segment, config.isLazyLoadOnStart(), loadSegmentsIntoPageCacheOnBootstrapExec);
                try {
                  backgroundSegmentAnnouncer.announceSegment(segment);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new SegmentLoadingException(e, "Loading Interrupted");
                }
              }
              catch (SegmentLoadingException e) {
                log.error(e, "Segment[%s] failed to load", segment.getId());
                failedSegments.add(segment);
              }
              finally {
                latch.countDown();
              }
            }
        );
      }

      try {
        latch.await();

        if (failedSegments.size() > 0) {
          log.makeAlert("%,d errors seen while loading segments", failedSegments.size())
             .addData("failedSegments", failedSegments)
             .emit();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.makeAlert(e, "LoadingInterrupted").emit();
      }

      backgroundSegmentAnnouncer.finishAnnouncing();
    }
    catch (SegmentLoadingException e) {
      log.makeAlert(e, "Failed to load segments -- likely problem with announcing.")
         .addData("numSegments", segments.size())
         .emit();
    }
    finally {
      log.info("Finished cache load in [%,d]ms", stopwatch.millisElapsed());
      if (loadingExecutor != null) {
        loadingExecutor.shutdownNow();
      }
      if (loadSegmentsIntoPageCacheOnBootstrapExec != null) {
        // At this stage, all tasks have been submitted, send a shutdown command to the bootstrap
        // thread pool so threads will exit after finishing the tasks
        loadSegmentsIntoPageCacheOnBootstrapExec.shutdown();
      }
    }
  }

  /**
   * Cleans up a failed LOAD request by completely removing the partially
   * downloaded segment files and unannouncing the segment for safe measure.
   */
  @VisibleForTesting
  void cleanupFailedLoad(DataSegment segment)
  {
    try {
      announcer.unannounceSegment(segment);
    }
    catch (Exception e) {
      raiseAlertForSegment(segment, e, "Failed to unannounce segment during clean up");
    }

    dropSegment(segment);
  }

  /**
   * Drops the given segment synchronously.
   */
  private void dropSegment(DataSegment segment)
  {
    try {
      segmentManager.dropSegment(segment);

      File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getId().toString());
      if (!segmentInfoCacheFile.delete()) {
        log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
      }
    }
    catch (Exception e) {
      raiseAlertForSegment(segment, e, "Failed to delete segment files. Possible resource leak.");
    }
  }

  private void raiseAlertForSegment(DataSegment segment, Throwable t, String message)
  {
    log.makeAlert(t, message).addData("segment", segment).emit();
  }

  public Collection<DataSegment> getSegmentsToDrop()
  {
    return ImmutableList.copyOf(segmentsToDrop);
  }

  /**
   * Submits a batch of segment load/drop requests for processing and returns a
   * Future of the result. The previously submitted batch, if any, is resolved
   * and the underlying requests are cancelled unless they are required by the
   * current batch too.
   *
   * @return Future of List of results of each change request in the batch.
   * This future completes as soon as any pending request in this batch is completed
   * or if a new batch is submitted.
   */
  public ListenableFuture<List<DataSegmentChangeResponse>> submitRequestBatch(
      List<DataSegmentChangeRequest> changeRequests
  )
  {
    if (!isStarted()) {
      throw new ISE("SegmentLoadDropHandler has not started yet.");
    }

    final LoadDropBatch newBatch = new LoadDropBatch(changeRequests);
    synchronized (taskQueueLock) {
      final LoadDropBatch oldBatch = currentBatch.getAndSet(newBatch);
      if (oldBatch != null) {
        oldBatch.cancelTaskIfNotRequiredBy(newBatch);
        oldBatch.resolve();
      }

      for (SegmentTask task : newBatch.requestToTask.values()) {
        segmentToTasks.computeIfAbsent(task.segment, s -> new SegmentTaskPair())
                      .setWaitingTask(task);
      }
    }
    processQueuedTasks();
    return newBatch;
  }

  /**
   * Submits a single load/drop request for processing. This method should be
   * used only in Curator-based segment loading.
   */
  public void submitCuratorRequest(DataSegmentChangeRequest request, DataSegmentChangeCallback callback)
  {
    if (!isStarted()) {
      throw new ISE("SegmentLoadDropHandler has not started yet.");
    } else if (isNoopRequest(request)) {
      return;
    }

    synchronized (taskQueueLock) {
      segmentToTasks.computeIfAbsent(request.getSegment(), s -> new SegmentTaskPair())
                    .setWaitingTask(createTaskFor(request, callback));
    }
    processQueuedTasks();
  }

  /**
   * Cleans up completed load/drop tasks and starts ready tasks. If there is
   * already another thread waiting to process queued tasks, then this method
   * returns immediately.
   */
  private void processQueuedTasks()
  {
    // Toggling the flag hasUnhandledUpdates ensures that there is
    // at most one thread waiting on the lock
    if (hasUnhandledUpdates.compareAndSet(false, true)) {
      // There will only ever be one thread waiting on this lock because the flag
      // hasUnhandledUpdates would be true causing other threads to return immediately
      synchronized (taskQueueLock) {
        hasUnhandledUpdates.set(false);
        runTasksAndHandleCompleted();

        // Resolve current batch if any result is ready
        final LoadDropBatch currentBatch = this.currentBatch.get();
        if (currentBatch != null) {
          currentBatch.resolveIfResultsReady();
        }
      }
    }
  }

  /**
   * Removes completed tasks from the queue.
   */
  @GuardedBy("taskQueueLock")
  private void runTasksAndHandleCompleted()
  {
    Set<DataSegment> segmentsWithNoTasks = new HashSet<>();
    segmentToTasks.forEach((segment, pair) -> {
      if (pair.runningTask != null && !pair.runningTask.isComplete()) {
        // The currently running task for this segment has not completed yet
        return;
      }

      final SegmentTask waitingTask = pair.waitingTask;
      if (waitingTask == null) {
        segmentsWithNoTasks.add(segment);
      } else if (waitingTask.isPreCompleted()) {
        segmentsWithNoTasks.add(segment);
      } else {
        loadingExec.submit(waitingTask);
        pair.startWaitingTask();
      }
    });

    segmentsWithNoTasks.forEach(segmentToTasks::remove);
  }

  /**
   * Whether this server should announce itself as a data server using
   * {@link DataSegmentServerAnnouncer}.
   * <p>
   * Returns true if:
   * <p>
   * (1) EITHER the {@link #serverTypeConfig} indicates that the node is a segment server.
   * This is necessary for Brokers to be able to detect that we exist.
   * (2) OR the {@link #config} has non-empty storage locations. This is necessary
   * for the Coordinator to be able to assign segments to the server.
   */
  private boolean shouldAnnounceSelfAsDataServer()
  {
    return serverTypeConfig.getServerType().isSegmentServer() || !config.getLocations().isEmpty();
  }

  /**
   * Represents the future result of a single batch of segment load drop requests.
   */
  private class LoadDropBatch extends AbstractFuture<List<DataSegmentChangeResponse>>
  {
    private final List<DataSegmentChangeRequest> changeRequests;
    private final Map<DataSegmentChangeRequest, SegmentTask> requestToTask =new HashMap<>();
    private final boolean hasNoopRequests;

    LoadDropBatch(List<DataSegmentChangeRequest> changeRequests)
    {
      this.changeRequests = changeRequests;

      boolean hasNoopRequests = false;
      final Set<DataSegment> requestedSegments = new HashSet<>();
      for (DataSegmentChangeRequest request : changeRequests) {
        if (isNoopRequest(request)) {
          hasNoopRequests = true;
        } else if (requestedSegments.contains(request.getSegment())) {
          throw InvalidInput.exception("Batch has multiple requests for segment[%s]", request.getSegment().getId());
        } else {
          requestToTask.put(request, createTaskFor(request, null));
          requestedSegments.add(request.getSegment());
        }
      }
      this.hasNoopRequests = hasNoopRequests;
    }

    void cancelTaskIfNotRequiredBy(LoadDropBatch latestBatch)
    {
      Set<DataSegment> latestRequestedSegments
          = latestBatch.requestToTask.keySet()
                                     .stream()
                                     .map(DataSegmentChangeRequest::getSegment)
                                     .collect(Collectors.toSet());
      requestToTask.values().stream()
                   .filter(task -> latestRequestedSegments.contains(task.segment))
                   .forEach(SegmentTask::cancel);
    }

    void resolveIfResultsReady()
    {
      if (isDone()) {
        return;
      }

      if (hasNoopRequests || requestToTask.values().stream().anyMatch(SegmentTask::isResultReady)) {
        resolve();
      }
    }

    void resolve()
    {
      if (isDone()) {
        return;
      }

      final List<DataSegmentChangeResponse> results = new ArrayList<>();
      for (DataSegmentChangeRequest request : changeRequests) {
        SegmentTask task = requestToTask.get(request);
        SegmentChangeStatus status = task == null ? SegmentChangeStatus.SUCCESS : task.getStatus();
        results.add(new DataSegmentChangeResponse(request, status));
      }
      set(results);
    }
  }

  /**
   * Task for loading or dropping a single segment. For any given segment, only
   * a single task can be running on the executor at any point.
   */
  private abstract class SegmentTask implements Runnable
  {
    @Nullable
    private final DataSegmentChangeCallback callback;
    final DataSegment segment;

    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicReference<SegmentChangeStatus> status = new AtomicReference<>();

    SegmentTask(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
    {
      this.segment = segment;
      this.callback = callback;
      this.status.set(SegmentChangeStatus.PENDING);
    }

    abstract boolean isAlreadySucceeded();
    abstract boolean isAlreadyComplete();
    abstract void perform();

    final boolean isPreCompleted()
    {
      if (isAlreadySucceeded()) {
        setStatus(SegmentChangeStatus.SUCCESS);
      }
      final boolean isCompleted = isAlreadyComplete();
      if (isCompleted) {
        markCompleted();
      }
      return isCompleted;
    }

    @Override
    public void run()
    {
      if (isComplete()) {
        return;
      }
      perform();
      processQueuedTasks();
    }

    /**
     * Load requests complete after segment has been downloaded and announced.
     * Drop requests complete after segment has been unannounced and dropped.
     */
    boolean isComplete()
    {
      return completed.get() || cancelled.get();
    }

    void markCompleted()
    {
      completed.set(true);
      segmentsToDrop.remove(segment);
    }

    boolean isResultReady()
    {
      return status.get().getState() != SegmentChangeStatus.State.PENDING;
    }

    void cancel()
    {
      cancelled.set(true);
    }

    void setStatus(SegmentChangeStatus status)
    {
      boolean updated = this.status.compareAndSet(SegmentChangeStatus.PENDING, status);
      if (updated && callback != null) {
        callback.execute();
      }
    }

    SegmentChangeStatus getStatus()
    {
      return status.get();
    }
  }

  /**
   * Task that loads and announces a segment. The result becomes ready and the
   * task completes when the segment has been both loaded and announced.
   */
  private class LoadTask extends SegmentTask
  {
    LoadTask(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
    {
      super(segment, callback);
    }

    @Override
    boolean isAlreadySucceeded()
    {
      return announcer.isSegmentAnnounced(segment)
             && segmentCacheManager.isSegmentCached(segment);
    }

    @Override
    boolean isAlreadyComplete()
    {
      return isAlreadySucceeded();
    }

    @Override
    void perform()
    {
      try {
        log.info("Loading segment[%s]", segment.getId());
        loadSegment(segment, false, null);
        announcer.announceSegment(segment);
        setStatus(SegmentChangeStatus.SUCCESS);
      }
      catch (Throwable e) {
        raiseAlertForSegment(segment, e, "Failed to load segment");
        setStatus(SegmentChangeStatus.failed(e.getMessage()));
      }
      finally {
        markCompleted();
      }
    }
  }

  /**
   * Task that unannounces and drops a segment. The result is ready after the
   * segment has been unannounced but the task completes after the segment files
   * have been removed as well.
   */
  private class DropTask extends SegmentTask
  {
    DropTask(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
    {
      super(segment, callback);
    }

    @Override
    boolean isAlreadyComplete()
    {
      return !announcer.isSegmentAnnounced(segment)
             && !segmentCacheManager.isSegmentCached(segment);
    }

    @Override
    boolean isAlreadySucceeded()
    {
      return !announcer.isSegmentAnnounced(segment);
    }

    @Override
    void perform()
    {
      try {
        announcer.unannounceSegment(segment);
        setStatus(SegmentChangeStatus.SUCCESS);
        scheduleDrop();
      }
      catch (Exception e) {
        raiseAlertForSegment(segment, e, "Failed to unannounce segment");
        setStatus(SegmentChangeStatus.failed(e.getMessage()));
      }
    }

    void scheduleDrop()
    {
      final long dropDelayMillis = config.getDropSegmentDelayMillis();
      log.info("Completely removing segment[%s] in [%,d] millis.", segment.getId(), dropDelayMillis);
      segmentsToDrop.add(segment);

      loadingExec.schedule(
          () -> {
            if (!isComplete()) {
              dropSegment(segment);
              markCompleted();
            }
          },
          dropDelayMillis,
          TimeUnit.MILLISECONDS
      );
    }
  }

  /**
   * Pair of tasks for a given segment. At any point, there can be at most two
   * tasks for a segment: one running and one waiting.
   */
  private static class SegmentTaskPair
  {
    @Nullable
    private SegmentTask runningTask;

    @Nullable
    private SegmentTask waitingTask;

    private synchronized void startWaitingTask()
    {
      runningTask = waitingTask;
    }

    private synchronized void setWaitingTask(SegmentTask task)
    {
      waitingTask = task;
      if (runningTask != null && !runningTask.getClass().equals(waitingTask.getClass())) {
        runningTask.cancel();
      }
    }
  }

  private static boolean isNoopRequest(DataSegmentChangeRequest request)
  {
    return request instanceof SegmentChangeRequestNoop;
  }

  private SegmentTask createTaskFor(
      DataSegmentChangeRequest request,
      @Nullable DataSegmentChangeCallback callback
  )
  {
    if (request instanceof SegmentChangeRequestLoad) {
      return new LoadTask(request.getSegment(), callback);
    } else if (request instanceof SegmentChangeRequestDrop) {
      return new DropTask(request.getSegment(), callback);
    } else {
      throw new IAE("Could not create segment task for invalid request[%s].", request);
    }
  }

}
