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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.SegmentRowCountDistribution;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
@ManageLifecycle
public class SegmentLoadDropHandler implements DataSegmentChangeHandler
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoadDropHandler.class);

  // Synchronizes removals from segmentsToDelete
  private final Object segmentDeleteLock = new Object();

  // Synchronizes start/stop of this object.
  private final Object startStopLock = new Object();

  private final SegmentLoaderConfig config;
  private final DataSegmentAnnouncer announcer;
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentManager segmentManager;
  private final ScheduledExecutorService exec;
  private final ServerTypeConfig serverTypeConfig;
  private final ConcurrentSkipListSet<DataSegment> segmentsToDelete;

  private volatile boolean started = false;

  // Keep history of load/drop request status in a LRU cache to maintain idempotency if same request shows up
  // again and to return status of a completed request. Maximum size of this cache must be significantly greater
  // than number of pending load/drop requests. so that history is not lost too quickly.
  private final Cache<DataSegmentChangeRequest, AtomicReference<SegmentChangeStatus>> requestStatuses;
  private final Object requestStatusesLock = new Object();

  // This is the list of unresolved futures returned to callers of processBatch(List<DataSegmentChangeRequest>)
  // Threads loading/dropping segments resolve these futures as and when some segment load/drop finishes.
  private final LinkedHashSet<CustomSettableFuture> waitingFutures = new LinkedHashSet<>();

  @Inject
  public SegmentLoadDropHandler(
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentManager segmentManager,
      ServerTypeConfig serverTypeConfig
  )
  {
    this(
        config,
        announcer,
        serverAnnouncer,
        segmentManager,
        Executors.newScheduledThreadPool(
            config.getNumLoadingThreads(),
            Execs.makeThreadFactory("SimpleDataSegmentChangeHandler-%s")
        ),
        serverTypeConfig
    );
  }

  @VisibleForTesting
  SegmentLoadDropHandler(
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentManager segmentManager,
      ScheduledExecutorService exec,
      ServerTypeConfig serverTypeConfig
  )
  {
    this.config = config;
    this.announcer = announcer;
    this.serverAnnouncer = serverAnnouncer;
    this.segmentManager = segmentManager;
    this.exec = exec;
    this.serverTypeConfig = serverTypeConfig;

    this.segmentsToDelete = new ConcurrentSkipListSet<>();
    requestStatuses = CacheBuilder.newBuilder().maximumSize(config.getStatusQueueMaxSize()).initialCapacity(8).build();
  }

  @LifecycleStart
  public void start() throws IOException
  {
    synchronized (startStopLock) {
      if (started) {
        return;
      }

      log.info("Starting...");
      try {
        if (segmentManager.canHandleSegments()) {
          bootstrapCachedSegments();
        }

        if (shouldAnnounce()) {
          serverAnnouncer.announce();
        }
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
      }
      started = true;
      log.info("Started.");
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      if (!started) {
        return;
      }

      log.info("Stopping...");
      try {
        if (shouldAnnounce()) {
          serverAnnouncer.unannounce();
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        started = false;
      }
      log.info("Stopped.");
    }
  }

  public boolean isStarted()
  {
    return started;
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
   * Bulk loading of cached segments into page cache during bootstrap.
   */
  private void bootstrapCachedSegments() throws IOException
  {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final List<DataSegment> segments = segmentManager.getCachedSegments();

    // Start a temporary thread pool to load segments into page cache during bootstrap
    final ExecutorService loadingExecutor = Execs.multiThreaded(
        config.getNumBootstrapThreads(), "Segment-Load-Startup-%s"
    );

    try (final BackgroundSegmentAnnouncer backgroundSegmentAnnouncer =
             new BackgroundSegmentAnnouncer(announcer, exec, config.getAnnounceIntervalMillis())) {

      backgroundSegmentAnnouncer.startAnnouncing();

      final int numSegments = segments.size();
      final CountDownLatch latch = new CountDownLatch(numSegments);
      final AtomicInteger counter = new AtomicInteger(0);
      final CopyOnWriteArrayList<DataSegment> failedSegments = new CopyOnWriteArrayList<>();
      for (final DataSegment segment : segments) {
        loadingExecutor.submit(
            () -> {
              try {
                log.info(
                    "Loading segment[%d/%d][%s]",
                    counter.incrementAndGet(), numSegments, segment.getId()
                );
                try {
                  segmentManager.loadSegmentOnBootstrap(
                      segment,
                      () -> this.removeSegment(segment, DataSegmentChangeCallback.NOOP, false)
                  );
                }
                catch (Exception e) {
                  removeSegment(segment, DataSegmentChangeCallback.NOOP, false);
                  throw new SegmentLoadingException(e, "Exception loading segment[%s]", segment.getId());
                }
                try {
                  backgroundSegmentAnnouncer.announceSegment(segment);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new SegmentLoadingException(e, "Loading Interrupted");
                }
              }
              catch (SegmentLoadingException e) {
                log.error(e, "[%s] failed to load", segment.getId());
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
      loadingExecutor.shutdownNow();
      stopwatch.stop();
      // At this stage, all tasks have been submitted, send a shutdown command to cleanup any resources alloted
      // for the bootstrapping function.
      segmentManager.shutdownBootstrap();
      log.info("Cache load of [%d] bootstrap segments took [%,d]ms.", segments.size(), stopwatch.millisElapsed());
    }
  }

  @Override
  public void addSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
  {
    SegmentChangeStatus result = null;
    try {
      log.info("Loading segment[%s]", segment.getId());
      /*
         The lock below is used to prevent a race condition when the scheduled runnable in removeSegment() starts,
         and if (segmentsToDelete.remove(segment)) returns true, in which case historical will start deleting segment
         files. At that point, it's possible that right after the "if" check, addSegment() is called and actually loads
         the segment, which makes dropping segment and downloading segment happen at the same time.
       */
      if (segmentsToDelete.contains(segment)) {
        /*
           Both contains(segment) and remove(segment) can be moved inside the synchronized block. However, in that case,
           each time when addSegment() is called, it has to wait for the lock in order to make progress, which will make
           things slow. Given that in most cases segmentsToDelete.contains(segment) returns false, it will save a lot of
           cost of acquiring lock by doing the "contains" check outside the synchronized block.
         */
        synchronized (segmentDeleteLock) {
          segmentsToDelete.remove(segment);
        }
      }
      try {
        segmentManager.loadSegment(segment);
      }
      catch (Exception e) {
        removeSegment(segment, DataSegmentChangeCallback.NOOP, false);
        throw new SegmentLoadingException(e, "Exception loading segment[%s]", segment.getId());
      }
      try {
        // Announce segment even if the segment file already exists.
        announcer.announceSegment(segment);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Failed to announce segment[%s]", segment.getId());
      }

      result = SegmentChangeStatus.SUCCESS;
    }
    catch (Throwable e) {
      log.makeAlert(e, "Failed to load segment")
         .addData("segment", segment)
         .emit();
      result = SegmentChangeStatus.failed(e.toString());
    }
    finally {
      updateRequestStatus(new SegmentChangeRequestLoad(segment), result);
      if (null != callback) {
        callback.execute();
      }
    }
  }

  @Override
  public void removeSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
  {
    removeSegment(segment, callback, true);
  }

  @VisibleForTesting
  void removeSegment(
      final DataSegment segment,
      @Nullable final DataSegmentChangeCallback callback,
      final boolean scheduleDrop
  )
  {
    SegmentChangeStatus result = null;
    try {
      announcer.unannounceSegment(segment);
      segmentsToDelete.add(segment);

      Runnable runnable = () -> {
        try {
          synchronized (segmentDeleteLock) {
            if (segmentsToDelete.remove(segment)) {
              segmentManager.dropSegment(segment);
            }
          }
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to remove segment! Possible resource leak!")
             .addData("segment", segment)
             .emit();
        }
      };

      if (scheduleDrop) {
        log.info(
            "Completely removing segment[%s] in [%,d]ms.",
            segment.getId(), config.getDropSegmentDelayMillis()
        );
        exec.schedule(
            runnable,
            config.getDropSegmentDelayMillis(),
            TimeUnit.MILLISECONDS
        );
      } else {
        runnable.run();
      }

      result = SegmentChangeStatus.SUCCESS;
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to remove segment")
         .addData("segment", segment)
         .emit();
      result = SegmentChangeStatus.failed(e.getMessage());
    }
    finally {
      updateRequestStatus(new SegmentChangeRequestDrop(segment), result);
      if (null != callback) {
        callback.execute();
      }
    }
  }

  public Collection<DataSegment> getSegmentsToDelete()
  {
    return ImmutableList.copyOf(segmentsToDelete);
  }

  public ListenableFuture<List<DataSegmentChangeResponse>> processBatch(List<DataSegmentChangeRequest> changeRequests)
  {
    boolean isAnyRequestDone = false;

    Map<DataSegmentChangeRequest, AtomicReference<SegmentChangeStatus>> statuses = Maps.newHashMapWithExpectedSize(changeRequests.size());

    for (DataSegmentChangeRequest cr : changeRequests) {
      AtomicReference<SegmentChangeStatus> status = processRequest(cr);
      if (status.get().getState() != SegmentChangeStatus.State.PENDING) {
        isAnyRequestDone = true;
      }
      statuses.put(cr, status);
    }

    CustomSettableFuture future = new CustomSettableFuture(waitingFutures, statuses);

    if (isAnyRequestDone) {
      future.resolve();
    } else {
      synchronized (waitingFutures) {
        waitingFutures.add(future);
      }
    }

    return future;
  }

  private AtomicReference<SegmentChangeStatus> processRequest(DataSegmentChangeRequest changeRequest)
  {
    synchronized (requestStatusesLock) {
      AtomicReference<SegmentChangeStatus> status = requestStatuses.getIfPresent(changeRequest);

      // If last load/drop request status is failed, here can try that again
      if (status == null || status.get().getState() == SegmentChangeStatus.State.FAILED) {
        changeRequest.go(
            new DataSegmentChangeHandler()
            {
              @Override
              public void addSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
              {
                requestStatuses.put(changeRequest, new AtomicReference<>(SegmentChangeStatus.PENDING));
                exec.submit(
                    () -> SegmentLoadDropHandler.this.addSegment(
                        ((SegmentChangeRequestLoad) changeRequest).getSegment(),
                        () -> resolveWaitingFutures()
                    )
                );
              }

              @Override
              public void removeSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
              {
                requestStatuses.put(changeRequest, new AtomicReference<>(SegmentChangeStatus.PENDING));
                SegmentLoadDropHandler.this.removeSegment(
                    ((SegmentChangeRequestDrop) changeRequest).getSegment(),
                    () -> resolveWaitingFutures(),
                    true
                );
              }
            },
            this::resolveWaitingFutures
        );
      } else if (status.get().getState() == SegmentChangeStatus.State.SUCCESS) {
        // SUCCESS case, we'll clear up the cached success while serving it to this client
        // Not doing this can lead to an incorrect response to upcoming clients for a reload
        requestStatuses.invalidate(changeRequest);
        return status;
      }
      return requestStatuses.getIfPresent(changeRequest);
    }
  }

  private void updateRequestStatus(DataSegmentChangeRequest changeRequest, @Nullable SegmentChangeStatus result)
  {
    if (result == null) {
      result = SegmentChangeStatus.failed("Unknown reason. Check server logs.");
    }
    synchronized (requestStatusesLock) {
      AtomicReference<SegmentChangeStatus> statusRef = requestStatuses.getIfPresent(changeRequest);
      if (statusRef != null) {
        statusRef.set(result);
      }
    }
  }

  private void resolveWaitingFutures()
  {
    LinkedHashSet<CustomSettableFuture> waitingFuturesCopy;
    synchronized (waitingFutures) {
      waitingFuturesCopy = new LinkedHashSet<>(waitingFutures);
      waitingFutures.clear();
    }
    for (CustomSettableFuture future : waitingFuturesCopy) {
      future.resolve();
    }
  }

  /**
   * Returns whether or not we should announce ourselves as a data server using {@link DataSegmentServerAnnouncer}.
   *
   * Returns true if _either_:
   *
   * <li> Our {@link #serverTypeConfig} indicates we are a segment server. This is necessary for Brokers to be able
   * to detect that we exist.</li>
   * <li> The segment manager is able to handle segments. This is necessary for Coordinators to be able to
   * assign segments to us.</li>
   */
  private boolean shouldAnnounce()
  {
    return serverTypeConfig.getServerType().isSegmentServer() || segmentManager.canHandleSegments();
  }

  private static class BackgroundSegmentAnnouncer implements AutoCloseable
  {
    private static final EmittingLogger log = new EmittingLogger(BackgroundSegmentAnnouncer.class);

    private final int intervalMillis;
    private final DataSegmentAnnouncer announcer;
    private final ScheduledExecutorService exec;
    private final LinkedBlockingQueue<DataSegment> queue;
    private final SettableFuture<Boolean> doneAnnouncing;

    private final Object lock = new Object();

    private volatile boolean finished = false;
    @Nullable
    private volatile ScheduledFuture startedAnnouncing = null;
    @Nullable
    private volatile ScheduledFuture nextAnnoucement = null;

    public BackgroundSegmentAnnouncer(
        DataSegmentAnnouncer announcer,
        ScheduledExecutorService exec,
        int intervalMillis
    )
    {
      this.announcer = announcer;
      this.exec = exec;
      this.intervalMillis = intervalMillis;
      this.queue = new LinkedBlockingQueue<>();
      this.doneAnnouncing = SettableFuture.create();
    }

    public void announceSegment(final DataSegment segment) throws InterruptedException
    {
      if (finished) {
        throw new ISE("Announce segment called after finishAnnouncing");
      }
      queue.put(segment);
    }

    public void startAnnouncing()
    {
      if (intervalMillis <= 0) {
        return;
      }

      log.info("Starting background segment announcing task");

      // schedule background announcing task
      nextAnnoucement = startedAnnouncing = exec.schedule(
          new Runnable()
          {
            @Override
            public void run()
            {
              synchronized (lock) {
                try {
                  if (!(finished && queue.isEmpty())) {
                    final List<DataSegment> segments = new ArrayList<>();
                    queue.drainTo(segments);
                    try {
                      announcer.announceSegments(segments);
                      nextAnnoucement = exec.schedule(this, intervalMillis, TimeUnit.MILLISECONDS);
                    }
                    catch (IOException e) {
                      doneAnnouncing.setException(
                          new SegmentLoadingException(e, "Failed to announce segments[%s]", segments)
                      );
                    }
                  } else {
                    doneAnnouncing.set(true);
                  }
                }
                catch (Exception e) {
                  doneAnnouncing.setException(e);
                }
              }
            }
          },
          intervalMillis,
          TimeUnit.MILLISECONDS
      );
    }

    public void finishAnnouncing() throws SegmentLoadingException
    {
      synchronized (lock) {
        finished = true;
        // announce any remaining segments
        try {
          final List<DataSegment> segments = new ArrayList<>();
          queue.drainTo(segments);
          announcer.announceSegments(segments);
        }
        catch (Exception e) {
          throw new SegmentLoadingException(e, "Failed to announce segments[%s]", queue);
        }

        // get any exception that may have been thrown in background announcing
        try {
          // check in case intervalMillis is <= 0
          if (startedAnnouncing != null) {
            startedAnnouncing.cancel(false);
          }
          // - if the task is waiting on the lock, then the queue will be empty by the time it runs
          // - if the task just released it, then the lock ensures any exception is set in doneAnnouncing
          if (doneAnnouncing.isDone()) {
            doneAnnouncing.get();
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SegmentLoadingException(e, "Loading Interrupted");
        }
        catch (ExecutionException e) {
          throw new SegmentLoadingException(e.getCause(), "Background Announcing Task Failed");
        }
      }
      log.info("Completed background segment announcing");
    }

    @Override
    public void close()
    {
      // stop background scheduling
      synchronized (lock) {
        finished = true;
        if (nextAnnoucement != null) {
          nextAnnoucement.cancel(false);
        }
      }
    }
  }

  // Future with cancel() implementation to remove it from "waitingFutures" list
  private class CustomSettableFuture extends AbstractFuture<List<DataSegmentChangeResponse>>
  {
    private final LinkedHashSet<CustomSettableFuture> waitingFutures;
    private final Map<DataSegmentChangeRequest, AtomicReference<SegmentChangeStatus>> statusRefs;

    private CustomSettableFuture(
        LinkedHashSet<CustomSettableFuture> waitingFutures,
        Map<DataSegmentChangeRequest, AtomicReference<SegmentChangeStatus>> statusRefs
    )
    {
      this.waitingFutures = waitingFutures;
      this.statusRefs = statusRefs;
    }

    public void resolve()
    {
      synchronized (requestStatusesLock) {
        if (isDone()) {
          return;
        }

        final List<DataSegmentChangeResponse> result = new ArrayList<>(statusRefs.size());
        statusRefs.forEach((request, statusRef) -> {
          // Remove complete statuses from the cache
          final SegmentChangeStatus status = statusRef.get();
          if (status != null && status.getState() != SegmentChangeStatus.State.PENDING) {
            requestStatuses.invalidate(request);
          }
          result.add(new DataSegmentChangeResponse(request, status));
        });

        set(result);
      }
    }

    @Override
    public boolean cancel(boolean interruptIfRunning)
    {
      synchronized (waitingFutures) {
        waitingFutures.remove(this);
      }
      return true;
    }
  }

}

