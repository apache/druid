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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.apache.druid.client.BootstrapSegmentsResponse;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.DataSourceTaskIdHolder;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

/**
 * Responsible for bootstrapping segments already cached on disk and bootstrap segments fetched from the coordinator.
 * Also responsible for announcing the node as a data server if applicable, once the bootstrapping operations
 * are complete.
 */
@ManageLifecycle
public class SegmentBootstrapper
{
  private final SegmentLoadDropHandler loadDropHandler;
  private final SegmentLoaderConfig config;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentManager segmentManager;
  private final ServerTypeConfig serverTypeConfig;
  private final CoordinatorClient coordinatorClient;
  private final ServiceEmitter emitter;

  private volatile boolean isComplete = false;

  // Synchronizes start/stop of this object.
  private final Object startStopLock = new Object();

  private static final EmittingLogger log = new EmittingLogger(SegmentBootstrapper.class);

  private final DataSourceTaskIdHolder datasourceHolder;

  @Inject
  public SegmentBootstrapper(
      SegmentLoadDropHandler loadDropHandler,
      SegmentLoaderConfig config,
      DataSegmentAnnouncer segmentAnnouncer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentManager segmentManager,
      ServerTypeConfig serverTypeConfig,
      CoordinatorClient coordinatorClient,
      ServiceEmitter emitter,
      DataSourceTaskIdHolder datasourceHolder
  )
  {
    this.loadDropHandler = loadDropHandler;
    this.config = config;
    this.segmentAnnouncer = segmentAnnouncer;
    this.serverAnnouncer = serverAnnouncer;
    this.segmentManager = segmentManager;
    this.serverTypeConfig = serverTypeConfig;
    this.coordinatorClient = coordinatorClient;
    this.emitter = emitter;
    this.datasourceHolder = datasourceHolder;
  }

  @LifecycleStart
  public void start() throws IOException
  {
    synchronized (startStopLock) {
      if (isComplete) {
        return;
      }

      log.info("Starting...");
      try {
        if (segmentManager.canHandleSegments()) {
          loadSegmentsOnStartup();
        }

        if (shouldAnnounce()) {
          serverAnnouncer.announce();
        }
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
      }
      isComplete = true;
      log.info("Started.");
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      if (!isComplete) {
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
        isComplete = false;
      }
      log.info("Stopped.");
    }
  }

  public boolean isBootstrappingComplete()
  {
    return isComplete;
  }

  /**
   * Bulk loading of the following segments into the page cache at startup:
   * <li> Previously cached segments </li>
   * <li> Bootstrap segments from the coordinator </li>
   */
  private void loadSegmentsOnStartup() throws IOException
  {
    final List<DataSegment> segmentsOnStartup = new ArrayList<>();
    segmentsOnStartup.addAll(segmentManager.getCachedSegments());
    segmentsOnStartup.addAll(getBootstrapSegments());

    final Stopwatch stopwatch = Stopwatch.createStarted();

    // Start a temporary thread pool to load segments into page cache during bootstrap
    final ExecutorService bootstrapExecutor = Execs.multiThreaded(
        config.getNumBootstrapThreads(), "Segment-Bootstrap-%s"
    );

    // Start a temporary scheduled executor for background segment announcing
    final ScheduledExecutorService backgroundAnnouncerExecutor = Executors.newScheduledThreadPool(
        config.getNumLoadingThreads(), Execs.makeThreadFactory("Background-Segment-Announcer-%s")
    );

    try (final BackgroundSegmentAnnouncer backgroundSegmentAnnouncer =
             new BackgroundSegmentAnnouncer(segmentAnnouncer, backgroundAnnouncerExecutor, config.getAnnounceIntervalMillis())) {

      backgroundSegmentAnnouncer.startAnnouncing();

      final int numSegments = segmentsOnStartup.size();
      final CountDownLatch latch = new CountDownLatch(numSegments);
      final AtomicInteger counter = new AtomicInteger(0);
      final CopyOnWriteArrayList<DataSegment> failedSegments = new CopyOnWriteArrayList<>();
      for (final DataSegment segment : segmentsOnStartup) {
        bootstrapExecutor.submit(
            () -> {
              try {
                log.info(
                    "Loading segment[%d/%d][%s]",
                    counter.incrementAndGet(), numSegments, segment.getId()
                );
                try {
                  segmentManager.loadSegmentOnBootstrap(
                      segment,
                      () -> loadDropHandler.removeSegment(segment, DataSegmentChangeCallback.NOOP, false)
                  );
                }
                catch (Exception e) {
                  loadDropHandler.removeSegment(segment, DataSegmentChangeCallback.NOOP, false);
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
          log.makeAlert("[%,d] errors seen while loading segments on startup", failedSegments.size())
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
      log.makeAlert(e, "Failed to load segments on startup -- likely problem with announcing.")
         .addData("numSegments", segmentsOnStartup.size())
         .emit();
    }
    finally {
      bootstrapExecutor.shutdownNow();
      backgroundAnnouncerExecutor.shutdownNow();
      stopwatch.stop();
      // At this stage, all tasks have been submitted, send a shutdown command to cleanup any resources alloted
      // for the bootstrapping function.
      segmentManager.shutdownBootstrap();
      log.info("Loaded [%d] segments on startup in [%,d]ms.", segmentsOnStartup.size(), stopwatch.millisElapsed());
    }
  }

  /**
   * @return a list of bootstrap segments. When bootstrap segments cannot be found, an empty list is returned.
   * The bootstrap segments returned are filtered by the broadcast datasources indicated by {@link DataSourceTaskIdHolder#getBroadcastDatasourceLoadingSpec()}
   * if applicable.
   */
  private List<DataSegment> getBootstrapSegments()
  {
    final BroadcastDatasourceLoadingSpec.Mode mode = datasourceHolder.getBroadcastDatasourceLoadingSpec().getMode();
    if (mode == BroadcastDatasourceLoadingSpec.Mode.NONE) {
      log.info("Skipping fetch of bootstrap segments.");
      return ImmutableList.of();
    }

    log.info("Fetching bootstrap segments from the coordinator with BroadcastDatasourceLoadingSpec mode[%s].", mode);
    final Stopwatch stopwatch = Stopwatch.createStarted();

    List<DataSegment> bootstrapSegments = new ArrayList<>();

    try {
      final BootstrapSegmentsResponse response =
          FutureUtils.getUnchecked(coordinatorClient.fetchBootstrapSegments(), true);
      if (mode == BroadcastDatasourceLoadingSpec.Mode.ONLY_REQUIRED) {
        final Set<String> broadcastDatasourcesToLoad = datasourceHolder.getBroadcastDatasourceLoadingSpec().getBroadcastDatasourcesToLoad();
        final List<DataSegment> filteredBroadcast = new ArrayList<>();
        response.getIterator().forEachRemaining(segment -> {
          if (broadcastDatasourcesToLoad.contains(segment.getDataSource())) {
            filteredBroadcast.add(segment);
          }
        });
        bootstrapSegments = filteredBroadcast;
      } else {
        bootstrapSegments = ImmutableList.copyOf(response.getIterator());
      }
    }
    catch (Exception e) {
      log.warn("Error fetching bootstrap segments from the coordinator: [%s]. ", e.getMessage());
    }
    finally {
      stopwatch.stop();
      final long fetchRunMillis = stopwatch.millisElapsed();
      emitter.emit(new ServiceMetricEvent.Builder().setMetric("segment/bootstrap/time", fetchRunMillis));
      emitter.emit(new ServiceMetricEvent.Builder().setMetric("segment/bootstrap/count", bootstrapSegments.size()));
      log.info("Fetched [%d] bootstrap segments in [%d]ms.", bootstrapSegments.size(), fetchRunMillis);
    }
    return bootstrapSegments;
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

    private final int announceIntervalMillis;
    private final DataSegmentAnnouncer segmentAnnouncer;
    private final ScheduledExecutorService exec;
    private final LinkedBlockingQueue<DataSegment> queue;
    private final SettableFuture<Boolean> doneAnnouncing;

    private final Object lock = new Object();

    private volatile boolean finished = false;
    @Nullable
    private volatile ScheduledFuture startedAnnouncing = null;
    @Nullable
    private volatile ScheduledFuture nextAnnoucement = null;

    BackgroundSegmentAnnouncer(
        DataSegmentAnnouncer segmentAnnouncer,
        ScheduledExecutorService exec,
        int announceIntervalMillis
    )
    {
      this.segmentAnnouncer = segmentAnnouncer;
      this.exec = exec;
      this.announceIntervalMillis = announceIntervalMillis;
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
      if (announceIntervalMillis <= 0) {
        log.info("Skipping background segment announcing as announceIntervalMillis is [%d].", announceIntervalMillis);
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
                      segmentAnnouncer.announceSegments(segments);
                      nextAnnoucement = exec.schedule(this, announceIntervalMillis, TimeUnit.MILLISECONDS);
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
          announceIntervalMillis,
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
          segmentAnnouncer.announceSegments(segments);
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
}
