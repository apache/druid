/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
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
 */
public class ZkCoordinator extends BaseZkCoordinator
{
  private static final EmittingLogger log = new EmittingLogger(ZkCoordinator.class);

  private final ObjectMapper jsonMapper;
  private final SegmentLoaderConfig config;
  private final DataSegmentAnnouncer announcer;
  private final ServerManager serverManager;
  private final ScheduledExecutorService exec;

  @Inject
  public ZkCoordinator(
      ObjectMapper jsonMapper,
      SegmentLoaderConfig config,
      ZkPathsConfig zkPaths,
      DruidServerMetadata me,
      DataSegmentAnnouncer announcer,
      CuratorFramework curator,
      ServerManager serverManager,
      ScheduledExecutorFactory factory
  )
  {
    super(jsonMapper, zkPaths, config, me, curator);

    this.jsonMapper = jsonMapper;
    this.config = config;
    this.announcer = announcer;
    this.serverManager = serverManager;

    this.exec = factory.create(1, "ZkCoordinator-Exec--%d");
  }

  @Override
  public void loadLocalCache()
  {
    final long start = System.currentTimeMillis();
    File baseDir = config.getInfoDir();
    if (!baseDir.exists() && !config.getInfoDir().mkdirs()) {
      return;
    }

    List<DataSegment> cachedSegments = Lists.newArrayList();
    File[] segmentsToLoad = baseDir.listFiles();
    for (int i = 0; i < segmentsToLoad.length; i++) {
      File file = segmentsToLoad[i];
      log.info("Loading segment cache file [%d/%d][%s].", i, segmentsToLoad.length, file);
      try {
        DataSegment segment = jsonMapper.readValue(file, DataSegment.class);
        if (serverManager.isSegmentCached(segment)) {
          cachedSegments.add(segment);
        } else {
          log.warn("Unable to find cache file for %s. Deleting lookup entry", segment.getIdentifier());

          File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
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

    addSegments(
        cachedSegments,
        new DataSegmentChangeCallback()
        {
          @Override
          public void execute()
          {
            log.info("Cache load took %,d ms", System.currentTimeMillis() - start);
          }
        }
    );
  }

  @Override
  public DataSegmentChangeHandler getDataSegmentChangeHandler()
  {
    return ZkCoordinator.this;
  }

  private boolean loadSegment(DataSegment segment, DataSegmentChangeCallback callback) throws SegmentLoadingException
  {
    final boolean loaded;
    try {
      loaded = serverManager.loadSegment(segment);
    }
    catch (Exception e) {
      removeSegment(segment, callback);
      throw new SegmentLoadingException(e, "Exception loading segment[%s]", segment.getIdentifier());
    }

    if (loaded) {
      File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
      if (!segmentInfoCacheFile.exists()) {
        try {
          jsonMapper.writeValue(segmentInfoCacheFile, segment);
        }
        catch (IOException e) {
          removeSegment(segment, callback);
          throw new SegmentLoadingException(
              e, "Failed to write to disk segment info cache file[%s]", segmentInfoCacheFile
          );
        }
      }
    }
    return loaded;
  }

  @Override
  public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
  {
    try {
      log.info("Loading segment %s", segment.getIdentifier());
      if(loadSegment(segment, callback)) {
        try {
          announcer.announceSegment(segment);
        }
        catch (IOException e) {
          throw new SegmentLoadingException(e, "Failed to announce segment[%s]", segment.getIdentifier());
        }
      }
    }
    catch (SegmentLoadingException e) {
      log.makeAlert(e, "Failed to load segment for dataSource")
         .addData("segment", segment)
         .emit();
    }
    finally {
      callback.execute();
    }
  }

  private void addSegments(Collection<DataSegment> segments, final DataSegmentChangeCallback callback)
  {
    ExecutorService loadingExecutor = null;
    try (final BackgroundSegmentAnnouncer backgroundSegmentAnnouncer =
             new BackgroundSegmentAnnouncer(announcer, exec, config.getAnnounceIntervalMillis())) {

      backgroundSegmentAnnouncer.startAnnouncing();

      loadingExecutor = Execs.multiThreaded(config.getNumBootstrapThreads(), "ZkCoordinator-loading-%s");

      final int numSegments = segments.size();
      final CountDownLatch latch = new CountDownLatch(numSegments);
      final AtomicInteger counter = new AtomicInteger(0);
      final CopyOnWriteArrayList<DataSegment> failedSegments = new CopyOnWriteArrayList<>();
      for (final DataSegment segment : segments) {
        loadingExecutor.submit(
            new Runnable() {
              @Override
              public void run() {
                try {
                  log.info("Loading segment[%d/%d][%s]", counter.getAndIncrement(), numSegments, segment.getIdentifier());
                  final boolean loaded = loadSegment(segment, callback);
                  if (loaded) {
                    try {
                      backgroundSegmentAnnouncer.announceSegment(segment);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new SegmentLoadingException(e, "Loading Interrupted");
                    }
                  }
                } catch (SegmentLoadingException e) {
                  log.error(e, "[%s] failed to load", segment.getIdentifier());
                  failedSegments.add(segment);
                } finally {
                  latch.countDown();
                }
              }
            }
        );
      }

      try{
        latch.await();

        if(failedSegments.size() > 0) {
          log.makeAlert("%,d errors seen while loading segments", failedSegments.size())
              .addData("failedSegments", failedSegments);
        }
      } catch(InterruptedException e) {
        Thread.currentThread().interrupt();
        log.makeAlert(e, "LoadingInterrupted");
      }

      backgroundSegmentAnnouncer.finishAnnouncing();
    }
    catch (SegmentLoadingException e) {
      log.makeAlert(e, "Failed to load segments -- likely problem with announcing.")
          .addData("numSegments", segments.size())
          .emit();
    }
    finally {
      callback.execute();
      if (loadingExecutor != null) {
        loadingExecutor.shutdownNow();
      }
    }
  }


  @Override
  public void removeSegment(final DataSegment segment, final DataSegmentChangeCallback callback)
  {
    try {
      announcer.unannounceSegment(segment);

      log.info("Completely removing [%s] in [%,d] millis", segment.getIdentifier(), config.getDropSegmentDelayMillis());
      exec.schedule(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                serverManager.dropSegment(segment);

                File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
                if (!segmentInfoCacheFile.delete()) {
                  log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to remove segment! Possible resource leak!")
                   .addData("segment", segment)
                   .emit();
              }
            }
          },
          config.getDropSegmentDelayMillis(),
          TimeUnit.MILLISECONDS
      );
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to remove segment")
         .addData("segment", segment)
         .emit();
    }
    finally {
      callback.execute();
    }
  }

  private static class BackgroundSegmentAnnouncer implements AutoCloseable {
    private static final EmittingLogger log = new EmittingLogger(BackgroundSegmentAnnouncer.class);

    private final int intervalMillis;
    private final DataSegmentAnnouncer announcer;
    private final ScheduledExecutorService exec;
    private final LinkedBlockingQueue<DataSegment> queue;
    private final SettableFuture<Boolean> doneAnnouncing;

    private final Object lock = new Object();

    private volatile boolean finished = false;
    private volatile ScheduledFuture startedAnnouncing = null;
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
      this.queue = Queues.newLinkedBlockingQueue();
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
                    final List<DataSegment> segments = Lists.newLinkedList();
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
          final List<DataSegment> segments = Lists.newLinkedList();
          queue.drainTo(segments);
          announcer.announceSegments(segments);
        }
        catch (Exception e) {
          throw new SegmentLoadingException(e, "Failed to announce segments[%s]", queue);
        }

        // get any exception that may have been thrown in background annoucing
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
