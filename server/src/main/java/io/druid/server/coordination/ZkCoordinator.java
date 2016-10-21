/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class ZkCoordinator implements DataSegmentChangeHandler
{
  private static final EmittingLogger log = new EmittingLogger(ZkCoordinator.class);

  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final ZkPathsConfig zkPaths;
  private final SegmentLoaderConfig config;
  private final DruidServerMetadata me;
  private final CuratorFramework curator;
  private final DataSegmentAnnouncer announcer;
  private final ServerManager serverManager;
  private final ScheduledExecutorService exec;
  private final ConcurrentSkipListSet<DataSegment> segmentsToDelete;


  private volatile PathChildrenCache loadQueueCache;
  private volatile boolean started = false;

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
    this.jsonMapper = jsonMapper;
    this.zkPaths = zkPaths;
    this.config = config;
    this.me = me;
    this.curator = curator;
    this.announcer = announcer;
    this.serverManager = serverManager;

    this.exec = factory.create(1, "ZkCoordinator-Exec--%d");
    this.segmentsToDelete = new ConcurrentSkipListSet<>();
  }

  @LifecycleStart
  public void start() throws IOException
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Starting zkCoordinator for server[%s]", me.getName());

      final String loadQueueLocation = ZKPaths.makePath(zkPaths.getLoadQueuePath(), me.getName());
      final String servedSegmentsLocation = ZKPaths.makePath(zkPaths.getServedSegmentsPath(), me.getName());
      final String liveSegmentsLocation = ZKPaths.makePath(zkPaths.getLiveSegmentsPath(), me.getName());

      loadQueueCache = new PathChildrenCache(
          curator,
          loadQueueLocation,
          true,
          true,
          Execs.multiThreaded(config.getNumLoadingThreads(), "ZkCoordinator-%s")
      );

      try {
        curator.newNamespaceAwareEnsurePath(loadQueueLocation).ensure(curator.getZookeeperClient());
        curator.newNamespaceAwareEnsurePath(servedSegmentsLocation).ensure(curator.getZookeeperClient());
        curator.newNamespaceAwareEnsurePath(liveSegmentsLocation).ensure(curator.getZookeeperClient());

        loadLocalCache();

        loadQueueCache.getListenable().addListener(
            new PathChildrenCacheListener()
            {
              @Override
              public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
              {
                final ChildData child = event.getData();
                switch (event.getType()) {
                  case CHILD_ADDED:
                    final String path = child.getPath();
                    final DataSegmentChangeRequest request = jsonMapper.readValue(
                        child.getData(), DataSegmentChangeRequest.class
                    );

                    log.info("New request[%s] with zNode[%s].", request.asString(), path);

                    try {
                      request.go(
                          getDataSegmentChangeHandler(),
                          new DataSegmentChangeCallback()
                          {
                            boolean hasRun = false;

                            @Override
                            public void execute()
                            {
                              try {
                                if (!hasRun) {
                                  curator.delete().guaranteed().forPath(path);
                                  log.info("Completed request [%s]", request.asString());
                                  hasRun = true;
                                }
                              }
                              catch (Exception e) {
                                try {
                                  curator.delete().guaranteed().forPath(path);
                                }
                                catch (Exception e1) {
                                  log.error(e1, "Failed to delete zNode[%s], but ignoring exception.", path);
                                }
                                log.error(e, "Exception while removing zNode[%s]", path);
                                throw Throwables.propagate(e);
                              }
                            }
                          }
                      );
                    }
                    catch (Exception e) {
                      try {
                        curator.delete().guaranteed().forPath(path);
                      }
                      catch (Exception e1) {
                        log.error(e1, "Failed to delete zNode[%s], but ignoring exception.", path);
                      }

                      log.makeAlert(e, "Segment load/unload: uncaught exception.")
                         .addData("node", path)
                         .addData("nodeProperties", request)
                         .emit();
                    }

                    break;
                  case CHILD_REMOVED:
                    log.info("zNode[%s] was removed", event.getData().getPath());
                    break;
                  default:
                    log.info("Ignoring event[%s]", event);
                }
              }
            }
        );
        loadQueueCache.start();
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw Throwables.propagate(e);
      }

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    log.info("Stopping ZkCoordinator for [%s]", me);
    synchronized (lock) {
      if (!started) {
        return;
      }

      try {
        loadQueueCache.close();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      finally {
        loadQueueCache = null;
        started = false;
      }
    }
  }

  public boolean isStarted()
  {
    return started;
  }

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

  public DataSegmentChangeHandler getDataSegmentChangeHandler()
  {
    return ZkCoordinator.this;
  }

  /**
   * Load a single segment. If the segment is loaded succesfully, this function simply returns. Otherwise it will
   * throw a SegmentLoadingException
   *
   * @throws SegmentLoadingException
   */
  private void loadSegment(DataSegment segment, DataSegmentChangeCallback callback) throws SegmentLoadingException
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
  }

  @Override
  public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
  {
    try {
      log.info("Loading segment %s", segment.getIdentifier());
      /*
         The lock below is used to prevent a race condition when the scheduled runnable in removeSegment() starts,
         and if(segmentsToDelete.remove(segment)) returns true, in which case historical will start deleting segment
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
        synchronized (lock) {
          segmentsToDelete.remove(segment);
        }
      }
      loadSegment(segment, callback);
      if (!announcer.isAnnounced(segment)) {
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
            new Runnable()
            {
              @Override
              public void run()
              {
                try {
                  log.info(
                      "Loading segment[%d/%d][%s]",
                      counter.getAndIncrement(),
                      numSegments,
                      segment.getIdentifier()
                  );
                  loadSegment(segment, callback);
                  if (!announcer.isAnnounced(segment)) {
                    try {
                      backgroundSegmentAnnouncer.announceSegment(segment);
                    }
                    catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new SegmentLoadingException(e, "Loading Interrupted");
                    }
                  }
                }
                catch (SegmentLoadingException e) {
                  log.error(e, "[%s] failed to load", segment.getIdentifier());
                  failedSegments.add(segment);
                }
                finally {
                  latch.countDown();
                }
              }
            }
        );
      }

      try {
        latch.await();

        if (failedSegments.size() > 0) {
          log.makeAlert("%,d errors seen while loading segments", failedSegments.size())
             .addData("failedSegments", failedSegments);
        }
      }
      catch (InterruptedException e) {
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
      segmentsToDelete.add(segment);

      log.info("Completely removing [%s] in [%,d] millis", segment.getIdentifier(), config.getDropSegmentDelayMillis());
      exec.schedule(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                synchronized (lock) {
                  if (segmentsToDelete.remove(segment)) {
                    serverManager.dropSegment(segment);

                    File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
                    if (!segmentInfoCacheFile.delete()) {
                      log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
                    }
                  }
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

  public Collection<DataSegment> getPendingDeleteSnapshot()
  {
    return ImmutableList.copyOf(segmentsToDelete);
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
