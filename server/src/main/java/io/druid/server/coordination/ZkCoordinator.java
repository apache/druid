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
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.SegmentManager;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentManager segmentManager;
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
      DataSegmentServerAnnouncer serverAnnouncer,
      CuratorFramework curator,
      SegmentManager segmentManager,
      ScheduledExecutorFactory factory
  )
  {
    this.jsonMapper = jsonMapper;
    this.zkPaths = zkPaths;
    this.config = config;
    this.me = me;
    this.curator = curator;
    this.announcer = announcer;
    this.serverAnnouncer = serverAnnouncer;
    this.segmentManager = segmentManager;

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
        serverAnnouncer.announce();

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
        serverAnnouncer.unannounce();
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

  private void loadLocalCache()
  {
    final long loadLocalCacheStartMs = System.currentTimeMillis();
    File baseDir = config.getInfoDir();
    if (!baseDir.exists() && !config.getInfoDir().mkdirs()) {
      return;
    }

    List<DataSegment> cachedSegments = Lists.newArrayList();
    File[] segmentsToLoad = baseDir.listFiles();
    int ignored = 0;
    for (int i = 0; i < segmentsToLoad.length; i++) {
      File file = segmentsToLoad[i];
      log.info("Loading segment cache file [%d/%d][%s].", i, segmentsToLoad.length, file);
      try {
        final DataSegment segment = jsonMapper.readValue(file, DataSegment.class);

        if (!segment.getIdentifier().equals(file.getName())) {
          log.warn("Ignoring cache file[%s] for segment[%s].", file.getPath(), segment.getIdentifier());
          ignored++;
        } else if (segmentManager.isSegmentCached(segment)) {
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

    if (ignored > 0) {
      log.makeAlert("Ignored misnamed segment cache files on startup.")
         .addData("numIgnored", ignored)
         .emit();
    }

    addSegments(cachedSegments);
    log.info("Cache load took %,d ms", System.currentTimeMillis() - loadLocalCacheStartMs);
  }

  public DataSegmentChangeHandler getDataSegmentChangeHandler()
  {
    return ZkCoordinator.this;
  }

  /**
   * Load a single segment. If the segment is loaded successfully, this function simply returns. Otherwise it will
   * throw a SegmentLoadingException
   *
   * @throws SegmentLoadingException
   */
  private void loadSegment(DataSegment segment, DataSegmentChangeCallback callback) throws SegmentLoadingException
  {
    final boolean loaded;
    try {
      loaded = segmentManager.loadSegment(segment);
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
      try {
        announcer.announceSegment(segment);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Failed to announce segment[%s]", segment.getIdentifier());
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

  private void addSegments(Collection<DataSegment> segments)
  {
    ExecutorService loadingExecutor = null;
    final BackgroundSegmentAnnouncer segmentAnnouncer = new BackgroundSegmentAnnouncer(announcer, exec, config);
    try {
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
                  loadSegment(segment, () -> {});
                  try {
                    segmentAnnouncer.announceSegment(segment);
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SegmentLoadingException(e, "Loading Interrupted");
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
    }
    finally {
      segmentAnnouncer.finishAnnouncingAsync();
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
                    segmentManager.dropSegment(segment);

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

  private static class BackgroundSegmentAnnouncer
  {
    private static final EmittingLogger log = new EmittingLogger(BackgroundSegmentAnnouncer.class);

    private final DataSegmentAnnouncer announcer;
    private final SegmentQueue queue;
    @Nullable
    private final ScheduledFuture<?> periodicBackgroundAnnouncement;

    private volatile boolean finished = false;

    public BackgroundSegmentAnnouncer(
        DataSegmentAnnouncer announcer,
        ScheduledExecutorService exec,
        SegmentLoaderConfig config
    )
    {
      this.announcer = announcer;
      this.queue = makeQueue(config);
      int intervalMillis = config.getAnnounceIntervalMillis();
      if (intervalMillis > 0) {
        log.info("Starting background segment announcing task");
        periodicBackgroundAnnouncement = exec.scheduleWithFixedDelay(
            this::periodicAnnounce,
            intervalMillis,
            intervalMillis,
            TimeUnit.MILLISECONDS
        );
      } else {
        periodicBackgroundAnnouncement = null;
      }
    }

    private static SegmentQueue makeQueue(SegmentLoaderConfig config)
    {
      int delaySeconds = config.getLocalCachedSegmentLoadingAnnounceDelaySeconds();
      if (delaySeconds == 0) {
        return new SimpleSegmentQueue();
      } else {
        return new DelayedSegmentQueue(TimeUnit.SECONDS.toNanos(delaySeconds));
      }
    }

    public void announceSegment(final DataSegment segment) throws InterruptedException
    {
      if (finished) {
        throw new ISE("Announce segment called after finishAnnouncing");
      }
      queue.put(segment);
    }

    private void periodicAnnounce()
    {
      if (!(finished && queue.isEmpty())) {
        final List<DataSegment> segments = queue.drainAvailable();
        try {
          // With delays, it might be that we drained no elements this time
          if (!segments.isEmpty()) {
            announcer.announceSegments(segments);
          }
        }
        catch (IOException e) {
          throw new RuntimeException(new SegmentLoadingException(e, "Failed to announce segments[%s]", segments));
        }
      }
    }

    void finishAnnouncingAsync()
    {
      Execs.makeThread(
          "ZkCoordinator - Segment Announce Finisher",
          () -> {
            try {
              finishAnnouncing();
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to announce segments -- likely problem with announcing.")
                 .emit();
            }
          },
          true
      ).start();
    }

    void finishAnnouncing() throws SegmentLoadingException
    {
      try {
        cancelPeriodicBackgroundAnnouncement();
        final long finishAnnouncingStartMs = System.currentTimeMillis();
        finished = true;
        // announce any remaining segments
        final List<DataSegment> segments = queue.takeAllRemaining();
        try {
          announcer.announceSegments(segments);
        }
        catch (Exception e) {
          throw new SegmentLoadingException(e, "Failed to announce segments[%s]", segments);
        }
        finally {
          log.info(
              "Finished background segment accouncing in %,d ms",
              System.currentTimeMillis() - finishAnnouncingStartMs
          );
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SegmentLoadingException(e, "Announcing Interrupted");
      }
    }

    private void cancelPeriodicBackgroundAnnouncement() throws InterruptedException
    {
      if (periodicBackgroundAnnouncement == null) {
        return;
      }
      periodicBackgroundAnnouncement.cancel(false);
      try {
        periodicBackgroundAnnouncement.get();
      }
      catch (CancellationException e) {
        // expected, ignore
      }
      catch (ExecutionException e) {
        log.makeAlert(e.getCause(), "Failed to announce segments in background")
           .emit();
      }
    }
  }

  private interface SegmentQueue
  {
    void put(DataSegment segment) throws InterruptedException;
    List<DataSegment> drainAvailable();
    List<DataSegment> takeAllRemaining() throws InterruptedException;
    boolean isEmpty();
  }

  private static class SimpleSegmentQueue implements SegmentQueue
  {
    private final BlockingQueue<DataSegment> delegate = new LinkedBlockingQueue<>();

    @Override
    public void put(DataSegment segment) throws InterruptedException
    {
      delegate.put(segment);
    }

    @Override
    public List<DataSegment> drainAvailable()
    {
      List<DataSegment> list = new ArrayList<>(delegate.size());
      delegate.drainTo(list);
      return list;
    }

    @Override
    public List<DataSegment> takeAllRemaining()
    {
      return drainAvailable();
    }

    @Override
    public boolean isEmpty()
    {
      return delegate.isEmpty();
    }
  }

  private static class DelayedSegmentQueue implements SegmentQueue
  {
    private final DelayQueue<DelayedDataSegment> delayQueue = new DelayQueue<>();
    private final long delayNs;

    private DelayedSegmentQueue(long delayNs)
    {
      this.delayNs = delayNs;
    }

    @Override
    public void put(DataSegment segment)
    {
      delayQueue.put(new DelayedDataSegment(segment, System.nanoTime() + delayNs));
    }

    @Override
    public List<DataSegment> drainAvailable()
    {
      List<DelayedDataSegment> list = new ArrayList<>();
      delayQueue.drainTo(list);
      return list.stream().map(d -> d.segment).collect(Collectors.toList());
    }

    @Override
    public List<DataSegment> takeAllRemaining() throws InterruptedException
    {
      List<DataSegment> list = new ArrayList<>(delayQueue.size());
      while (!delayQueue.isEmpty()) {
        list.add(delayQueue.take().segment);
      }
      return list;
    }

    @Override
    public boolean isEmpty()
    {
      return delayQueue.isEmpty();
    }
  }

  @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
  private static class DelayedDataSegment implements Delayed
  {
    final DataSegment segment;
    final long timeOutNs;

    private DelayedDataSegment(DataSegment segment, long timeOutNs)
    {
      this.segment = segment;
      this.timeOutNs = timeOutNs;
    }

    @Override
    public long getDelay(TimeUnit unit)
    {
      return unit.convert(timeOutNs - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @SuppressWarnings("SubtractionInCompareTo")
    @Override
    public int compareTo(Delayed o)
    {
      // nanosecond "stamps" should be compared with -, not Long.compare(), because they may overflow themselves
      return Ints.saturatedCast(timeOutNs - ((DelayedDataSegment) o).timeOutNs);
    }
  }
}
