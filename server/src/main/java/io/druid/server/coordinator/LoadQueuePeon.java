/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.guava.Comparators;
import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestLoad;
import io.druid.server.coordination.SegmentChangeRequestNoop;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(LoadQueuePeon.class);
  private static final int DROP = 0;
  private static final int LOAD = 1;

  private static Comparator<SegmentHolder> segmentHolderComparator = new Comparator<SegmentHolder>()
  {
    private Comparator<DataSegment> comparator = Comparators.inverse(DataSegment.bucketMonthComparator());

    @Override
    public int compare(SegmentHolder lhs, SegmentHolder rhs)
    {
      return comparator.compare(lhs.getSegment(), rhs.getSegment());
    }
  };

  private final CuratorFramework curator;
  private final String basePath;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService zkWritingExecutor;
  private final DruidCoordinatorConfig config;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  private final ConcurrentSkipListSet<SegmentHolder> segmentsToLoad = new ConcurrentSkipListSet<SegmentHolder>(
      segmentHolderComparator
  );
  private final ConcurrentSkipListSet<SegmentHolder> segmentsToDrop = new ConcurrentSkipListSet<SegmentHolder>(
      segmentHolderComparator
  );

  private final Object lock = new Object();

  private volatile SegmentHolder currentlyLoading = null;

  LoadQueuePeon(
      CuratorFramework curator,
      String basePath,
      ObjectMapper jsonMapper,
      ScheduledExecutorService zkWritingExecutor,
      DruidCoordinatorConfig config
  )
  {
    this.curator = curator;
    this.basePath = basePath;
    this.jsonMapper = jsonMapper;
    this.zkWritingExecutor = zkWritingExecutor;
    this.config = config;
  }

  @JsonProperty
  public Set<DataSegment> getSegmentsToLoad()
  {
    return new ConcurrentSkipListSet<DataSegment>(
        Collections2.transform(
            segmentsToLoad,
            new Function<SegmentHolder, DataSegment>()
            {
              @Override
              public DataSegment apply(SegmentHolder input)
              {
                return input.getSegment();
              }
            }
        )
    );
  }

  @JsonProperty
  public Set<DataSegment> getSegmentsToDrop()
  {
    return new ConcurrentSkipListSet<DataSegment>(
        Collections2.transform(
            segmentsToDrop,
            new Function<SegmentHolder, DataSegment>()
            {
              @Override
              public DataSegment apply(SegmentHolder input)
              {
                return input.getSegment();
              }
            }
        )
    );
  }

  public long getLoadQueueSize()
  {
    return queuedSize.get();
  }

  public int getAndResetFailedAssignCount()
  {
    return failedAssignCount.getAndSet(0);
  }

  public void loadSegment(
      DataSegment segment,
      LoadPeonCallback callback
  )
  {
    synchronized (lock) {
      if ((currentlyLoading != null) &&
          currentlyLoading.getSegmentIdentifier().equals(segment.getIdentifier())) {
        if (callback != null) {
          currentlyLoading.addCallback(callback);
        }
        return;
      }
    }

    SegmentHolder holder = new SegmentHolder(segment, LOAD, Arrays.asList(callback));

    synchronized (lock) {
      if (segmentsToLoad.contains(holder)) {
        if ((callback != null)) {
          currentlyLoading.addCallback(callback);
        }
        return;
      }
    }

    log.info("Asking server peon[%s] to load segment[%s]", basePath, segment);
    queuedSize.addAndGet(segment.getSize());
    segmentsToLoad.add(holder);
    doNext();
  }

  public void dropSegment(
      DataSegment segment,
      LoadPeonCallback callback
  )
  {
    synchronized (lock) {
      if ((currentlyLoading != null) &&
          currentlyLoading.getSegmentIdentifier().equals(segment.getIdentifier())) {
        if (callback != null) {
          currentlyLoading.addCallback(callback);
        }
        return;
      }
    }

    SegmentHolder holder = new SegmentHolder(segment, DROP, Arrays.asList(callback));

    synchronized (lock) {
      if (segmentsToDrop.contains(holder)) {
        if (callback != null) {
          currentlyLoading.addCallback(callback);
        }
        return;
      }
    }

    log.info("Asking server peon[%s] to drop segment[%s]", basePath, segment);
    segmentsToDrop.add(holder);
    doNext();
  }

  private void doNext()
  {
    synchronized (lock) {
      if (currentlyLoading == null) {
        if (!segmentsToDrop.isEmpty()) {
          currentlyLoading = segmentsToDrop.first();
          log.info("Server[%s] dropping [%s]", basePath, currentlyLoading);
        } else if (!segmentsToLoad.isEmpty()) {
          currentlyLoading = segmentsToLoad.first();
          log.info("Server[%s] loading [%s]", basePath, currentlyLoading);
        } else {
          return;
        }

        zkWritingExecutor.execute(
            new Runnable()
            {
              @Override
              public void run()
              {
                synchronized (lock) {
                  try {
                    if (currentlyLoading == null) {
                      log.makeAlert("Crazy race condition! server[%s]", basePath)
                         .emit();
                      actionCompleted();
                      doNext();
                      return;
                    }
                    log.info("Server[%s] adding segment[%s]", basePath, currentlyLoading.getSegmentIdentifier());
                    final String path = ZKPaths.makePath(basePath, currentlyLoading.getSegmentIdentifier());
                    final byte[] payload = jsonMapper.writeValueAsBytes(currentlyLoading.getChangeRequest());
                    curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);

                    zkWritingExecutor.schedule(
                        new Runnable()
                        {
                          @Override
                          public void run()
                          {
                            try {
                              if (curator.checkExists().forPath(path) != null) {
                                failAssign(new ISE("%s was never removed! Failing this assign!", path));
                              }
                            }
                            catch (Exception e) {
                              failAssign(e);
                            }
                          }
                        },
                        config.getLoadTimeoutDelay().getMillis(),
                        TimeUnit.MILLISECONDS
                    );

                    final Stat stat = curator.checkExists().usingWatcher(
                        new CuratorWatcher()
                        {
                          @Override
                          public void process(WatchedEvent watchedEvent) throws Exception
                          {
                            switch (watchedEvent.getType()) {
                              case NodeDeleted:
                                entryRemoved(watchedEvent.getPath());
                            }
                          }
                        }
                    ).forPath(path);

                    if (stat == null) {
                      final byte[] noopPayload = jsonMapper.writeValueAsBytes(new SegmentChangeRequestNoop());

                      // Create a node and then delete it to remove the registered watcher.  This is a work-around for
                      // a zookeeper race condition.  Specifically, when you set a watcher, it fires on the next event
                      // that happens for that node.  If no events happen, the watcher stays registered foreverz.
                      // Couple that with the fact that you cannot set a watcher when you create a node, but what we
                      // want is to create a node and then watch for it to get deleted.  The solution is that you *can*
                      // set a watcher when you check to see if it exists so, we first create the node and then set a
                      // watcher on its existence.  However, if already does not exist by the time the existence check
                      // returns, then the watcher that was set will never fire (nobody will ever create the node
                      // again) and thus lead to a slow, but real, memory leak.  So, we create another node to cause
                      // that watcher to fire and delete it right away.
                      //
                      // We do not create the existence watcher first, because then it will fire when we create the
                      // node and we'll have the same race when trying to refresh that watcher.
                      curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, noopPayload);

                      entryRemoved(path);
                    }
                  }
                  catch (Exception e) {
                    failAssign(e);
                  }
                }
              }
            }
        );
      } else {
        log.info(
            "Server[%s] skipping doNext() because something is currently loading[%s].", basePath, currentlyLoading
        );
      }
    }
  }

  private void actionCompleted()
  {
    if (currentlyLoading != null) {
      switch (currentlyLoading.getType()) {
        case LOAD:
          segmentsToLoad.remove(currentlyLoading);
          queuedSize.addAndGet(-currentlyLoading.getSegmentSize());
          break;
        case DROP:
          segmentsToDrop.remove(currentlyLoading);
          break;
        default:
          throw new UnsupportedOperationException();
      }
      currentlyLoading.executeCallbacks();
      currentlyLoading = null;
    }
  }

  public void stop()
  {
    synchronized (lock) {
      if (currentlyLoading != null) {
        currentlyLoading.executeCallbacks();
        currentlyLoading = null;
      }

      if (!segmentsToDrop.isEmpty()) {
        for (SegmentHolder holder : segmentsToDrop) {
          holder.executeCallbacks();
        }
      }
      segmentsToDrop.clear();

      if (!segmentsToLoad.isEmpty()) {
        for (SegmentHolder holder : segmentsToLoad) {
          holder.executeCallbacks();
        }
      }
      segmentsToLoad.clear();

      queuedSize.set(0L);
      failedAssignCount.set(0);
    }
  }

  private void entryRemoved(String path)
  {
    synchronized (lock) {
      if (currentlyLoading == null) {
        log.warn("Server[%s] an entry[%s] was removed even though it wasn't loading!?", basePath, path);
        return;
      }
      if (!ZKPaths.getNodeFromPath(path).equals(currentlyLoading.getSegmentIdentifier())) {
        log.warn(
            "Server[%s] entry [%s] was removed even though it's not what is currently loading[%s]",
            basePath, path, currentlyLoading
        );
        return;
      }
      actionCompleted();
      log.info("Server[%s] done processing [%s]", basePath, path);
    }

    doNext();
  }

  private void failAssign(Exception e)
  {
    synchronized (lock) {
      log.error(e, "Server[%s], throwable caught when submitting [%s].", basePath, currentlyLoading);
      failedAssignCount.getAndIncrement();
      // Act like it was completed so that the coordinator gives it to someone else
      actionCompleted();
      doNext();
    }
  }

  private class SegmentHolder
  {
    private final DataSegment segment;
    private final DataSegmentChangeRequest changeRequest;
    private final int type;
    private final List<LoadPeonCallback> callbacks = Lists.newArrayList();

    private SegmentHolder(
        DataSegment segment,
        int type,
        Collection<LoadPeonCallback> callbacks
    )
    {
      this.segment = segment;
      this.type = type;
      this.changeRequest = (type == LOAD)
                           ? new SegmentChangeRequestLoad(segment)
                           : new SegmentChangeRequestDrop(segment);
      this.callbacks.addAll(callbacks);
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public int getType()
    {
      return type;
    }

    public String getSegmentIdentifier()
    {
      return segment.getIdentifier();
    }

    public long getSegmentSize()
    {
      return segment.getSize();
    }

    public void addCallbacks(Collection<LoadPeonCallback> newCallbacks)
    {
      synchronized (callbacks) {
        callbacks.addAll(newCallbacks);
      }
    }

    public void addCallback(LoadPeonCallback newCallback)
    {
      synchronized (callbacks) {
        callbacks.add(newCallback);
      }
    }

    public void executeCallbacks()
    {
      synchronized (callbacks) {
        for (LoadPeonCallback callback : callbacks) {
          if (callback != null) {
            callback.execute();
          }
        }
        callbacks.clear();
      }
    }

    public DataSegmentChangeRequest getChangeRequest()
    {
      return changeRequest;
    }

    @Override
    public String toString()
    {
      return changeRequest.toString();
    }
  }
}