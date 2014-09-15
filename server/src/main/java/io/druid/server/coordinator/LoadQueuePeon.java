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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
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

  private static Comparator<DataSegment> segmentComparator = Comparators.inverse(DataSegment.bucketMonthComparator());

  private static void executeCallbacks(List<LoadPeonCallback> callbacks)
  {
    for (LoadPeonCallback callback : callbacks) {
      if (callback != null) {
        callback.execute();
      }
    }
  }

  private final CuratorFramework curator;
  private final String basePath;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService zkWritingExecutor;
  private final ExecutorService callBackExecutor;
  private final DruidCoordinatorConfig config;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToLoad = new ConcurrentSkipListMap<>(
      segmentComparator
  );
  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToDrop = new ConcurrentSkipListMap<>(
      segmentComparator
  );

  private final Object lock = new Object();

  private volatile SegmentHolder currentlyProcessing = null;

  LoadQueuePeon(
      CuratorFramework curator,
      String basePath,
      ObjectMapper jsonMapper,
      ScheduledExecutorService zkWritingExecutor,
      ExecutorService callbackExecutor,
      DruidCoordinatorConfig config
  )
  {
    this.curator = curator;
    this.basePath = basePath;
    this.jsonMapper = jsonMapper;
    this.callBackExecutor = callbackExecutor;
    this.zkWritingExecutor = zkWritingExecutor;
    this.config = config;
  }

  @JsonProperty
  public Set<DataSegment> getSegmentsToLoad()
  {
    return segmentsToLoad.keySet();
  }

  @JsonProperty
  public Set<DataSegment> getSegmentsToDrop()
  {
    return segmentsToDrop.keySet();
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
      final DataSegment segment,
      final LoadPeonCallback callback
  )
  {
    synchronized (lock) {
      if ((currentlyProcessing != null) &&
          currentlyProcessing.getSegmentIdentifier().equals(segment.getIdentifier())) {
        if (callback != null) {
          currentlyProcessing.addCallback(callback);
        }
        return;
      }
    }

    synchronized (lock) {
      final SegmentHolder existingHolder = segmentsToLoad.get(segment);
      if (existingHolder != null) {
        if ((callback != null)) {
          existingHolder.addCallback(callback);
        }
        return;
      }
    }

    log.info("Asking server peon[%s] to load segment[%s]", basePath, segment.getIdentifier());
    queuedSize.addAndGet(segment.getSize());
    segmentsToLoad.put(segment, new SegmentHolder(segment, LOAD, Arrays.asList(callback)));
    doNext();
  }

  public void dropSegment(
      final DataSegment segment,
      final LoadPeonCallback callback
  )
  {
    synchronized (lock) {
      if ((currentlyProcessing != null) &&
          currentlyProcessing.getSegmentIdentifier().equals(segment.getIdentifier())) {
        if (callback != null) {
          currentlyProcessing.addCallback(callback);
        }
        return;
      }
    }

    synchronized (lock) {
      final SegmentHolder existingHolder = segmentsToDrop.get(segment);
      if (existingHolder != null) {
        if (callback != null) {
          existingHolder.addCallback(callback);
        }
        return;
      }
    }

    log.info("Asking server peon[%s] to drop segment[%s]", basePath, segment.getIdentifier());
    segmentsToDrop.put(segment, new SegmentHolder(segment, DROP, Arrays.asList(callback)));
    doNext();
  }

  private void doNext()
  {
    synchronized (lock) {
      if (currentlyProcessing == null) {
        if (!segmentsToDrop.isEmpty()) {
          currentlyProcessing = segmentsToDrop.firstEntry().getValue();
          log.info("Server[%s] dropping [%s]", basePath, currentlyProcessing.getSegmentIdentifier());
        } else if (!segmentsToLoad.isEmpty()) {
          currentlyProcessing = segmentsToLoad.firstEntry().getValue();
          log.info("Server[%s] loading [%s]", basePath, currentlyProcessing.getSegmentIdentifier());
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
                    if (currentlyProcessing == null) {
                      log.makeAlert("Crazy race condition! server[%s]", basePath)
                         .emit();
                      actionCompleted();
                      doNext();
                      return;
                    }
                    log.info("Server[%s] processing segment[%s]", basePath, currentlyProcessing.getSegmentIdentifier());
                    final String path = ZKPaths.makePath(basePath, currentlyProcessing.getSegmentIdentifier());
                    final byte[] payload = jsonMapper.writeValueAsBytes(currentlyProcessing.getChangeRequest());
                    curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);

                    zkWritingExecutor.schedule(
                        new Runnable()
                        {
                          @Override
                          public void run()
                          {
                            try {
                              if (curator.checkExists().forPath(path) != null) {
                                failAssign(new ISE("%s was never removed! Failing this operation!", path));
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
            "Server[%s] skipping doNext() because something is currently loading[%s].",
            basePath,
            currentlyProcessing.getSegmentIdentifier()
        );
      }
    }
  }

  private void actionCompleted()
  {
    if (currentlyProcessing != null) {
      switch (currentlyProcessing.getType()) {
        case LOAD:
          segmentsToLoad.remove(currentlyProcessing.getSegment());
          queuedSize.addAndGet(-currentlyProcessing.getSegmentSize());
          break;
        case DROP:
          segmentsToDrop.remove(currentlyProcessing.getSegment());
          break;
        default:
          throw new UnsupportedOperationException();
      }

      final List<LoadPeonCallback> callbacks = currentlyProcessing.getCallbacks();
      currentlyProcessing = null;
      callBackExecutor.execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              executeCallbacks(callbacks);
            }
          }
      );
    }
  }

  public void stop()
  {
    synchronized (lock) {
      if (currentlyProcessing != null) {
        executeCallbacks(currentlyProcessing.getCallbacks());
        currentlyProcessing = null;
      }

      if (!segmentsToDrop.isEmpty()) {
        for (SegmentHolder holder : segmentsToDrop.values()) {
          executeCallbacks(holder.getCallbacks());
        }
      }
      segmentsToDrop.clear();

      if (!segmentsToLoad.isEmpty()) {
        for (SegmentHolder holder : segmentsToLoad.values()) {
          executeCallbacks(holder.getCallbacks());
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
      if (currentlyProcessing == null) {
        log.warn("Server[%s] an entry[%s] was removed even though it wasn't loading!?", basePath, path);
        return;
      }
      if (!ZKPaths.getNodeFromPath(path).equals(currentlyProcessing.getSegmentIdentifier())) {
        log.warn(
            "Server[%s] entry [%s] was removed even though it's not what is currently loading[%s]",
            basePath, path, currentlyProcessing
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
      log.error(e, "Server[%s], throwable caught when submitting [%s].", basePath, currentlyProcessing);
      failedAssignCount.getAndIncrement();
      // Act like it was completed so that the coordinator gives it to someone else
      actionCompleted();
      doNext();
    }
  }

  private static class SegmentHolder
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

    public List<LoadPeonCallback> getCallbacks()
    {
      synchronized (callbacks) {
        return callbacks;
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
