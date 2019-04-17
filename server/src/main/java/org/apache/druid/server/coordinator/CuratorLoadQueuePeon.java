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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.SegmentChangeRequestNoop;
import org.apache.druid.timeline.DataSegment;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Use {@link HttpLoadQueuePeon} instead.
 */
@Deprecated
public class CuratorLoadQueuePeon extends LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(CuratorLoadQueuePeon.class);
  private static final int DROP = 0;
  private static final int LOAD = 1;

  private final CuratorFramework curator;
  private final String basePath;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService processingExecutor;
  /**
   * Threadpool with daemon threads running scheduled tasks that monitor whether
   * the zk nodes created for segment processing are removed
   */
  private final ScheduledExecutorService monitorNodeRemovedExecutor;
  private final ExecutorService callBackExecutor;
  private final DruidCoordinatorConfig config;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToLoad = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );
  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToDrop = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );
  private final ConcurrentSkipListSet<DataSegment> segmentsMarkedToDrop = new ConcurrentSkipListSet<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );

  CuratorLoadQueuePeon(
      CuratorFramework curator,
      String basePath,
      ObjectMapper jsonMapper,
      ScheduledExecutorService processingExecutor,
      ExecutorService callbackExecutor,
      DruidCoordinatorConfig config
  )
  {
    this.curator = curator;
    this.basePath = basePath;
    this.jsonMapper = jsonMapper;
    this.callBackExecutor = callbackExecutor;
    this.processingExecutor = processingExecutor;
    this.config = config;
    this.monitorNodeRemovedExecutor =
        Executors.newScheduledThreadPool(
            config.getNumZookeeperMonitorThreads(),
            Execs.makeThreadFactory("LoadQueuePeon-NodeRemovedMonitor--%d")
        );
  }

  @JsonProperty
  @Override
  public Set<DataSegment> getSegmentsToLoad()
  {
    return segmentsToLoad.keySet();
  }

  @JsonProperty
  @Override
  public Set<DataSegment> getSegmentsToDrop()
  {
    return segmentsToDrop.keySet();
  }

  @JsonProperty
  @Override
  public Set<DataSegment> getSegmentsMarkedToDrop()
  {
    return segmentsMarkedToDrop;
  }

  @Override
  public long getLoadQueueSize()
  {
    return queuedSize.get();
  }

  @Override
  public int getAndResetFailedAssignCount()
  {
    return failedAssignCount.getAndSet(0);
  }

  @Override
  public int getNumberOfSegmentsInQueue()
  {
    return segmentsToLoad.size();
  }

  @Override
  public void loadSegment(final DataSegment segment, final LoadPeonCallback callback)
  {

    final SegmentHolder existingHolder = segmentsToLoad.get(segment);
    if (existingHolder != null) {
      if ((callback != null)) {
        existingHolder.addCallback(callback);
      }
      return;
    }
    log.debug("Asking server peon[%s] to load segment[%s]", basePath, segment.getId());
    queuedSize.addAndGet(segment.getSize());
    SegmentHolder segmentHolder = new SegmentHolder(segment, LOAD, Collections.singletonList(callback));
    segmentsToLoad.put(segment, segmentHolder);
    processingExecutor.submit(new SegmentChangeProcessor(segmentHolder));
  }

  @Override
  public void dropSegment(
      final DataSegment segment,
      final LoadPeonCallback callback
  )
  {
    final SegmentHolder existingHolder = segmentsToDrop.get(segment);
    if (existingHolder != null) {
      if (callback != null) {
        existingHolder.addCallback(callback);
      }
      return;
    }
    log.debug("Asking server peon[%s] to drop segment[%s]", basePath, segment.getId());
    SegmentHolder segmentHolder = new SegmentHolder(segment, DROP, Collections.singletonList(callback));
    segmentsToDrop.put(segment, segmentHolder);
    processingExecutor.submit(new SegmentChangeProcessor(segmentHolder));
  }

  @Override
  public void markSegmentToDrop(DataSegment dataSegment)
  {
    segmentsMarkedToDrop.add(dataSegment);
  }

  @Override
  public void unmarkSegmentToDrop(DataSegment dataSegment)
  {
    segmentsMarkedToDrop.remove(dataSegment);
  }

  private class SegmentChangeProcessor implements Runnable
  {
    private final SegmentHolder segmentHolder;

    private SegmentChangeProcessor(SegmentHolder segmentHolder)
    {
      this.segmentHolder = segmentHolder;
    }

    @Override
    public void run()
    {
      try {
        final String path = ZKPaths.makePath(basePath, segmentHolder.getSegmentIdentifier());
        final byte[] payload = jsonMapper.writeValueAsBytes(segmentHolder.getChangeRequest());
        curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
        log.debug(
            "ZKNode created for server to [%s] %s [%s]",
            basePath,
            segmentHolder.getType() == LOAD ? "load" : "drop",
            segmentHolder.getSegmentIdentifier()
        );
        final ScheduledFuture<?> future = monitorNodeRemovedExecutor.schedule(
            () -> {
              try {
                if (curator.checkExists().forPath(path) != null) {
                  failAssign(segmentHolder, new ISE("%s was never removed! Failing this operation!", path));
                } else {
                  log.debug("%s detected to be removed. ", path);
                }
              }
              catch (Exception e) {
                failAssign(segmentHolder, e);
              }
            },
            config.getLoadTimeoutDelay().getMillis(),
            TimeUnit.MILLISECONDS
        );

        final Stat stat = curator.checkExists().usingWatcher(
            (CuratorWatcher) watchedEvent -> {
              switch (watchedEvent.getType()) {
                case NodeDeleted:
                  // Cancel the check node deleted task since we have already
                  // been notified by the zk watcher
                  future.cancel(true);
                  entryRemoved(segmentHolder, watchedEvent.getPath());
                  break;
                default:
                  // do nothing
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
          entryRemoved(segmentHolder, path);
        }
      }
      catch (KeeperException.NodeExistsException ne) {
        // This is expected when historicals haven't yet picked up processing this segment and coordinator
        // tries reassigning it to the same node.
        log.warn(ne, "ZK node already exists because segment change request hasn't yet been processed");
        failAssign(segmentHolder);
      }
      catch (Exception e) {
        failAssign(segmentHolder, e);
      }
    }
  }

  private void actionCompleted(SegmentHolder segmentHolder)
  {
    switch (segmentHolder.getType()) {
      case LOAD:
        segmentsToLoad.remove(segmentHolder.getSegment());
        queuedSize.addAndGet(-segmentHolder.getSegmentSize());
        break;
      case DROP:
        segmentsToDrop.remove(segmentHolder.getSegment());
        break;
      default:
        throw new UnsupportedOperationException();
    }

    callBackExecutor.execute(
        () -> executeCallbacks(segmentHolder)
    );
  }


  @Override
  public void start()
  { }

  @Override
  public void stop()
  {
    for (SegmentHolder holder : segmentsToDrop.values()) {
      executeCallbacks(holder);
    }
    segmentsToDrop.clear();

    for (SegmentHolder holder : segmentsToLoad.values()) {
      executeCallbacks(holder);
    }
    segmentsToLoad.clear();

    queuedSize.set(0L);
    failedAssignCount.set(0);
    processingExecutor.shutdown();
    callBackExecutor.shutdown();
    monitorNodeRemovedExecutor.shutdown();
  }

  private void entryRemoved(SegmentHolder segmentHolder, String path)
  {
    if (!ZKPaths.getNodeFromPath(path).equals(segmentHolder.getSegmentIdentifier())) {
      log.warn(
          "Server[%s] entry [%s] was removed even though it's not what is currently loading[%s]",
          basePath, path, segmentHolder
      );
      return;
    }
    actionCompleted(segmentHolder);
    log.debug(
        "Server[%s] done processing %s of segment [%s]",
        basePath,
        segmentHolder.getType() == LOAD ? "load" : "drop",
        path
    );
  }

  private void failAssign(SegmentHolder segmentHolder)
  {
    failAssign(segmentHolder, null);
  }

  private void failAssign(SegmentHolder segmentHolder, Exception e)
  {
    if (e != null) {
      log.error(e, "Server[%s], throwable caught when submitting [%s].", basePath, segmentHolder);
    }
    failedAssignCount.getAndIncrement();
    // Act like it was completed so that the coordinator gives it to someone else
    actionCompleted(segmentHolder);
  }


  private static class SegmentHolder
  {
    private final DataSegment segment;
    private final DataSegmentChangeRequest changeRequest;
    private final int type;
    private final List<LoadPeonCallback> callbacks = new ArrayList<>();

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
      return segment.getId().toString();
    }

    public long getSegmentSize()
    {
      return segment.getSize();
    }

    public void addCallback(LoadPeonCallback newCallback)
    {
      synchronized (callbacks) {
        callbacks.add(newCallback);
      }
    }

    List<LoadPeonCallback> snapshotCallbacks()
    {
      synchronized (callbacks) {
        // Return a copy so that callers get a consistent view
        return ImmutableList.copyOf(callbacks);
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

  private void executeCallbacks(SegmentHolder holder)
  {
    for (LoadPeonCallback callback : holder.snapshotCallbacks()) {
      if (callback != null) {
        callBackExecutor.submit(() -> callback.execute());
      }
    }
  }
}
