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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.SegmentChangeRequestNoop;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Use {@link HttpLoadQueuePeon} instead.
 */
@Deprecated
public class CuratorLoadQueuePeonV2 extends LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(CuratorLoadQueuePeon.class);
  private static final int DROP = 0;
  private static final int LOAD = 1;
  private final CuratorFramework curator;
  private final String basePath;
  private final ObjectMapper jsonMapper;
  private final ExecutorService processingExecutor;
  private final ScheduledExecutorService checkNodeRemovedExecutor;
  private final ExecutorService callBackExecutor;
  private final DruidCoordinatorConfig config;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToLoad = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );
  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToDrop = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );
  private final ConcurrentSkipListSet<DataSegment> segmentsMarkedToDrop = new ConcurrentSkipListSet<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );

  private static final int MAX_TASK_WAIT = 100;

  CuratorLoadQueuePeonV2(
      CuratorFramework curator,
      String basePath,
      ObjectMapper jsonMapper,
      ExecutorService processingExecutor,
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
    //TODO: samarth configure the size of this pool
    this.checkNodeRemovedExecutor = Executors.newScheduledThreadPool(5, new ThreadFactoryBuilder().setNameFormat(
        "ZKNodeDeletionChecker--%d").build());
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
  public void loadSegment(
      final DataSegment segment,
      final LoadPeonCallback callback
  )
  {

    final SegmentHolder existingHolder = segmentsToLoad.get(segment);
    if (existingHolder != null) {
      if ((callback != null)) {
        existingHolder.addCallback(callback);
      }
      return;
    }
    log.info("Asking server peon[%s] to load segment[%s]", basePath, segment.getId());
    queuedSize.addAndGet(segment.getSize());
    SegmentHolder holder = new SegmentHolder(segment, LOAD, Collections.singletonList(callback));
    segmentsToLoad.put(segment, holder);
    processingExecutor.submit((Callable<Void>) () -> {
      processSegmentChangeRequest(holder);
      return null;
    });
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
    log.info("Asking server peon[%s] to drop segment[%s]", basePath, segment.getId());
    SegmentHolder segmentHolder = new SegmentHolder(segment, DROP, Collections.singletonList(callback));
    segmentsToDrop.put(segment, segmentHolder);
    processingExecutor.submit((Callable<Void>) () -> {
      processSegmentChangeRequest(segmentHolder);
      return null;
    });
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

  private void processSegmentChangeRequest(SegmentHolder segmentHolder)
  {
    try {
      // Wait for a random duration since we do not want zookeeper getting pounded with create node requests
      TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(MAX_TASK_WAIT));
      log.info("Server[%s] %s [%s]", basePath, segmentHolder.getType() == LOAD ?
                                               "loading" : "dropping", segmentHolder.getSegmentId().toString());
      final String path = ZKPaths.makePath(basePath, segmentHolder.getSegmentId().toString());
      final byte[] payload = jsonMapper.writeValueAsBytes(segmentHolder.getChangeRequest());
      curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);

      checkNodeRemovedExecutor.schedule(
          () -> {
            try {
              if (curator.checkExists().forPath(path) != null) {
                failAssign(segmentHolder, new ISE("%s was never removed! Failing this operation!", path));
              } else {
                log.info("%s detected to be removed. ", path);
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
    catch (Exception e) {
      failAssign(segmentHolder, e);
    }
  }

  private void actionCompleted(SegmentHolder segmentHolder)
  {
    log.info("Action completed: " + segmentHolder.getType());
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

    final List<LoadPeonCallback> callbacks = segmentHolder.getCallbacks();
    callBackExecutor.execute(
        () -> executeCallbacks(callbacks)
    );
  }


  @Override
  public void start()
  {}

  @Override
  public void stop()
  {
    for (SegmentHolder holder : segmentsToDrop.values()) {
      executeCallbacks(holder.getCallbacks());
    }
    segmentsToDrop.clear();

    for (SegmentHolder holder : segmentsToLoad.values()) {
      executeCallbacks(holder.getCallbacks());
    }
    segmentsToLoad.clear();

    queuedSize.set(0L);
    failedAssignCount.set(0);
    processingExecutor.shutdown();
    callBackExecutor.shutdown();
    checkNodeRemovedExecutor.shutdown();
  }

  private void entryRemoved(SegmentHolder segmentHolder, String path)
  {
    if (!ZKPaths.getNodeFromPath(path).equals(segmentHolder.getSegmentId().toString())) {
      log.warn(
          "Server[%s] entry [%s] was removed even though it's not what is currently loading[%s]",
          basePath, path, segmentHolder
      );
      return;
    }
    actionCompleted(segmentHolder);
    log.info("Server[%s] done processing [%s]", basePath, path);
  }

  private void failAssign(SegmentHolder segmentHolder, Exception e)
  {
    log.error(e, "Server[%s], throwable caught when submitting [%s].", basePath, segmentHolder);
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

    private SegmentHolder(DataSegment segment, int type, Collection<LoadPeonCallback> callbacks)
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

    public SegmentId getSegmentId()
    {
      return segment.getId();
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

  private void executeCallbacks(List<LoadPeonCallback> callbacks)
  {
    // Create a copy of the passed list since it can be concurrently modified by another thread when
    // segmentHolder.addCallBack() is called
    for (LoadPeonCallback callback : ImmutableList.copyOf(callbacks)) {
      if (callback != null) {
        callBackExecutor.submit(() -> callback.execute());
      }
    }
  }
}
