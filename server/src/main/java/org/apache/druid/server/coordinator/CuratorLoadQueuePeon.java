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
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.SegmentChangeRequestNoop;
import org.apache.druid.timeline.DataSegment;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Use {@link HttpLoadQueuePeon} instead.
 * <p>
 * Objects of this class can be accessed by multiple threads. State wise, this class
 * is thread safe and callers of the public methods can expect thread safe behavior.
 * Though, like a typical object being accessed by multiple threads,
 * callers shouldn't expect strict consistency in results between two calls
 * of the same or different methods.
 */
@Deprecated
public class CuratorLoadQueuePeon extends LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(CuratorLoadQueuePeon.class);

  private final CuratorFramework curator;
  private final String basePath;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService processingExecutor;

  /**
   * Threadpool with daemon threads that execute callback actions associated
   * with loading or dropping segments.
   */
  private final ExecutorService callBackExecutor;
  private final DruidCoordinatorConfig config;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  /**
   * Needs to be thread safe since it can be concurrently accessed via
   * {@link #loadSegment(DataSegment, LoadPeonCallback)}, {@link #actionCompleted(SegmentHolder)},
   * {@link #getSegmentsToLoad()} and {@link #stop()}
   */
  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToLoad = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );

  /**
   * Needs to be thread safe since it can be concurrently accessed via
   * {@link #dropSegment(DataSegment, LoadPeonCallback)}, {@link #actionCompleted(SegmentHolder)},
   * {@link #getSegmentsToDrop()} and {@link #stop()}
   */
  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToDrop = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );

  /**
   * Needs to be thread safe since it can be concurrently accessed via
   * {@link #markSegmentToDrop(DataSegment)}}, {@link #unmarkSegmentToDrop(DataSegment)}}
   * and {@link #getSegmentsToDrop()}
   */
  private final ConcurrentSkipListSet<DataSegment> segmentsMarkedToDrop = new ConcurrentSkipListSet<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );

  /**
   * Needs to be thread safe since it can be concurrently accessed via
   * {@link #failAssign(SegmentHolder, boolean, Exception)}, {@link #actionCompleted(SegmentHolder)},
   * {@link #getTimedOutSegments()} and {@link #stop()}
   */
  private final ConcurrentSkipListSet<DataSegment> timedOutSegments = new ConcurrentSkipListSet<>(
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
  public Set<DataSegment> getTimedOutSegments()
  {
    return timedOutSegments;
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
  public void loadSegment(final DataSegment segment, @Nullable final LoadPeonCallback callback)
  {
    SegmentHolder segmentHolder = new SegmentHolder(segment, Action.LOAD, Collections.singletonList(callback));
    final SegmentHolder existingHolder = segmentsToLoad.putIfAbsent(segment, segmentHolder);
    if (existingHolder != null) {
      existingHolder.addCallback(callback);
      return;
    }
    log.debug("Asking server peon[%s] to load segment[%s]", basePath, segment.getId());
    queuedSize.addAndGet(segment.getSize());
    processingExecutor.submit(new SegmentChangeProcessor(segmentHolder));
  }

  @Override
  public void dropSegment(final DataSegment segment, @Nullable final LoadPeonCallback callback)
  {
    SegmentHolder segmentHolder = new SegmentHolder(segment, Action.DROP, Collections.singletonList(callback));
    final SegmentHolder existingHolder = segmentsToDrop.putIfAbsent(segment, segmentHolder);
    if (existingHolder != null) {
      existingHolder.addCallback(callback);
      return;
    }
    log.debug("Asking server peon[%s] to drop segment[%s]", basePath, segment.getId());
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
            segmentHolder.getAction(),
            segmentHolder.getSegmentIdentifier()
        );
        final ScheduledFuture<?> nodeDeletedCheck = scheduleNodeDeletedCheck(path);
        final Stat stat = curator.checkExists().usingWatcher(
            (CuratorWatcher) watchedEvent -> {
              switch (watchedEvent.getType()) {
                case NodeDeleted:
                  // Cancel the check node deleted task since we have already
                  // been notified by the zk watcher
                  nodeDeletedCheck.cancel(true);
                  onZkNodeDeleted(segmentHolder, watchedEvent.getPath());
                  break;
                default:
                  // do nothing
              }
            }
        ).forPath(path);

        // Cleanup watcher to avoid memory leak if we missed the NodeDeleted event
        if (stat == null) {
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
          final byte[] noopPayload = jsonMapper.writeValueAsBytes(new SegmentChangeRequestNoop());
          curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, noopPayload);
          onZkNodeDeleted(segmentHolder, path);
        }
      }
      catch (KeeperException.NodeExistsException ne) {
        // This is expected when historicals haven't yet picked up processing this segment and coordinator
        // tries reassigning it to the same node.
        log.warn(ne, "ZK node already exists because segment change request hasn't yet been processed");
        failAssign(segmentHolder, true, null);
      }
      catch (Exception e) {
        failAssign(segmentHolder, false, e);
      }
    }

    @Nonnull
    private ScheduledFuture<?> scheduleNodeDeletedCheck(String path)
    {
      return processingExecutor.schedule(
          () -> {
            try {
              if (curator.checkExists().forPath(path) != null) {
                failAssign(
                    segmentHolder,
                    true,
                    new ISE(
                        "%s operation timed out and [%s] was never removed! "
                        + "These segments may still get processed.",
                        segmentHolder.getAction(),
                        path
                    )
                );
              } else {
                log.debug("Path [%s] has been removed.", path);
              }
            }
            catch (Exception e) {
              log.error(e, "Exception caught and ignored when checking whether zk node was deleted");
              failAssign(segmentHolder, false, e);
            }
          },
          config.getLoadTimeoutDelay().getMillis(),
          TimeUnit.MILLISECONDS
      );
    }
  }

  private void actionCompleted(SegmentHolder segmentHolder)
  {
    switch (segmentHolder.getAction()) {
      case LOAD:
        // When load failed a segment will be removed from the segmentsToLoad twice and
        // null value will be returned at the second time in which case queueSize may be negative.
        // See https://github.com/apache/druid/pull/10362 for more details.
        if (null != segmentsToLoad.remove(segmentHolder.getSegment())) {
          queuedSize.addAndGet(-segmentHolder.getSegmentSize());
          timedOutSegments.remove(segmentHolder.getSegment());
        }
        break;
      case DROP:
        segmentsToDrop.remove(segmentHolder.getSegment());
        timedOutSegments.remove(segmentHolder.getSegment());
        break;
      default:
        throw new UnsupportedOperationException();
    }
    executeCallbacks(segmentHolder, true);
  }


  @Override
  public void start()
  {
  }

  @Override
  public void stop()
  {
    for (SegmentHolder holder : segmentsToDrop.values()) {
      executeCallbacks(holder, false);
    }
    segmentsToDrop.clear();

    for (SegmentHolder holder : segmentsToLoad.values()) {
      executeCallbacks(holder, false);
    }
    segmentsToLoad.clear();

    timedOutSegments.clear();
    queuedSize.set(0L);
    failedAssignCount.set(0);
  }

  private void onZkNodeDeleted(SegmentHolder segmentHolder, String path)
  {
    if (!ZKPaths.getNodeFromPath(path).equals(segmentHolder.getSegmentIdentifier())) {
      log.warn(
          "Server[%s] entry [%s] was removed even though it's not what is currently loading[%s]",
          basePath,
          path,
          segmentHolder
      );
      return;
    }
    actionCompleted(segmentHolder);
    log.debug(
        "Server[%s] done processing %s of segment [%s]",
        basePath,
        segmentHolder.getAction(),
        path
    );
  }

  private void failAssign(SegmentHolder segmentHolder, boolean handleTimeout, Exception e)
  {
    if (e != null) {
      log.error(e, "Server[%s], throwable caught when submitting [%s].", basePath, segmentHolder);
    }
    failedAssignCount.getAndIncrement();

    if (handleTimeout) {
      // Avoid removing the segment entry from the load/drop list in case config.getLoadTimeoutDelay() expires.
      // This is because the ZK Node is still present and it may be processed after this timeout and so the coordinator
      // needs to take this into account.
      log.debug(
          "Skipping segment removal from [%s] queue, since ZK Node still exists!",
          segmentHolder.getAction()
      );
      timedOutSegments.add(segmentHolder.getSegment());
      executeCallbacks(segmentHolder, false);
    } else {
      // This may have failed for a different reason and so act like it was completed.
      actionCompleted(segmentHolder);
    }
  }

  private enum Action
  {
    LOAD, DROP
  }

  private static class SegmentHolder
  {
    private final DataSegment segment;
    private final DataSegmentChangeRequest changeRequest;
    private final Action type;
    // Guaranteed to store only non-null elements
    private final List<LoadPeonCallback> callbacks = new ArrayList<>();

    private SegmentHolder(
        DataSegment segment,
        Action type,
        Collection<LoadPeonCallback> callbacksParam
    )
    {
      this.segment = segment;
      this.type = type;
      this.changeRequest = (type == Action.LOAD)
                           ? new SegmentChangeRequestLoad(segment)
                           : new SegmentChangeRequestDrop(segment);
      Iterator<LoadPeonCallback> itr = callbacksParam.iterator();
      while (itr.hasNext()) {
        LoadPeonCallback c = itr.next();
        if (c != null) {
          callbacks.add(c);
        }
      }
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public Action getAction()
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

    public void addCallback(@Nullable LoadPeonCallback newCallback)
    {
      if (newCallback != null) {
        synchronized (callbacks) {
          callbacks.add(newCallback);
        }
      }
    }

    List<LoadPeonCallback> snapshotCallbacks()
    {
      synchronized (callbacks) {
        // Return an immutable copy so that callers don't have to worry about concurrent modification
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

  private void executeCallbacks(SegmentHolder holder, boolean success)
  {
    for (LoadPeonCallback callback : holder.snapshotCallbacks()) {
      callBackExecutor.submit(() -> callback.execute(success));
    }
  }
}
