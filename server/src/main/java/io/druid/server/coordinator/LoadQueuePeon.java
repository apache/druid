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

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestLoad;
import io.druid.timeline.DataSegment;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public abstract class LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(LoadQueuePeon.class);
  private static final int DROP = 0;
  private static final int LOAD = 1;

  private static void executeCallbacks(List<LoadPeonCallback> callbacks)
  {
    for (LoadPeonCallback callback : callbacks) {
      if (callback != null) {
        callback.execute();
      }
    }
  }


  private final ScheduledExecutorService processingExecutor;
  private final ExecutorService callBackExecutor;
  private final String peonId;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToLoad = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );
  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToDrop = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );

  private final Object lock = new Object();

  private volatile SegmentHolder currentlyProcessing = null;
  private boolean stopped = false;

  LoadQueuePeon(
      String peonId,
      ScheduledExecutorService processingExecutor,
      ExecutorService callbackExecutor
  )
  {
    this.peonId = peonId;
    this.callBackExecutor = callbackExecutor;
    this.processingExecutor = processingExecutor;
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

    log.info("Asking server peon[%s] to load segment[%s]", peonId, segment.getIdentifier());
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

    log.info("Asking server peon[%s] to drop segment[%s]", peonId, segment.getIdentifier());
    segmentsToDrop.put(segment, new SegmentHolder(segment, DROP, Arrays.asList(callback)));
    doNext();
  }

  private void doNext()
  {
    synchronized (lock) {
      if (currentlyProcessing == null) {
        if (!segmentsToDrop.isEmpty()) {
          currentlyProcessing = segmentsToDrop.firstEntry().getValue();
          log.info("Server[%s] dropping [%s]", peonId, currentlyProcessing.getSegmentIdentifier());
        } else if (!segmentsToLoad.isEmpty()) {
          currentlyProcessing = segmentsToLoad.firstEntry().getValue();
          log.info("Server[%s] loading [%s]", peonId, currentlyProcessing.getSegmentIdentifier());
        } else {
          return;
        }

        processingExecutor.execute(
            new Runnable()
            {
              @Override
              public void run()
              {
                synchronized (lock) {
                  // expected when the coordinator looses leadership and LoadQueuePeon is stopped.
                  if (currentlyProcessing == null) {
                    if (!stopped) {
                      log.makeAlert("Crazy race condition! server[%s]", peonId)
                         .emit();
                    }
                    doNext();
                    return;
                  }
                  log.info("Server[%s] processing segment[%s]", peonId, currentlyProcessing.getSegmentIdentifier());
                  processHolder(currentlyProcessing);
                }
              }
            }
        );
      } else {
        log.info(
            "Server[%s] skipping doNext() because something is currently loading[%s].",
            peonId,
            currentlyProcessing.getSegmentIdentifier()
        );
      }
    }
  }

  /**
   * Processes the segmentHolder asynchronously. Completion of the processing action is notified
   * via calling {@link #actionCompleted(SegmentHolder)} method. Any exception during processing
   * is notified via {@link #failAssign(SegmentHolder, Exception)} method.
   * NOTE: This method is always invoked using the processingExecutor.
   *
   * @param holder segment holder to be processed.
   */
  abstract void processHolder(SegmentHolder holder);

  void actionCompleted(SegmentHolder holder)
  {
    synchronized (lock) {
      if (currentlyProcessing == null) {
        log.warn("Server[%s] completed processing[%s] even though it wasn't processing!?", peonId, holder);
        return;
      }
      if (currentlyProcessing != holder) {
        log.warn(
            "Server[%s] completed processing[%s] while it was processing[%s]!?",
            peonId,
            holder,
            currentlyProcessing
        );
        return;
      }
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
      log.info("Server[%s] done processing [%s]", peonId, holder);
    }
    doNext();
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
      stopped = true;
    }
  }

  void failAssign(SegmentHolder holder, Exception e)
  {
    synchronized (lock) {
      log.error(e, "Server[%s], throwable caught when submitting [%s].", peonId, currentlyProcessing);
      failedAssignCount.getAndIncrement();
      // Act like it was completed so that the coordinator gives it to someone else
      actionCompleted(holder);
      doNext();
    }
  }

  static class SegmentHolder
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
