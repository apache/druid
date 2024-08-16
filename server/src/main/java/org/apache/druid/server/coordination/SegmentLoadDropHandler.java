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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.SegmentRowCountDistribution;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsible for loading and dropping of segments by a process that can serve segments.
 */
@ManageLifecycle
public class SegmentLoadDropHandler implements DataSegmentChangeHandler
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoadDropHandler.class);

  // Synchronizes removals from segmentsToDelete
  private final Object segmentDeleteLock = new Object();

  private final SegmentLoaderConfig config;
  private final DataSegmentAnnouncer announcer;
  private final SegmentManager segmentManager;
  private final ScheduledExecutorService exec;

  private final ConcurrentSkipListSet<DataSegment> segmentsToDelete;

  // Keep history of load/drop request status in a LRU cache to maintain idempotency if same request shows up
  // again and to return status of a completed request. Maximum size of this cache must be significantly greater
  // than number of pending load/drop requests. so that history is not lost too quickly.
  private final Cache<DataSegmentChangeRequest, AtomicReference<SegmentChangeStatus>> requestStatuses;
  private final Object requestStatusesLock = new Object();

  // This is the list of unresolved futures returned to callers of processBatch(List<DataSegmentChangeRequest>)
  // Threads loading/dropping segments resolve these futures as and when some segment load/drop finishes.
  private final LinkedHashSet<CustomSettableFuture> waitingFutures = new LinkedHashSet<>();

  @Inject
  public SegmentLoadDropHandler(
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      SegmentManager segmentManager
  )
  {
    this(
        config,
        announcer,
        segmentManager,
        Executors.newScheduledThreadPool(
            config.getNumLoadingThreads(),
            Execs.makeThreadFactory("SimpleDataSegmentChangeHandler-%s")
        )
    );
  }

  @VisibleForTesting
  SegmentLoadDropHandler(
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      SegmentManager segmentManager,
      ScheduledExecutorService exec
  )
  {
    this.config = config;
    this.announcer = announcer;
    this.segmentManager = segmentManager;
    this.exec = exec;

    this.segmentsToDelete = new ConcurrentSkipListSet<>();
    requestStatuses = CacheBuilder.newBuilder().maximumSize(config.getStatusQueueMaxSize()).initialCapacity(8).build();
  }

  public Map<String, Long> getAverageNumOfRowsPerSegmentForDatasource()
  {
    return segmentManager.getAverageRowCountForDatasource();
  }

  public Map<String, SegmentRowCountDistribution> getRowCountDistributionPerDatasource()
  {
    return segmentManager.getRowCountDistribution();
  }

  @Override
  public void addSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
  {
    SegmentChangeStatus result = null;
    try {
      log.info("Loading segment[%s]", segment.getId());
      /*
         The lock below is used to prevent a race condition when the scheduled runnable in removeSegment() starts,
         and if (segmentsToDelete.remove(segment)) returns true, in which case historical will start deleting segment
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
        synchronized (segmentDeleteLock) {
          segmentsToDelete.remove(segment);
        }
      }
      try {
        segmentManager.loadSegment(segment);
      }
      catch (Exception e) {
        removeSegment(segment, DataSegmentChangeCallback.NOOP, false);
        throw new SegmentLoadingException(e, "Exception loading segment[%s]", segment.getId());
      }
      try {
        // Announce segment even if the segment file already exists.
        announcer.announceSegment(segment);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Failed to announce segment[%s]", segment.getId());
      }

      result = SegmentChangeStatus.SUCCESS;
    }
    catch (Throwable e) {
      log.makeAlert(e, "Failed to load segment")
         .addData("segment", segment)
         .emit();
      result = SegmentChangeStatus.failed(e.toString());
    }
    finally {
      updateRequestStatus(new SegmentChangeRequestLoad(segment), result);
      if (null != callback) {
        callback.execute();
      }
    }
  }

  @Override
  public void removeSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
  {
    removeSegment(segment, callback, true);
  }

  @VisibleForTesting
  void removeSegment(
      final DataSegment segment,
      @Nullable final DataSegmentChangeCallback callback,
      final boolean scheduleDrop
  )
  {
    SegmentChangeStatus result = null;
    try {
      announcer.unannounceSegment(segment);
      segmentsToDelete.add(segment);

      Runnable runnable = () -> {
        try {
          synchronized (segmentDeleteLock) {
            if (segmentsToDelete.remove(segment)) {
              segmentManager.dropSegment(segment);
            }
          }
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to remove segment! Possible resource leak!")
             .addData("segment", segment)
             .emit();
        }
      };

      if (scheduleDrop) {
        log.info(
            "Completely removing segment[%s] in [%,d]ms.",
            segment.getId(), config.getDropSegmentDelayMillis()
        );
        exec.schedule(
            runnable,
            config.getDropSegmentDelayMillis(),
            TimeUnit.MILLISECONDS
        );
      } else {
        runnable.run();
      }

      result = SegmentChangeStatus.SUCCESS;
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to remove segment")
         .addData("segment", segment)
         .emit();
      result = SegmentChangeStatus.failed(e.getMessage());
    }
    finally {
      updateRequestStatus(new SegmentChangeRequestDrop(segment), result);
      if (null != callback) {
        callback.execute();
      }
    }
  }

  public Collection<DataSegment> getSegmentsToDelete()
  {
    return ImmutableList.copyOf(segmentsToDelete);
  }

  public ListenableFuture<List<DataSegmentChangeResponse>> processBatch(List<DataSegmentChangeRequest> changeRequests)
  {
    boolean isAnyRequestDone = false;

    Map<DataSegmentChangeRequest, AtomicReference<SegmentChangeStatus>> statuses = Maps.newHashMapWithExpectedSize(changeRequests.size());

    for (DataSegmentChangeRequest cr : changeRequests) {
      AtomicReference<SegmentChangeStatus> status = processRequest(cr);
      if (status.get().getState() != SegmentChangeStatus.State.PENDING) {
        isAnyRequestDone = true;
      }
      statuses.put(cr, status);
    }

    CustomSettableFuture future = new CustomSettableFuture(waitingFutures, statuses);

    if (isAnyRequestDone) {
      future.resolve();
    } else {
      synchronized (waitingFutures) {
        waitingFutures.add(future);
      }
    }

    return future;
  }

  private AtomicReference<SegmentChangeStatus> processRequest(DataSegmentChangeRequest changeRequest)
  {
    synchronized (requestStatusesLock) {
      AtomicReference<SegmentChangeStatus> status = requestStatuses.getIfPresent(changeRequest);

      // If last load/drop request status is failed, here can try that again
      if (status == null || status.get().getState() == SegmentChangeStatus.State.FAILED) {
        changeRequest.go(
            new DataSegmentChangeHandler()
            {
              @Override
              public void addSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
              {
                requestStatuses.put(changeRequest, new AtomicReference<>(SegmentChangeStatus.PENDING));
                exec.submit(
                    () -> SegmentLoadDropHandler.this.addSegment(
                        ((SegmentChangeRequestLoad) changeRequest).getSegment(),
                        () -> resolveWaitingFutures()
                    )
                );
              }

              @Override
              public void removeSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
              {
                requestStatuses.put(changeRequest, new AtomicReference<>(SegmentChangeStatus.PENDING));
                SegmentLoadDropHandler.this.removeSegment(
                    ((SegmentChangeRequestDrop) changeRequest).getSegment(),
                    () -> resolveWaitingFutures(),
                    true
                );
              }
            },
            this::resolveWaitingFutures
        );
      } else if (status.get().getState() == SegmentChangeStatus.State.SUCCESS) {
        // SUCCESS case, we'll clear up the cached success while serving it to this client
        // Not doing this can lead to an incorrect response to upcoming clients for a reload
        requestStatuses.invalidate(changeRequest);
        return status;
      }
      return requestStatuses.getIfPresent(changeRequest);
    }
  }

  private void updateRequestStatus(DataSegmentChangeRequest changeRequest, @Nullable SegmentChangeStatus result)
  {
    if (result == null) {
      result = SegmentChangeStatus.failed("Unknown reason. Check server logs.");
    }
    synchronized (requestStatusesLock) {
      AtomicReference<SegmentChangeStatus> statusRef = requestStatuses.getIfPresent(changeRequest);
      if (statusRef != null) {
        statusRef.set(result);
      }
    }
  }

  private void resolveWaitingFutures()
  {
    LinkedHashSet<CustomSettableFuture> waitingFuturesCopy;
    synchronized (waitingFutures) {
      waitingFuturesCopy = new LinkedHashSet<>(waitingFutures);
      waitingFutures.clear();
    }
    for (CustomSettableFuture future : waitingFuturesCopy) {
      future.resolve();
    }
  }

  // Future with cancel() implementation to remove it from "waitingFutures" list
  private class CustomSettableFuture extends AbstractFuture<List<DataSegmentChangeResponse>>
  {
    private final LinkedHashSet<CustomSettableFuture> waitingFutures;
    private final Map<DataSegmentChangeRequest, AtomicReference<SegmentChangeStatus>> statusRefs;

    private CustomSettableFuture(
        LinkedHashSet<CustomSettableFuture> waitingFutures,
        Map<DataSegmentChangeRequest, AtomicReference<SegmentChangeStatus>> statusRefs
    )
    {
      this.waitingFutures = waitingFutures;
      this.statusRefs = statusRefs;
    }

    public void resolve()
    {
      synchronized (requestStatusesLock) {
        if (isDone()) {
          return;
        }

        final List<DataSegmentChangeResponse> result = new ArrayList<>(statusRefs.size());
        statusRefs.forEach((request, statusRef) -> {
          // Remove complete statuses from the cache
          final SegmentChangeStatus status = statusRef.get();
          if (status != null && status.getState() != SegmentChangeStatus.State.PENDING) {
            requestStatuses.invalidate(request);
          }
          result.add(new DataSegmentChangeResponse(request, status));
        });

        set(result);
      }
    }

    @Override
    public boolean cancel(boolean interruptIfRunning)
    {
      synchronized (waitingFutures) {
        waitingFutures.remove(this);
      }
      return true;
    }
  }
}

