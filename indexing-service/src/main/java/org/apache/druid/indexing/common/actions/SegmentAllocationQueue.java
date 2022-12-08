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

package org.apache.druid.indexing.common.actions;

import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Queue for {@link SegmentAllocateRequest}s.
 */
@ManageLifecycle
public class SegmentAllocationQueue
{
  private static final Logger log = new Logger(SegmentAllocationQueue.class);

  private static final int MAX_QUEUE_SIZE = 2000;
  private static final int MAX_BATCH_SIZE = 500;

  private final long maxWaitTimeMillis;

  private final TaskLockbox taskLockbox;
  private final ScheduledExecutorService executor;
  private final IndexerMetadataStorageCoordinator metadataStorage;
  private final AtomicBoolean isLeader = new AtomicBoolean(false);
  private final ServiceEmitter emitter;

  private final ConcurrentHashMap<AllocateRequestKey, AllocateRequestBatch> keyToBatch = new ConcurrentHashMap<>();
  private final BlockingDeque<AllocateRequestKey> processingQueue = new LinkedBlockingDeque<>(MAX_QUEUE_SIZE);

  @Inject
  public SegmentAllocationQueue(
      TaskLockbox taskLockbox,
      TaskLockConfig taskLockConfig,
      IndexerMetadataStorageCoordinator metadataStorage,
      ServiceEmitter emitter,
      ScheduledExecutorFactory executorFactory
  )
  {
    this.emitter = emitter;
    this.taskLockbox = taskLockbox;
    this.metadataStorage = metadataStorage;
    this.maxWaitTimeMillis = taskLockConfig.getBatchAllocationWaitTime();

    this.executor = taskLockConfig.isBatchSegmentAllocation()
                    ? executorFactory.create(1, "SegmentAllocQueue-%s") : null;
  }

  @LifecycleStart
  public void start()
  {
    if (isEnabled()) {
      log.info("Initializing segment allocation queue.");
      scheduleQueuePoll(maxWaitTimeMillis);
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (isEnabled()) {
      log.info("Tearing down segment allocation queue.");
      executor.shutdownNow();
    }
  }

  public void becomeLeader()
  {
    if (!isLeader.compareAndSet(false, true)) {
      log.info("Already the leader. Queue processing has started.");
    } else if (isEnabled()) {
      log.info("Elected leader. Starting queue processing.");
    } else {
      log.info(
          "Elected leader but batched segment allocation is disabled. "
          + "Segment allocation queue will not be used."
      );
    }
  }

  public void stopBeingLeader()
  {
    if (!isLeader.compareAndSet(true, false)) {
      log.info("Already surrendered leadership. Queue processing is stopped.");
    } else if (isEnabled()) {
      log.info("Not leader anymore. Stopping queue processing.");
    } else {
      log.info("Not leader anymore. Segment allocation queue is already disabled.");
    }
  }

  public boolean isEnabled()
  {
    return executor != null && !executor.isShutdown();
  }

  private void scheduleQueuePoll(long delay)
  {
    executor.schedule(this::processBatchesDue, delay, TimeUnit.MILLISECONDS);
  }

  /**
   * Gets the number of batches currently in the queue.
   */
  public int size()
  {
    return processingQueue.size();
  }

  /**
   * Queues a SegmentAllocateRequest. The returned future may complete successfully
   * with a non-null value or with a non-null value.
   */
  public Future<SegmentIdWithShardSpec> add(SegmentAllocateRequest request)
  {
    if (!isLeader.get()) {
      throw new ISE("Cannot allocate segment if not leader.");
    } else if (!isEnabled()) {
      throw new ISE("Batched segment allocation is disabled.");
    }

    final AllocateRequestKey requestKey = getKeyForAvailableBatch(request);
    final AtomicReference<Future<SegmentIdWithShardSpec>> futureReference = new AtomicReference<>();

    // Possible race condition:
    // t1 -> new batch is added to queue or batch already exists in queue
    // t2 -> executor pops batch, processes all requests in it
    // t1 -> new request is added to dangling batch and is never picked up
    // Solution: For existing batch, call keyToBatch.remove() on the key to
    // wait on keyToBatch.compute() to finish before proceeding with processBatch().
    // For new batch, keyToBatch.remove() would not wait as key is not in map yet
    // but a new batch is unlikely to be due immediately, so it won't get popped right away.
    keyToBatch.compute(requestKey, (key, existingBatch) -> {
      if (existingBatch == null) {
        AllocateRequestBatch newBatch = new AllocateRequestBatch(key);
        futureReference.set(newBatch.add(request));
        return addBatchToQueue(newBatch) ? newBatch : null;
      } else {
        futureReference.set(existingBatch.add(request));
        return existingBatch;
      }
    });

    return futureReference.get();
  }

  /**
   * Returns the key for a batch that is not added to the queue yet and/or has
   * available space. Throws an exception if the queue is already full and no
   * batch has available capacity.
   */
  private AllocateRequestKey getKeyForAvailableBatch(SegmentAllocateRequest request)
  {
    for (int batchIncrementalId = 0; batchIncrementalId < MAX_QUEUE_SIZE; ++batchIncrementalId) {
      AllocateRequestKey nextKey = new AllocateRequestKey(request, maxWaitTimeMillis, batchIncrementalId);
      AllocateRequestBatch nextBatch = keyToBatch.get(nextKey);
      if (nextBatch == null || nextBatch.size() < MAX_BATCH_SIZE) {
        return nextKey;
      }
    }

    throw new ISE("Allocation queue is at capacity, all batches are full.");
  }

  /**
   * Tries to add the given batch to the processing queue. Fails all the pending
   * requests in the batch if we are not leader or if the queue is full.
   */
  private boolean addBatchToQueue(AllocateRequestBatch batch)
  {
    batch.key.resetQueueTime();
    if (!isLeader.get()) {
      batch.failPendingRequests("Cannot allocate segment if not leader");
      return false;
    } else if (processingQueue.offer(batch.key)) {
      log.debug("Added a new batch [%s] to queue.", batch.key);
      return true;
    } else {
      batch.failPendingRequests(
          "Segment allocation queue is full. Check the metric `task/action/batch/runTime` "
          + "to determine if metadata operations are slow."
      );
      return false;
    }
  }

  /**
   * Tries to add the given batch to the processing queue. If a batch already
   * exists for this key, transfers all the requests from this batch to the
   * existing one.
   */
  private void requeueBatch(AllocateRequestBatch batch)
  {
    log.info("Requeueing [%d] failed requests in batch [%s].", batch.size(), batch.key);
    keyToBatch.compute(batch.key, (key, existingBatch) -> {
      if (existingBatch == null) {
        return addBatchToQueue(batch) ? batch : null;
      } else {
        // Merge requests from this batch to existing one
        existingBatch.transferRequestsFrom(batch);
        return existingBatch;
      }
    });
  }

  private void processBatchesDue()
  {
    clearQueueIfNotLeader();

    // Process all batches which are due
    log.debug("Processing batches which are due. Queue size [%d].", processingQueue.size());
    int numProcessedBatches = 0;

    AllocateRequestKey nextKey = processingQueue.peekFirst();
    while (nextKey != null && nextKey.isDue()) {
      processingQueue.pollFirst();
      AllocateRequestBatch nextBatch = keyToBatch.remove(nextKey);

      boolean processed;
      try {
        processed = processBatch(nextBatch);
      }
      catch (Throwable t) {
        nextBatch.failPendingRequests(t);
        processed = true;
        log.error(t, "Error while processing batch [%s]", nextKey);
      }

      // Requeue if not fully processed yet
      if (processed) {
        ++numProcessedBatches;
      } else {
        requeueBatch(nextBatch);
      }

      nextKey = processingQueue.peek();
    }

    // Schedule the next round of processing
    final long nextScheduleDelay;
    if (processingQueue.isEmpty()) {
      nextScheduleDelay = maxWaitTimeMillis;
    } else {
      nextKey = processingQueue.peek();
      long timeElapsed = System.currentTimeMillis() - nextKey.getQueueTime();
      nextScheduleDelay = Math.max(0, maxWaitTimeMillis - timeElapsed);
    }
    scheduleQueuePoll(nextScheduleDelay);
    log.info("Processed [%d] batches, next execution in [%d ms]", numProcessedBatches, nextScheduleDelay);
  }

  /**
   * Removes items from the queue as long as we are not leader.
   */
  private void clearQueueIfNotLeader()
  {
    int failedBatches = 0;
    AllocateRequestKey nextKey = processingQueue.peekFirst();
    while (nextKey != null && !isLeader.get()) {
      processingQueue.pollFirst();
      AllocateRequestBatch nextBatch = keyToBatch.remove(nextKey);
      nextBatch.failPendingRequests("Cannot allocate segment if not leader");
      ++failedBatches;

      nextKey = processingQueue.peekFirst();
    }
    if (failedBatches > 0) {
      log.info("Not leader. Failed [%d] batches, remaining in queue [%d].", failedBatches, processingQueue.size());
    }
  }

  /**
   * Processes the given batch. Returns true if the batch was completely processed
   * and should not be requeued.
   */
  private boolean processBatch(AllocateRequestBatch requestBatch)
  {
    final AllocateRequestKey requestKey = requestBatch.key;
    if (requestBatch.isEmpty()) {
      return true;
    } else if (!isLeader.get()) {
      requestBatch.failPendingRequests("Cannot allocate segment if not leader");
      return true;
    }

    log.debug(
        "Processing [%d] requests for batch [%s], queue time [%s].",
        requestBatch.size(),
        requestKey,
        requestKey.getQueueTime()
    );

    final long startTimeMillis = System.currentTimeMillis();
    final int batchSize = requestBatch.size();
    emitBatchMetric("task/action/batch/size", batchSize, requestKey);
    emitBatchMetric("task/action/batch/queueTime", (startTimeMillis - requestKey.getQueueTime()), requestKey);

    final Set<DataSegment> usedSegments = retrieveUsedSegments(requestKey);
    final int successCount = allocateSegmentsForBatch(requestBatch, usedSegments);

    emitBatchMetric("task/action/batch/attempts", 1L, requestKey);
    emitBatchMetric("task/action/batch/runTime", (System.currentTimeMillis() - startTimeMillis), requestKey);
    log.info("Successfully processed [%d / %d] requests in batch [%s].", successCount, batchSize, requestKey);

    if (requestBatch.isEmpty()) {
      log.debug("All requests in batch [%s] have been processed.", requestKey);
      return true;
    }

    // Requeue the batch only if used segments have changed
    log.debug("There are [%d] failed requests in batch [%s].", requestBatch.size(), requestKey);
    final Set<DataSegment> updatedUsedSegments = retrieveUsedSegments(requestKey);

    if (updatedUsedSegments.equals(usedSegments)) {
      requestBatch.failPendingRequests("Allocation failed probably due to conflicting segments.");
      return true;
    } else {
      log.debug("Used segments have changed. Requeuing failed requests.");
      return false;
    }
  }

  private Set<DataSegment> retrieveUsedSegments(AllocateRequestKey key)
  {
    return new HashSet<>(
        metadataStorage.retrieveUsedSegmentsForInterval(
            key.dataSource,
            key.preferredAllocationInterval,
            Segments.ONLY_VISIBLE
        )
    );
  }

  private int allocateSegmentsForBatch(AllocateRequestBatch requestBatch, Set<DataSegment> usedSegments)
  {
    int successCount = 0;

    // Find requests whose row interval overlaps with an existing used segment
    final Set<SegmentAllocateRequest> allRequests = requestBatch.getRequests();
    final Set<SegmentAllocateRequest> requestsWithNoOverlappingSegment = new HashSet<>();

    if (usedSegments.isEmpty()) {
      requestsWithNoOverlappingSegment.addAll(allRequests);
    } else {
      final Interval[] sortedUsedSegmentIntervals = getSortedIntervals(usedSegments);
      final Map<Interval, List<SegmentAllocateRequest>> overlapIntervalToRequests = new HashMap<>();

      for (SegmentAllocateRequest request : allRequests) {
        // If there is an overlapping used segment, the interval of the used segment
        // is the only candidate for allocation for this request
        final Interval overlappingInterval = Intervals.findOverlappingInterval(
            request.getRowInterval(),
            sortedUsedSegmentIntervals
        );

        if (overlappingInterval == null) {
          requestsWithNoOverlappingSegment.add(request);
        } else if (overlappingInterval.contains(request.getRowInterval())) {
          // Found an enclosing interval, use this for allocation
          overlapIntervalToRequests.computeIfAbsent(overlappingInterval, i -> new ArrayList<>())
                                   .add(request);
        } else {
          // There is no valid allocation interval for this request due to a
          // partially overlapping used segment. Need not do anything right now.
          // The request will be retried upon requeueing the batch.
        }
      }

      // Try to allocate segments for the identified used segment intervals.
      // Do not retry the failed requests with other intervals unless the batch is requeued.
      for (Map.Entry<Interval, List<SegmentAllocateRequest>> entry : overlapIntervalToRequests.entrySet()) {
        successCount += allocateSegmentsForInterval(
            entry.getKey(),
            entry.getValue(),
            requestBatch
        );
      }
    }

    // For requests that do not overlap with a used segment, first try to allocate
    // using the preferred granularity, then smaller granularities
    final Set<SegmentAllocateRequest> pendingRequests = new HashSet<>(requestsWithNoOverlappingSegment);
    for (Granularity granularity :
        Granularity.granularitiesFinerThan(requestBatch.key.preferredSegmentGranularity)) {
      Map<Interval, List<SegmentAllocateRequest>> requestsByInterval =
          getRequestsByInterval(pendingRequests, granularity);

      for (Map.Entry<Interval, List<SegmentAllocateRequest>> entry : requestsByInterval.entrySet()) {
        successCount += allocateSegmentsForInterval(
            entry.getKey(),
            entry.getValue(),
            requestBatch
        );
        pendingRequests.retainAll(requestBatch.getRequests());
      }
    }

    return successCount;
  }

  private Interval[] getSortedIntervals(Set<DataSegment> usedSegments)
  {
    TreeSet<Interval> sortedSet = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    usedSegments.forEach(segment -> sortedSet.add(segment.getInterval()));
    return sortedSet.toArray(new Interval[0]);
  }

  /**
   * Tries to allocate segments for the given requests over the specified interval.
   * Returns the number of requests for which segments were successfully allocated.
   */
  private int allocateSegmentsForInterval(
      Interval tryInterval,
      List<SegmentAllocateRequest> requests,
      AllocateRequestBatch requestBatch
  )
  {
    if (requests.isEmpty()) {
      return 0;
    }

    final AllocateRequestKey requestKey = requestBatch.key;
    log.debug(
        "Trying allocation for [%d] requests, interval [%s] in batch [%s]",
        requests.size(),
        tryInterval,
        requestKey
    );

    final List<SegmentAllocateResult> results = taskLockbox.allocateSegments(
        requests,
        requestKey.dataSource,
        tryInterval,
        requestKey.skipSegmentLineageCheck,
        requestKey.lockGranularity
    );

    int successfulRequests = 0;
    for (int i = 0; i < requests.size(); ++i) {
      SegmentAllocateRequest request = requests.get(i);
      SegmentAllocateResult result = results.get(i);
      if (result.isSuccess()) {
        ++successfulRequests;
      }

      requestBatch.handleResult(result, request);
    }

    return successfulRequests;
  }

  private Map<Interval, List<SegmentAllocateRequest>> getRequestsByInterval(
      Set<SegmentAllocateRequest> requests,
      Granularity tryGranularity
  )
  {
    final Map<Interval, List<SegmentAllocateRequest>> tryIntervalToRequests = new HashMap<>();
    for (SegmentAllocateRequest request : requests) {
      Interval tryInterval = tryGranularity.bucket(request.getAction().getTimestamp());
      if (tryInterval.contains(request.getRowInterval())) {
        tryIntervalToRequests.computeIfAbsent(tryInterval, i -> new ArrayList<>()).add(request);
      }
    }
    return tryIntervalToRequests;
  }

  private void emitTaskMetric(String metric, long value, SegmentAllocateRequest request)
  {
    final ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, request.getTask());
    metricBuilder.setDimension("taskActionType", SegmentAllocateAction.TYPE);
    emitter.emit(metricBuilder.build(metric, value));
  }

  private void emitBatchMetric(String metric, long value, AllocateRequestKey key)
  {
    final ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
    metricBuilder.setDimension("taskActionType", SegmentAllocateAction.TYPE);
    metricBuilder.setDimension(DruidMetrics.DATASOURCE, key.dataSource);
    metricBuilder.setDimension(DruidMetrics.INTERVAL, key.preferredAllocationInterval.toString());
    emitter.emit(metricBuilder.build(metric, value));
  }

  /**
   * A batch of segment allocation requests.
   */
  private class AllocateRequestBatch
  {
    private final AllocateRequestKey key;

    /**
     * Map from allocate requests (represents a single SegmentAllocateAction)
     * to the future of allocated segment id.
     * <p>
     * This must be accessed through methods synchronized on this batch.
     * It is to avoid races between a new request being added just when the batch
     * is being processed.
     */
    private final Map<SegmentAllocateRequest, CompletableFuture<SegmentIdWithShardSpec>>
        requestToFuture = new HashMap<>();

    AllocateRequestBatch(AllocateRequestKey key)
    {
      this.key = key;
    }

    synchronized Future<SegmentIdWithShardSpec> add(SegmentAllocateRequest request)
    {
      log.debug("Adding request to batch [%s]: %s", key, request.getAction());
      return requestToFuture.computeIfAbsent(request, req -> new CompletableFuture<>());
    }

    synchronized void transferRequestsFrom(AllocateRequestBatch batch)
    {
      requestToFuture.putAll(batch.requestToFuture);
      batch.requestToFuture.clear();
    }

    synchronized Set<SegmentAllocateRequest> getRequests()
    {
      return new HashSet<>(requestToFuture.keySet());
    }

    synchronized void failPendingRequests(String reason)
    {
      failPendingRequests(new ISE(reason));
    }

    synchronized void failPendingRequests(Throwable cause)
    {
      if (!requestToFuture.isEmpty()) {
        log.warn("Failing [%d] requests in batch due to [%s]. Batch key: %s", size(), cause.getMessage(), key);
        requestToFuture.values().forEach(future -> future.completeExceptionally(cause));
        requestToFuture.keySet().forEach(
            request -> emitTaskMetric("task/action/failed/count", 1L, request)
        );
        requestToFuture.clear();
      }
    }

    synchronized void handleResult(SegmentAllocateResult result, SegmentAllocateRequest request)
    {
      request.incrementAttempts();

      if (result.isSuccess()) {
        emitTaskMetric("task/action/success/count", 1L, request);
        requestToFuture.remove(request).complete(result.getSegmentId());
      } else if (request.canRetry()) {
        log.info(
            "Allocation failed in attempt [%d] due to error [%s]. Can still retry. Action: %s",
            request.getAttempts(),
            result.getErrorMessage(),
            request.getAction()
        );
      } else {
        emitTaskMetric("task/action/failed/count", 1L, request);
        log.error(
            "Failing allocate action after [%d] attempts. Latest error [%s]. Action: %s",
            request.getAttempts(),
            result.getErrorMessage(),
            request.getAction()
        );
        requestToFuture.remove(request).completeExceptionally(new ISE(result.getErrorMessage()));
      }
    }

    synchronized boolean isEmpty()
    {
      return requestToFuture.isEmpty();
    }

    synchronized int size()
    {
      return requestToFuture.size();
    }
  }

  /**
   * Key to identify a batch of allocation requests.
   */
  private static class AllocateRequestKey
  {
    /**
     * ID to distinguish between two batches for the same datasource, groupId, etc.
     */
    private final int batchIncrementalId;

    private long queueTimeMillis;
    private final long maxWaitTimeMillis;

    private final String dataSource;
    private final String groupId;
    private final Interval preferredAllocationInterval;
    private final Granularity preferredSegmentGranularity;

    private final boolean skipSegmentLineageCheck;
    private final LockGranularity lockGranularity;

    private final boolean useNonRootGenPartitionSpace;

    private final int hash;

    /**
     * Creates a new key for the given request. The batch for a unique key will
     * always contain a single request.
     */
    AllocateRequestKey(SegmentAllocateRequest request, long maxWaitTimeMillis, int batchIncrementalId)
    {
      final SegmentAllocateAction action = request.getAction();
      final Task task = request.getTask();

      this.batchIncrementalId = batchIncrementalId;
      this.dataSource = action.getDataSource();
      this.groupId = task.getGroupId();
      this.skipSegmentLineageCheck = action.isSkipSegmentLineageCheck();
      this.lockGranularity = action.getLockGranularity();
      this.useNonRootGenPartitionSpace = action.getPartialShardSpec()
                                               .useNonRootGenerationPartitionSpace();
      this.preferredSegmentGranularity = action.getPreferredSegmentGranularity();
      this.preferredAllocationInterval = action.getPreferredSegmentGranularity()
                                               .bucket(action.getTimestamp());

      this.hash = Objects.hash(
          dataSource,
          groupId,
          batchIncrementalId,
          skipSegmentLineageCheck,
          useNonRootGenPartitionSpace,
          preferredAllocationInterval,
          lockGranularity
      );

      this.maxWaitTimeMillis = maxWaitTimeMillis;
    }

    void resetQueueTime()
    {
      queueTimeMillis = System.currentTimeMillis();
    }

    long getQueueTime()
    {
      return queueTimeMillis;
    }

    boolean isDue()
    {
      return System.currentTimeMillis() - queueTimeMillis >= maxWaitTimeMillis;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AllocateRequestKey that = (AllocateRequestKey) o;
      return dataSource.equals(that.dataSource)
             && groupId.equals(that.groupId)
             && batchIncrementalId == that.batchIncrementalId
             && skipSegmentLineageCheck == that.skipSegmentLineageCheck
             && useNonRootGenPartitionSpace == that.useNonRootGenPartitionSpace
             && preferredAllocationInterval.equals(that.preferredAllocationInterval)
             && lockGranularity == that.lockGranularity;
    }

    @Override
    public int hashCode()
    {
      return hash;
    }

    @Override
    public String toString()
    {
      return "{" +
             "ds='" + dataSource + '\'' +
             ", gr='" + groupId + '\'' +
             ", incId=" + batchIncrementalId +
             ", lock=" + lockGranularity +
             ", invl=" + preferredAllocationInterval +
             ", slc=" + skipSegmentLineageCheck +
             '}';
    }
  }
}
