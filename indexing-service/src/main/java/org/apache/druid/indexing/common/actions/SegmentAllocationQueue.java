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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Queue for {@link SegmentAllocateRequest}s.
 */
@ManageLifecycle
public class SegmentAllocationQueue implements DruidLeaderSelector.Listener
{
  private static final Logger log = new Logger(SegmentAllocationQueue.class);

  private final long maxWaitTimeMillis;
  private final boolean enabled;

  private final TaskLockbox taskLockbox;
  private final ScheduledExecutorService executor;
  private final IndexerMetadataStorageCoordinator metadataStorage;
  private final DruidLeaderSelector leaderSelector;
  private final ServiceEmitter emitter;

  private final ConcurrentHashMap<AllocateRequestKey, AllocateRequestBatch> keyToBatch = new ConcurrentHashMap<>();
  private final Deque<AllocateRequestBatch> processingQueue = new ConcurrentLinkedDeque<>();

  @Inject
  public SegmentAllocationQueue(
      TaskLockbox taskLockbox,
      TaskLockConfig taskLockConfig,
      IndexerMetadataStorageCoordinator metadataStorage,
      @IndexingService DruidLeaderSelector leaderSelector,
      ServiceEmitter emitter
  )
  {
    this.emitter = emitter;
    this.taskLockbox = taskLockbox;
    this.metadataStorage = metadataStorage;
    this.leaderSelector = leaderSelector;
    this.maxWaitTimeMillis = taskLockConfig.getBatchAllocationMaxWaitTime();
    this.enabled = taskLockConfig.isBatchSegmentAllocation();

    this.executor = ScheduledExecutors.fixed(1, "SegmentAllocQueue-%s");
  }

  @LifecycleStart
  public void start()
  {
    log.info("Starting queue.");
    if (leaderSelector.isLeader()) {
      scheduleQueuePoll(maxWaitTimeMillis);
      log.info("Scheduled queue processing.");
    }
    leaderSelector.registerListener(this);
  }

  @LifecycleStop
  public void stop()
  {
    log.info("Stopping queue.");
    executor.shutdownNow();
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  private void scheduleQueuePoll(long delay)
  {
    executor.schedule(this::processBatchesDue, delay, TimeUnit.MILLISECONDS);
  }

  /**
   * Queues a SegmentAllocateRequest. The returned future may complete successfully
   * with a non-null value or with a non-null value.
   */
  public Future<SegmentIdWithShardSpec> add(SegmentAllocateRequest request)
  {
    if (!leaderSelector.isLeader()) {
      throw new ISE("Cannot allocate segment if not leader.");
    }

    SegmentAllocateAction action = request.getAction();

    // Requests for exclusive time chunks cannot be batched with other requests
    boolean isExclusiveTimeChunkLock = action.getLockGranularity() == LockGranularity.TIME_CHUNK
                                       && action.getTaskLockType() == TaskLockType.EXCLUSIVE;
    final AllocateRequestKey requestKey = new AllocateRequestKey(action, isExclusiveTimeChunkLock);

    final AtomicReference<Future<SegmentIdWithShardSpec>> requestFuture = new AtomicReference<>();
    keyToBatch.compute(requestKey, (key, existingBatch) -> {
      AllocateRequestBatch computedBatch = existingBatch;
      if (computedBatch == null) {
        computedBatch = new AllocateRequestBatch(key);
        computedBatch.resetQueueTime();
        processingQueue.offer(computedBatch);
      }

      requestFuture.set(computedBatch.add(request));
      return computedBatch;
    });

    return requestFuture.get();
  }

  private void requeueBatch(AllocateRequestBatch batch)
  {
    log.info("Requeueing [%d] failed requests in batch [%s].", batch.size(), batch.key);
    keyToBatch.compute(batch.key, (key, existingBatch) -> {
      if (existingBatch == null) {
        batch.resetQueueTime();
        return batch;
      }

      // Merge requests from this batch to existing one
      existingBatch.merge(batch);
      return existingBatch;
    });
  }

  private void processBatchesDue()
  {
    // If not leader, clear the queue and do not schedule any more rounds of processing
    if (!leaderSelector.isLeader()) {
      log.info("Not leader anymore. Clearing [%d] batches from queue.", processingQueue.size());
      processingQueue.clear();
      keyToBatch.clear();
      return;
    }

    // Process all batches which are due
    log.info("Processing all batches which are due for execution.");
    AllocateRequestBatch nextBatch = processingQueue.peek();
    while (nextBatch != null && nextBatch.isDue()) {
      processingQueue.poll();
      boolean processed;
      try {
        processed = processBatch(nextBatch);
      }
      catch (Throwable t) {
        processed = true;
        log.error(t, "Error while processing batch [%s]", nextBatch.key);
      }

      if (processed) {
        nextBatch.markCompleted();
      } else {
        requeueBatch(nextBatch);
      }

      nextBatch = processingQueue.peek();
    }

    // Schedule the next round of processing
    final long nextScheduleDelay;
    if (processingQueue.isEmpty()) {
      nextScheduleDelay = maxWaitTimeMillis;
    } else {
      nextBatch = processingQueue.peek();
      long timeElapsed = System.currentTimeMillis() - nextBatch.getQueueTime();
      nextScheduleDelay = Math.max(0, maxWaitTimeMillis - timeElapsed);
    }
    scheduleQueuePoll(nextScheduleDelay);
  }

  /**
   * Processes the given batch. Returns true if the batch was completely processed
   * and should not be requeued.
   */
  private boolean processBatch(AllocateRequestBatch requestBatch)
  {
    final AllocateRequestKey requestKey = requestBatch.key;
    keyToBatch.remove(requestKey);
    if (requestBatch.isEmpty()) {
      return true;
    }

    log.info(
        "Processing [%d] requests for batch [%s], queue time [%s].",
        requestBatch.size(),
        requestKey,
        requestBatch.getQueueTime()
    );

    final long startTimeMillis = System.currentTimeMillis();
    final int batchSize = requestBatch.size();
    emitBatchMetric("task/action/batch/size", batchSize, requestKey);
    emitBatchMetric("task/action/batch/queueTime", (startTimeMillis - requestBatch.getQueueTime()), requestKey);

    final Set<DataSegment> usedSegments = retrieveUsedSegments(requestKey);
    final int successCount = allocateSegmentsForBatch(requestBatch, usedSegments);

    emitBatchMetric("task/action/batch/runTime", (System.currentTimeMillis() - startTimeMillis), requestKey);
    log.info("Successfully processed [%d / %d] requests in batch [%s].", successCount, batchSize, requestKey);

    if (requestBatch.isEmpty()) {
      log.info("All requests in batch [%s] have been processed.", requestKey);
      return true;
    }

    // Requeue the batch only if used segments have changed
    log.info("There are [%d] failed requests in batch [%s].", requestBatch.size(), requestKey);
    final Set<DataSegment> updatedUsedSegments = retrieveUsedSegments(requestKey);

    if (updatedUsedSegments.equals(usedSegments)) {
      log.error("Used segments have not changed. Not requeueing failed requests.");
      return true;
    } else {
      log.info("Used segments have changed. Requeuing failed requests");
      return false;
    }
  }

  private Set<DataSegment> retrieveUsedSegments(AllocateRequestKey key)
  {
    return new HashSet<>(
        metadataStorage.retrieveUsedSegmentsForInterval(
            key.dataSource,
            key.rowInterval,
            Segments.ONLY_VISIBLE
        )
    );
  }

  private int allocateSegmentsForBatch(AllocateRequestBatch requestBatch, Set<DataSegment> usedSegments)
  {
    final AllocateRequestKey requestKey = requestBatch.key;
    final List<Interval> tryIntervals = getTryIntervals(requestKey, usedSegments);
    if (tryIntervals.isEmpty()) {
      log.error("Found no valid interval containing the row interval [%s]", requestKey.rowInterval);
      return 0;
    }

    int successCount = 0;
    for (Interval tryInterval : tryIntervals) {
      final List<SegmentAllocateRequest> requests = requestBatch.getPendingRequests();
      if (requests.isEmpty()) {
        break;
      }

      log.info(
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
      emitBatchMetric("task/action/batch/retries", 1L, requestKey);

      successCount += updateBatchWithResults(requestBatch, requests, results);
    }

    return successCount;
  }

  /**
   * Gets the intervals for which allocation should be tried.
   * <p>
   * If there are no used segments for this row, first try to allocate segments
   * using the preferred segment granularity. If that fails due to other nearby
   * segments, try progressively smaller granularities.
   * <p>
   * If there are used segments for this row, try only the interval of those used
   * segments (we assume that all of them must have the same interval).
   */
  private List<Interval> getTryIntervals(AllocateRequestKey key, Set<DataSegment> usedSegments)
  {
    final Interval rowInterval = key.rowInterval;
    if (usedSegments.isEmpty()) {
      return Granularity.granularitiesFinerThan(key.preferredSegmentGranularity)
                        .stream()
                        .map(granularity -> granularity.bucket(rowInterval.getStart()))
                        .filter(interval -> interval.contains(rowInterval))
                        .collect(Collectors.toList());
    } else {
      Interval existingInterval = usedSegments.iterator().next().getInterval();
      if (existingInterval.contains(rowInterval)) {
        return Collections.singletonList(existingInterval);
      } else {
        return Collections.emptyList();
      }
    }
  }

  private int updateBatchWithResults(
      AllocateRequestBatch requestBatch,
      List<SegmentAllocateRequest> requests,
      List<SegmentAllocateResult> results
  )
  {
    int successCount = 0;
    for (int i = 0; i < requests.size(); ++i) {
      SegmentAllocateResult result = results.get(i);
      if (result.isSuccess()) {
        ++successCount;
      }

      requestBatch.handleResult(result, requests.get(i));
    }
    return successCount;
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
    metricBuilder.setDimension(DruidMetrics.INTERVAL, key.rowInterval.toString());
    emitter.emit(metricBuilder.build(metric, value));
  }

  @Override
  public void becomeLeader()
  {
    log.info("Elected leader. Starting queue processing.");

    // Start polling the queue
    scheduleQueuePoll(maxWaitTimeMillis);
  }

  @Override
  public void stopBeingLeader()
  {
    log.info("Not leader anymore. Stopping queue processing.");
  }

  /**
   * A batch of segment allocation requests.
   */
  private class AllocateRequestBatch
  {
    private long queueTimeMillis;
    private final AllocateRequestKey key;

    /**
     * This must be accessed through methods synchronized on this batch.
     * It is to avoid races between a new request being added just when the batch
     * is being processed.
     */
    @GuardedBy("this")
    private final Map<SegmentAllocateRequest, CompletableFuture<SegmentIdWithShardSpec>>
        requestToFuture = new HashMap<>();

    AllocateRequestBatch(AllocateRequestKey key)
    {
      log.info("Creating a new batch with key: %s", key);
      this.key = key;
    }

    synchronized Future<SegmentIdWithShardSpec> add(SegmentAllocateRequest request)
    {
      log.info("Adding request to batch [%s]: %s", key, request.getAction());
      return requestToFuture.computeIfAbsent(request, req -> new CompletableFuture<>());
    }

    synchronized void merge(AllocateRequestBatch batch)
    {
      requestToFuture.putAll(batch.requestToFuture);
      batch.requestToFuture.clear();
    }

    synchronized List<SegmentAllocateRequest> getPendingRequests()
    {
      return new ArrayList<>(requestToFuture.keySet());
    }

    synchronized void markCompleted()
    {
      if (!requestToFuture.isEmpty()) {
        log.info("Marking [%d] requests in batch [%s] as failed.", size(), key);
        requestToFuture.values().forEach(future -> future.complete(null));
      }
    }

    synchronized void handleResult(SegmentAllocateResult result, SegmentAllocateRequest request)
    {
      request.incrementAttempts();

      if (result.isSuccess()) {
        emitTaskMetric("task/action/success/count", 1L, request);
        requestToFuture.remove(request).complete(result.getSegmentId());
        return;
      }

      log.info("Failed to allocate segment for action [%s]: %s", request.getAction(), result.getErrorMessage());
      if (request.canRetry()) {
        log.debug(
            "Can requeue action [%s] after [%d] failed attempts.",
            request.getAction(),
            request.getAttempts()
        );
      } else {
        emitTaskMetric("task/action/failed/count", 1L, request);
        log.error(
            "Removing allocation action [%s] from batch after [%d] failed attempts.",
            request.getAction(),
            request.getAttempts()
        );
        requestToFuture.remove(request).complete(null);
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
      return System.currentTimeMillis() - queueTimeMillis > maxWaitTimeMillis;
    }
  }

  /**
   * Key to identify a batch of allocation requests.
   */
  private static class AllocateRequestKey
  {
    private final boolean unique;

    private final String dataSource;
    private final Interval rowInterval;
    private final Granularity queryGranularity;
    private final Granularity preferredSegmentGranularity;

    private final boolean skipSegmentLineageCheck;
    private final LockGranularity lockGranularity;
    private final TaskLockType taskLockType;

    private final boolean useNonRootGenPartitionSpace;

    private final int hash;
    private final String serialized;

    /**
     * Creates a new key for the given action. The batch for a unique key will
     * always contain a single request.
     */
    AllocateRequestKey(SegmentAllocateAction action, boolean unique)
    {
      this.unique = unique;
      this.dataSource = action.getDataSource();
      this.queryGranularity = action.getQueryGranularity();
      this.preferredSegmentGranularity = action.getPreferredSegmentGranularity();
      this.skipSegmentLineageCheck = action.isSkipSegmentLineageCheck();
      this.lockGranularity = action.getLockGranularity();
      this.taskLockType = action.getTaskLockType();

      this.useNonRootGenPartitionSpace = action.getPartialShardSpec()
                                               .useNonRootGenerationPartitionSpace();
      this.rowInterval = queryGranularity.bucket(action.getTimestamp())
                                         .withChronology(ISOChronology.getInstanceUTC());

      this.hash = unique ? super.hashCode() : Objects.hash(
          skipSegmentLineageCheck,
          useNonRootGenPartitionSpace,
          dataSource,
          rowInterval,
          queryGranularity,
          preferredSegmentGranularity,
          lockGranularity,
          taskLockType
      );
      this.serialized = serialize();
    }

    @Override
    public boolean equals(Object o)
    {
      if (unique) {
        return this == o;
      }
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AllocateRequestKey that = (AllocateRequestKey) o;
      return skipSegmentLineageCheck == that.skipSegmentLineageCheck
             && useNonRootGenPartitionSpace == that.useNonRootGenPartitionSpace
             && dataSource.equals(that.dataSource)
             && rowInterval.equals(that.rowInterval)
             && queryGranularity.equals(that.queryGranularity)
             && preferredSegmentGranularity.equals(that.preferredSegmentGranularity)
             && lockGranularity == that.lockGranularity
             && taskLockType == that.taskLockType;
    }

    @Override
    public int hashCode()
    {
      return hash;
    }

    @Override
    public String toString()
    {
      return serialized;
    }

    private String serialize()
    {
      return "{" +
             "unique=" + unique +
             ", skipLineageCheck=" + skipSegmentLineageCheck +
             ", ds='" + dataSource + '\'' +
             ", row=" + rowInterval +
             ", lock=" + lockGranularity + "/" + taskLockType +
             '}';
    }
  }
}
