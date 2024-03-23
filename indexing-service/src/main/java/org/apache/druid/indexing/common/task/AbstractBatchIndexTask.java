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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.batch.MaxAllowedLocksExceededException;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.granularity.IntervalsByGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.handoff.SegmentHandoffNotifier;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.IngestionSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Abstract class for batch tasks like {@link IndexTask}.
 * Provides some methods such as {@link #determineSegmentGranularity}, {@link #findInputSegments},
 * and {@link #determineLockGranularityAndTryLock} for easily acquiring task locks.
 */
public abstract class AbstractBatchIndexTask extends AbstractTask
{
  private static final Logger log = new Logger(AbstractBatchIndexTask.class);

  protected boolean segmentAvailabilityConfirmationCompleted = false;
  protected long segmentAvailabilityWaitTimeMs = 0L;

  @GuardedBy("this")
  private final TaskResourceCleaner resourceCloserOnAbnormalExit = new TaskResourceCleaner();

  @GuardedBy("this")
  private boolean stopped = false;

  private TaskLockHelper taskLockHelper;

  private final int maxAllowedLockCount;

  // Store lock versions
  Map<Interval, String> intervalToVersion = new HashMap<>();

  protected AbstractBatchIndexTask(String id, String dataSource, Map<String, Object> context, IngestionMode ingestionMode)
  {
    super(id, dataSource, context, ingestionMode);
    maxAllowedLockCount = -1;
  }

  protected AbstractBatchIndexTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context,
      int maxAllowedLockCount,
      IngestionMode ingestionMode
  )
  {
    super(id, groupId, taskResource, dataSource, context, ingestionMode);
    this.maxAllowedLockCount = maxAllowedLockCount;
  }

  /**
   * Run this task. Before running the task, it checks the current task is already stopped and
   * registers a cleaner to interrupt the thread running this task on abnormal exits.
   *
   * @see #runTask(TaskToolbox)
   * @see #stopGracefully(TaskConfig)
   */
  @Override
  public String setup(TaskToolbox toolbox) throws Exception
  {
    if (taskLockHelper == null) {
      // Subclasses generally use "isReady" to initialize the taskLockHelper. It's not guaranteed to be called before
      // "run", and so we call it here to ensure it happens.
      //
      // We're only really calling it for its side effects, and we expect it to return "true". If it doesn't, something
      // strange is going on, so bail out.
      if (!isReady(toolbox.getTaskActionClient())) {
        throw new ISE("Cannot start; not ready!");
      }
    }

    synchronized (this) {
      if (stopped) {
        return "Attempting to run a task that has been stopped. See overlord & task logs for more details.";
      } else {
        // Register the cleaner to interrupt the current thread first.
        // Since the resource closer cleans up the registered resources in LIFO order,
        // this will be executed last on abnormal exists.
        // The order is sometimes important. For example, Appenderator has two methods of close() and closeNow(), and
        // closeNow() is supposed to be called on abnormal exits. Interrupting the current thread could lead to close()
        // to be called indirectly, e.g., for Appenderators in try-with-resources. In this case, closeNow() should be
        // called before the current thread is interrupted, so that subsequent close() calls can be ignored.
        final Thread currentThread = Thread.currentThread();
        resourceCloserOnAbnormalExit.register(config -> currentThread.interrupt());
      }
    }
    return super.setup(toolbox);
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    synchronized (this) {
      stopped = true;
      resourceCloserOnAbnormalExit.clean(taskConfig);
    }
  }

  /**
   * Returns an {@link InputRow} iterator which iterates over an input source.
   * The returned iterator filters out rows which don't satisfy the given filter or cannot be parsed properly.
   * The returned iterator can throw {@link org.apache.druid.java.util.common.parsers.ParseException}s in
   * {@link Iterator#hasNext()} when it hits {@link ParseExceptionHandler#maxAllowedParseExceptions}.
   */
  public static FilteringCloseableInputRowIterator inputSourceReader(
      File tmpDir,
      DataSchema dataSchema,
      InputSource inputSource,
      @Nullable InputFormat inputFormat,
      Predicate<InputRow> rowFilter,
      RowIngestionMeters ingestionMeters,
      ParseExceptionHandler parseExceptionHandler
  ) throws IOException
  {
    final InputSourceReader inputSourceReader = dataSchema.getTransformSpec().decorate(
        inputSource.reader(
            InputRowSchemas.fromDataSchema(dataSchema),
            inputFormat,
            tmpDir
        )
    );
    return new FilteringCloseableInputRowIterator(
        inputSourceReader.read(ingestionMeters),
        rowFilter,
        ingestionMeters,
        parseExceptionHandler
    );
  }

  protected static Predicate<InputRow> defaultRowFilter(GranularitySpec granularitySpec)
  {
    return inputRow -> {
      if (inputRow == null) {
        return false;
      }
      final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
      return optInterval.isPresent();
    };
  }

  /**
   * Registers a resource cleaner which is executed on abnormal exits.
   *
   * @see Task#stopGracefully
   */
  protected void registerResourceCloserOnAbnormalExit(Consumer<TaskConfig> cleaner)
  {
    synchronized (this) {
      resourceCloserOnAbnormalExit.register(cleaner);
    }
  }

  /**
   * Return true if this task can overwrite existing segments.
   */
  public abstract boolean requireLockExistingSegments();

  /**
   * Find segments to lock in the given intervals.
   * If this task is intend to overwrite only some segments in those intervals, this method should return only those
   * segments instead of entire segments in those intervals.
   */
  public abstract List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException;

  /**
   * Returns true if this task is in the perfect (guaranteed) rollup mode.
   */
  public abstract boolean isPerfectRollup();

  /**
   * Returns the segmentGranularity defined in the ingestion spec.
   */
  @Nullable
  public abstract Granularity getSegmentGranularity();

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  public TaskLockHelper getTaskLockHelper()
  {
    return Preconditions.checkNotNull(taskLockHelper, "taskLockHelper is not initialized yet");
  }

  /**
   * Attempts to acquire a lock that covers certain intervals.
   * <p>
   * Will look at {@link Tasks#FORCE_TIME_CHUNK_LOCK_KEY} to decide whether to acquire a time chunk or segment lock.
   * <p>
   * If {@link Tasks#FORCE_TIME_CHUNK_LOCK_KEY} is set, or if {@param intervals} is nonempty, then this method
   * will initialize {@link #taskLockHelper} as a side effect.
   *
   * @return whether the lock was acquired
   */
  public boolean determineLockGranularityAndTryLock(TaskActionClient client, List<Interval> intervals)
      throws IOException
  {
    final boolean forceTimeChunkLock = getContextValue(
        Tasks.FORCE_TIME_CHUNK_LOCK_KEY,
        Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK
    );
    final IngestionMode ingestionMode = getIngestionMode();
    // Respect task context value most.
    if (forceTimeChunkLock || ingestionMode == IngestionMode.REPLACE) {
      log.info(
          "Using time chunk lock since forceTimeChunkLock is [%s] and mode is [%s].",
          forceTimeChunkLock, ingestionMode
      );
      taskLockHelper = createLockHelper(LockGranularity.TIME_CHUNK);
      if (!intervals.isEmpty()) {
        return tryTimeChunkLock(client, intervals);
      } else {
        return true;
      }
    } else {
      if (!intervals.isEmpty()) {
        final LockGranularityDetermineResult result = determineSegmentGranularity(client, intervals);
        taskLockHelper = createLockHelper(result.lockGranularity);
        return tryLockWithDetermineResult(client, result);
      } else {
        // This branch is the only one that will not initialize taskLockHelper.
        return true;
      }
    }
  }

  /**
   * Attempts to acquire a lock that covers certain segments.
   * <p>
   * Will look at {@link Tasks#FORCE_TIME_CHUNK_LOCK_KEY} to decide whether to acquire a time chunk or segment lock.
   * <p>
   * This method will initialize {@link #taskLockHelper} as a side effect.
   *
   * @return whether the lock was acquired
   */
  boolean determineLockGranularityAndTryLockWithSegments(
      TaskActionClient client,
      List<DataSegment> segments,
      BiConsumer<LockGranularity, List<DataSegment>> segmentCheckFunction
  ) throws IOException
  {
    final boolean forceTimeChunkLock = getContextValue(
        Tasks.FORCE_TIME_CHUNK_LOCK_KEY,
        Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK
    );

    if (forceTimeChunkLock) {
      log.info("[%s] is set to true in task context. Use timeChunk lock", Tasks.FORCE_TIME_CHUNK_LOCK_KEY);
      taskLockHelper = createLockHelper(LockGranularity.TIME_CHUNK);
      segmentCheckFunction.accept(LockGranularity.TIME_CHUNK, segments);
      return tryTimeChunkLock(
          client,
          new ArrayList<>(segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet()))
      );
    } else {
      final LockGranularityDetermineResult result = determineSegmentGranularity(segments);
      taskLockHelper = createLockHelper(result.lockGranularity);
      segmentCheckFunction.accept(result.lockGranularity, segments);
      return tryLockWithDetermineResult(client, result);
    }
  }

  private LockGranularityDetermineResult determineSegmentGranularity(TaskActionClient client, List<Interval> intervals)
      throws IOException
  {
    if (requireLockExistingSegments()) {
      if (isPerfectRollup()) {
        log.info("Using timeChunk lock for perfect rollup");
        return new LockGranularityDetermineResult(LockGranularity.TIME_CHUNK, intervals, null);
      } else if (!intervals.isEmpty()) {
        // This method finds segments falling in all given intervals and then tries to lock those segments.
        // Thus, there might be a race between calling findSegmentsToLock() and determineSegmentGranularity(),
        // i.e., a new segment can be added to the interval or an existing segment might be removed.
        // Removed segments should be fine because indexing tasks would do nothing with removed segments.
        // However, tasks wouldn't know about new segments added after findSegmentsToLock() call, it may missing those
        // segments. This is usually fine, but if you want to avoid this, you should use timeChunk lock instead.
        return determineSegmentGranularity(findSegmentsToLock(client, intervals));
      } else {
        log.info("Using segment lock for empty intervals");
        return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, Collections.emptyList());
      }
    } else {
      log.info("Using segment lock since we don't have to lock existing segments");
      return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, Collections.emptyList());
    }
  }

  private boolean tryLockWithDetermineResult(TaskActionClient client, LockGranularityDetermineResult result)
      throws IOException
  {
    if (result.lockGranularity == LockGranularity.TIME_CHUNK) {
      return tryTimeChunkLock(client, Preconditions.checkNotNull(result.intervals, "intervals"));
    } else {
      return taskLockHelper.verifyAndLockExistingSegments(
          client,
          Preconditions.checkNotNull(result.segments, "segments")
      );
    }
  }

  /**
   * Builds a TaskAction to publish segments based on the type of locks that this
   * task acquires.
   *
   * @see #determineLockType
   */
  protected TaskAction<SegmentPublishResult> buildPublishAction(
      Set<DataSegment> segmentsToBeOverwritten,
      Set<DataSegment> segmentsToPublish,
      TaskLockType lockType
  )
  {
    switch (lockType) {
      case REPLACE:
        return SegmentTransactionalReplaceAction.create(segmentsToPublish);
      case APPEND:
        return SegmentTransactionalAppendAction.forSegments(segmentsToPublish);
      default:
        return SegmentTransactionalInsertAction.overwriteAction(segmentsToBeOverwritten, segmentsToPublish);
    }
  }

  protected boolean tryTimeChunkLock(TaskActionClient client, List<Interval> intervals) throws IOException
  {
    // The given intervals are first converted to align with segment granularity. This is because,
    // when an overwriting task finds a version for a given input row, it expects the interval
    // associated to each version to be equal or larger than the time bucket where the input row falls in.
    // See ParallelIndexSupervisorTask.findVersion().
    final Iterator<Interval> intervalIterator;
    final Granularity segmentGranularity = getSegmentGranularity();
    if (segmentGranularity == null) {
      intervalIterator = JodaUtils.condenseIntervals(intervals).iterator();
    } else {
      IntervalsByGranularity intervalsByGranularity = new IntervalsByGranularity(intervals, segmentGranularity);
      // the following is calling a condense that does not materialize the intervals:
      intervalIterator = JodaUtils.condensedIntervalsIterator(intervalsByGranularity.granularityIntervalsIterator());
    }

    // Intervals are already condensed to avoid creating too many locks.
    // Intervals are also sorted and thus it's safe to compare only the previous interval and current one for dedup.
    Interval prev = null;
    int locksAcquired = 0;
    while (intervalIterator.hasNext()) {
      final Interval cur = intervalIterator.next();
      if (prev != null && cur.equals(prev)) {
        continue;
      }

      if (maxAllowedLockCount >= 0 && locksAcquired >= maxAllowedLockCount) {
        throw new MaxAllowedLocksExceededException(maxAllowedLockCount);
      }

      prev = cur;
      final TaskLockType taskLockType = determineLockType(LockGranularity.TIME_CHUNK);
      final TaskLock lock = client.submit(new TimeChunkLockTryAcquireAction(taskLockType, cur));
      if (lock == null) {
        return false;
      }
      if (lock.isRevoked()) {
        throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", cur));
      }
      locksAcquired++;
      intervalToVersion.put(cur, lock.getVersion());
    }
    return true;
  }

  private TaskLockHelper createLockHelper(LockGranularity lockGranularity)
  {
    return new TaskLockHelper(
        lockGranularity == LockGranularity.SEGMENT,
        determineLockType(lockGranularity)
    );
  }

  /**
   * Determines the type of lock to use with the given lock granularity.
   */
  private TaskLockType determineLockType(LockGranularity lockGranularity)
  {
    if (lockGranularity == LockGranularity.SEGMENT) {
      return TaskLockType.EXCLUSIVE;
    }

    final boolean useConcurrentLocks = QueryContexts.getAsBoolean(
        Tasks.USE_CONCURRENT_LOCKS,
        getContextValue(Tasks.USE_CONCURRENT_LOCKS),
        Tasks.DEFAULT_USE_CONCURRENT_LOCKS
    );
    final IngestionMode ingestionMode = getIngestionMode();
    if (useConcurrentLocks) {
      return ingestionMode == IngestionMode.APPEND ? TaskLockType.APPEND : TaskLockType.REPLACE;
    }

    final TaskLockType contextTaskLockType = QueryContexts.getAsEnum(
        Tasks.TASK_LOCK_TYPE,
        getContextValue(Tasks.TASK_LOCK_TYPE),
        TaskLockType.class
    );

    final TaskLockType lockType;
    if (contextTaskLockType == null) {
      lockType = getContextValue(Tasks.USE_SHARED_LOCK, false)
                 ? TaskLockType.SHARED : TaskLockType.EXCLUSIVE;
    } else {
      lockType = contextTaskLockType;
    }

    if ((lockType == TaskLockType.SHARED || lockType == TaskLockType.APPEND)
        && ingestionMode != IngestionMode.APPEND) {
      // Lock types SHARED and APPEND are allowed only in APPEND ingestion mode
      return Tasks.DEFAULT_TASK_LOCK_TYPE;
    } else {
      return lockType;
    }
  }

  private LockGranularityDetermineResult determineSegmentGranularity(List<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      log.info("Using segment lock for empty segments");
      // Set useSegmentLock even though we don't get any locks.
      // Note that we should get any lock before data ingestion if we are supposed to use timChunk lock.
      return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, Collections.emptyList());
    }

    if (requireLockExistingSegments()) {
      final Granularity granularityFromSegments = findGranularityFromSegments(segments);
      @Nullable
      final Granularity segmentGranularityFromSpec = getSegmentGranularity();
      final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());

      if (granularityFromSegments == null
          || segmentGranularityFromSpec != null
             && (!granularityFromSegments.equals(segmentGranularityFromSpec)
                 || segments.stream()
                            .anyMatch(segment -> !segmentGranularityFromSpec.isAligned(segment.getInterval())))) {
        // This case is one of the followings:
        // 1) Segments have different granularities.
        // 2) Segment granularity in ingestion spec is different from the one of existig segments.
        // 3) Some existing segments are not aligned with the segment granularity in the ingestion spec.
        log.info("Detected segmentGranularity change. Using timeChunk lock");
        return new LockGranularityDetermineResult(LockGranularity.TIME_CHUNK, intervals, null);
      } else {
        // Use segment lock
        // Create a timeline to find latest segments only
        final SegmentTimeline timeline = SegmentTimeline.forSegments(
            segments
        );

        Set<DataSegment> segmentsToLock = timeline.findNonOvershadowedObjectsInInterval(
            JodaUtils.umbrellaInterval(intervals),
            Partitions.ONLY_COMPLETE
        );
        log.info("No segmentGranularity change detected and it's not perfect rollup. Using segment lock");
        return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, new ArrayList<>(segmentsToLock));
      }
    } else {
      // Set useSegmentLock even though we don't get any locks.
      // Note that we should get any lock before data ingestion if we are supposed to use timChunk lock.
      log.info("Using segment lock since we don't have to lock existing segments");
      return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, Collections.emptyList());
    }
  }

  /**
   * We currently don't support appending perfectly rolled up segments. This might be supported in the future if there
   * is a good use case. If we want to support appending perfectly rolled up segments, we need to fix some other places
   * first. For example, {@link HashBasedNumberedShardSpec#getLookup} assumes that
   * the start partition ID of the set of perfectly rolled up segments is 0. Instead it might need to store an ordinal
   * in addition to the partition ID which represents the ordinal in the perfectly rolled up segment set.
   */
  public static boolean isGuaranteedRollup(
      IngestionMode ingestionMode,
      IndexTuningConfig tuningConfig
  )
  {
    Preconditions.checkArgument(
        !(ingestionMode == IngestionMode.APPEND && tuningConfig.isForceGuaranteedRollup()),
        "Perfect rollup cannot be guaranteed when appending to existing dataSources"
    );
    return tuningConfig.isForceGuaranteedRollup();
  }

  public static Function<Set<DataSegment>, Set<DataSegment>> compactionStateAnnotateFunction(
      boolean storeCompactionState,
      TaskToolbox toolbox,
      IngestionSpec ingestionSpec
  )
  {
    if (storeCompactionState) {
      TuningConfig tuningConfig = ingestionSpec.getTuningConfig();
      GranularitySpec granularitySpec = ingestionSpec.getDataSchema().getGranularitySpec();
      // We do not need to store dimensionExclusions and spatialDimensions since auto compaction does not support them
      DimensionsSpec dimensionsSpec = ingestionSpec.getDataSchema().getDimensionsSpec() == null
                                      ? null
                                      : new DimensionsSpec(ingestionSpec.getDataSchema().getDimensionsSpec().getDimensions());
      // We only need to store filter since that is the only field auto compaction support
      Map<String, Object> transformSpec = ingestionSpec.getDataSchema().getTransformSpec() == null || TransformSpec.NONE.equals(ingestionSpec.getDataSchema().getTransformSpec())
                                          ? null
                                          : new ClientCompactionTaskTransformSpec(ingestionSpec.getDataSchema().getTransformSpec().getFilter()).asMap(toolbox.getJsonMapper());
      List<Object> metricsSpec = ingestionSpec.getDataSchema().getAggregators() == null
                                 ? null
                                 : toolbox.getJsonMapper().convertValue(ingestionSpec.getDataSchema().getAggregators(), new TypeReference<List<Object>>() {});

      final CompactionState compactionState = new CompactionState(
          tuningConfig.getPartitionsSpec(),
          dimensionsSpec,
          metricsSpec,
          transformSpec,
          tuningConfig.getIndexSpec().asMap(toolbox.getJsonMapper()),
          granularitySpec.asMap(toolbox.getJsonMapper())
      );
      return segments -> segments
          .stream()
          .map(s -> s.withLastCompactionState(compactionState))
          .collect(Collectors.toSet());
    } else {
      return Function.identity();
    }
  }

  @Nullable
  static Granularity findGranularityFromSegments(List<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      return null;
    }
    final Period firstSegmentPeriod = segments.get(0).getInterval().toPeriod();
    final boolean allHasSameGranularity = segments
        .stream()
        .allMatch(segment -> firstSegmentPeriod.equals(segment.getInterval().toPeriod()));
    if (allHasSameGranularity) {
      return GranularityType.fromPeriod(firstSegmentPeriod).getDefaultGranularity();
    } else {
      return null;
    }
  }

  /**
   * <p>
   * This task will overwrite some segments with data read from input source outside of Druid.
   * As a result, only the segments falling in intervalsToRead should be locked.
   * <p>
   * The order of segments within the returned list is unspecified, but each segment is guaranteed to appear in the list
   * only once.
   */
  protected static List<DataSegment> findInputSegments(
      String dataSource,
      TaskActionClient actionClient,
      List<Interval> intervalsToRead
  ) throws IOException
  {
    return ImmutableList.copyOf(
        actionClient.submit(
            new RetrieveUsedSegmentsAction(dataSource, intervalsToRead)
        )
    );
  }

  /**
   * Wait for segments to become available on the cluster. If waitTimeout is reached, giveup on waiting. This is a
   * QoS method that can be used to make Batch Ingest tasks wait to finish until their ingested data is available on
   * the cluster. Doing so gives an end user assurance that a Successful task status means their data is available
   * for querying.
   *
   * @param toolbox {@link TaskToolbox} object with for assisting with task work.
   * @param segmentsToWaitFor {@link List} of segments to wait for availability.
   * @param waitTimeout Millis to wait before giving up
   * @return True if all segments became available, otherwise False.
   */
  protected boolean waitForSegmentAvailability(
      TaskToolbox toolbox,
      List<DataSegment> segmentsToWaitFor,
      long waitTimeout
  )
  {
    if (segmentsToWaitFor.isEmpty()) {
      log.info("Asked to wait for segments to be available, but I wasn't provided with any segments.");
      return true;
    } else if (waitTimeout < 0) {
      log.warn("Asked to wait for availability for < 0 seconds?! Requested waitTimeout: [%s]", waitTimeout);
      return false;
    }
    log.info("Waiting for [%d] segments to be loaded by the cluster...", segmentsToWaitFor.size());
    final long start = System.nanoTime();

    try (
        SegmentHandoffNotifier notifier = toolbox.getSegmentHandoffNotifierFactory()
                                                 .createSegmentHandoffNotifier(segmentsToWaitFor.get(0).getDataSource())
    ) {

      ExecutorService exec = Execs.directExecutor();
      CountDownLatch doneSignal = new CountDownLatch(segmentsToWaitFor.size());

      notifier.start();
      for (DataSegment s : segmentsToWaitFor) {
        notifier.registerSegmentHandoffCallback(
            new SegmentDescriptor(s.getInterval(), s.getVersion(), s.getShardSpec().getPartitionNum()),
            exec,
            () -> {
              log.debug(
                  "Confirmed availability for [%s]. Removing from list of segments to wait for",
                  s.getId()
              );
              doneSignal.countDown();
            }
        );
      }
      segmentAvailabilityConfirmationCompleted = doneSignal.await(waitTimeout, TimeUnit.MILLISECONDS);
      return segmentAvailabilityConfirmationCompleted;
    }
    catch (InterruptedException e) {
      log.warn("Interrupted while waiting for segment availablity; Unable to confirm availability!");
      Thread.currentThread().interrupt();
      return false;
    }
    finally {
      segmentAvailabilityWaitTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      toolbox.getEmitter().emit(
          new ServiceMetricEvent.Builder()
              .setDimension("dataSource", getDataSource())
              .setDimension("taskType", getType())
              .setDimension("taskId", getId())
              .setDimension("groupId", getGroupId())
              .setDimensionIfNotNull(DruidMetrics.TAGS, getContextValue(DruidMetrics.TAGS))
              .setDimension("segmentAvailabilityConfirmed", segmentAvailabilityConfirmationCompleted)
              .setMetric("task/segmentAvailability/wait/time", segmentAvailabilityWaitTimeMs)
      );
    }
  }
  
  @Nullable
  public static String findVersion(Map<Interval, String> versions, Interval interval)
  {
    return versions.entrySet().stream()
                   .filter(entry -> entry.getKey().contains(interval))
                   .map(Map.Entry::getValue)
                   .findFirst()
                   .orElse(null);
  }

  public static NonnullPair<Interval, String> findIntervalAndVersion(
      TaskToolbox toolbox,
      IngestionSpec<?, ?> ingestionSpec,
      DateTime timestamp,
      TaskLockType taskLockType
  ) throws IOException
  {
    // This method is called whenever subtasks need to allocate a new segment via the supervisor task.
    // As a result, this code is never called in the Overlord. For now using the materialized intervals
    // here is ok for performance reasons
    GranularitySpec granularitySpec = ingestionSpec.getDataSchema().getGranularitySpec();
    final Set<Interval> materializedBucketIntervals = granularitySpec.materializedBucketIntervals();

    // List locks whenever allocating a new segment because locks might be revoked and no longer valid.
    final List<TaskLock> locks = toolbox
        .getTaskActionClient()
        .submit(new LockListAction());
    final TaskLock revokedLock = locks.stream().filter(TaskLock::isRevoked).findAny().orElse(null);
    if (revokedLock != null) {
      throw new ISE("Lock revoked: [%s]", revokedLock);
    }
    final Map<Interval, String> versions = locks
        .stream()
        .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));

    Interval interval;
    String version;
    if (!materializedBucketIntervals.isEmpty()) {
      // If granularity spec has explicit intervals, we just need to find the version associated to the interval.
      // This is because we should have gotten all required locks up front when the task starts up.
      final Optional<Interval> maybeInterval = granularitySpec.bucketInterval(timestamp);
      if (!maybeInterval.isPresent()) {
        throw new IAE("Could not find interval for timestamp [%s]", timestamp);
      }

      interval = maybeInterval.get();
      if (!materializedBucketIntervals.contains(interval)) {
        throw new ISE("Unspecified interval[%s] in granularitySpec[%s]", interval, granularitySpec);
      }

      version = AbstractBatchIndexTask.findVersion(versions, interval);
      if (version == null) {
        throw new ISE("Cannot find a version for interval[%s]", interval);
      }
    } else {
      // We don't have explicit intervals. We can use the segment granularity to figure out what
      // interval we need, but we might not have already locked it.
      interval = granularitySpec.getSegmentGranularity().bucket(timestamp);
      version = AbstractBatchIndexTask.findVersion(versions, interval);
      if (version == null) {
        if (ingestionSpec.getTuningConfig() instanceof ParallelIndexTuningConfig) {
          final int maxAllowedLockCount = ((ParallelIndexTuningConfig) ingestionSpec.getTuningConfig())
              .getMaxAllowedLockCount();
          if (maxAllowedLockCount >= 0 && locks.size() >= maxAllowedLockCount) {
            throw new MaxAllowedLocksExceededException(maxAllowedLockCount);
          }
        }
        // We don't have a lock for this interval, so we should lock it now.
        final TaskLock lock = Preconditions.checkNotNull(
            toolbox.getTaskActionClient().submit(
                new TimeChunkLockTryAcquireAction(taskLockType, interval)
            ),
            "Cannot acquire a lock for interval[%s]",
            interval
        );
        if (lock.isRevoked()) {
          throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", interval));
        }
        version = lock.getVersion();
      }
    }
    return new NonnullPair<>(interval, version);
  }

  private static class LockGranularityDetermineResult
  {
    private final LockGranularity lockGranularity;
    @Nullable
    private final List<Interval> intervals; // null for segmentLock
    @Nullable
    private final List<DataSegment> segments; // null for timeChunkLock

    private LockGranularityDetermineResult(
        LockGranularity lockGranularity,
        @Nullable List<Interval> intervals,
        @Nullable List<DataSegment> segments
    )
    {
      this.lockGranularity = lockGranularity;
      this.intervals = intervals;
      this.segments = segments;
    }
  }

  /**
   * Get the version from the locks for a given timestamp. This will work if the locks were acquired upfront
   * @param timestamp
   * @return The interval andversion if n interval that contains an interval was found or null otherwise
   */
  @Nullable
  Pair<Interval, String> lookupVersion(DateTime timestamp)
  {
    java.util.Optional<Map.Entry<Interval, String>> intervalAndVersion = intervalToVersion.entrySet()
                                                                                          .stream()
                                                                                          .filter(e -> e.getKey()
                                                                                                        .contains(
                                                                                                            timestamp))
                                                                                          .findFirst();
    if (!intervalAndVersion.isPresent()) {
      return null;
    }
    return new Pair(intervalAndVersion.get().getKey(), intervalAndVersion.get().getValue());
  }

  protected SegmentIdWithShardSpec allocateNewSegmentForTombstone(
      IngestionSpec ingestionSchema,
      DateTime timestamp
  )
  {
    // Since tombstones are derived from inputIntervals, inputIntervals cannot be empty for replace, and locks are
    // all acquired upfront then the following stream query should always find the version
    Pair<Interval, String> intervalAndVersion = lookupVersion(timestamp);
    return new SegmentIdWithShardSpec(
        ingestionSchema.getDataSchema().getDataSource(),
        intervalAndVersion.lhs,
        intervalAndVersion.rhs,
        new TombstoneShardSpec()
    );
  }

}
