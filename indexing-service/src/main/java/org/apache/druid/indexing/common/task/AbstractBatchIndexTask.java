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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexIOConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.granularity.IntervalsByGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  @GuardedBy("this")
  private final TaskResourceCleaner resourceCloserOnAbnormalExit = new TaskResourceCleaner();

  @GuardedBy("this")
  private boolean stopped = false;

  private TaskLockHelper taskLockHelper;

  protected AbstractBatchIndexTask(String id, String dataSource, Map<String, Object> context)
  {
    super(id, dataSource, context);
  }

  protected AbstractBatchIndexTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context
  )
  {
    super(id, groupId, taskResource, dataSource, context);
  }

  /**
   * Run this task. Before running the task, it checks the current task is already stopped and
   * registers a cleaner to interrupt the thread running this task on abnormal exits.
   *
   * @see #runTask(TaskToolbox)
   * @see #stopGracefully(TaskConfig)
   */
  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
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
        return TaskStatus.failure(getId());
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
    return runTask(toolbox);
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
    final List<String> metricsNames = Arrays.stream(dataSchema.getAggregators())
                                            .map(AggregatorFactory::getName)
                                            .collect(Collectors.toList());
    final InputSourceReader inputSourceReader = dataSchema.getTransformSpec().decorate(
        inputSource.reader(
            new InputRowSchema(
                dataSchema.getTimestampSpec(),
                dataSchema.getDimensionsSpec(),
                metricsNames
            ),
            inputFormat,
            tmpDir
        )
    );
    return new FilteringCloseableInputRowIterator(
        inputSourceReader.read(),
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
   * The method to actually process this task. This method is executed in {@link #run(TaskToolbox)}.
   */
  public abstract TaskStatus runTask(TaskToolbox toolbox) throws Exception;

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
    // Respect task context value most.
    if (forceTimeChunkLock) {
      log.info("[%s] is set to true in task context. Use timeChunk lock", Tasks.FORCE_TIME_CHUNK_LOCK_KEY);
      taskLockHelper = new TaskLockHelper(false);
      if (!intervals.isEmpty()) {
        return tryTimeChunkLock(client, intervals);
      } else {
        return true;
      }
    } else {
      if (!intervals.isEmpty()) {
        final LockGranularityDetermineResult result = determineSegmentGranularity(client, intervals);
        taskLockHelper = new TaskLockHelper(result.lockGranularity == LockGranularity.SEGMENT);
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
      taskLockHelper = new TaskLockHelper(false);
      segmentCheckFunction.accept(LockGranularity.TIME_CHUNK, segments);
      return tryTimeChunkLock(
          client,
          new ArrayList<>(segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet()))
      );
    } else {
      final LockGranularityDetermineResult result = determineSegmentGranularity(segments);
      taskLockHelper = new TaskLockHelper(result.lockGranularity == LockGranularity.SEGMENT);
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
    while (intervalIterator.hasNext()) {
      final Interval cur = intervalIterator.next();
      if (prev != null && cur.equals(prev)) {
        continue;
      }
      prev = cur;
      final TaskLock lock = client.submit(new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, cur));
      if (lock == null) {
        return false;
      }
    }
    return true;
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
        final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(
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
  public static boolean isGuaranteedRollup(IndexIOConfig ioConfig, IndexTuningConfig tuningConfig)
  {
    Preconditions.checkArgument(
        !tuningConfig.isForceGuaranteedRollup() || !ioConfig.isAppendToExisting(),
        "Perfect rollup cannot be guaranteed when appending to existing dataSources"
    );
    return tuningConfig.isForceGuaranteedRollup();
  }

  public static Function<Set<DataSegment>, Set<DataSegment>> compactionStateAnnotateFunction(
      boolean storeCompactionState,
      TaskToolbox toolbox,
      IndexTuningConfig tuningConfig
  )
  {
    if (storeCompactionState) {
      final Map<String, Object> indexSpecMap = tuningConfig.getIndexSpec().asMap(toolbox.getJsonMapper());
      final CompactionState compactionState = new CompactionState(tuningConfig.getPartitionsSpec(), indexSpecMap);
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
   * If the given firehoseFactory is {@link IngestSegmentFirehoseFactory}, then it finds the segments to lock
   * from the firehoseFactory. This is because those segments will be read by this task no matter what segments would be
   * filtered by intervalsToRead, so they need to be locked.
   * <p>
   * However, firehoseFactory is not IngestSegmentFirehoseFactory, it means this task will overwrite some segments
   * with data read from some input source outside of Druid. As a result, only the segments falling in intervalsToRead
   * should be locked.
   * <p>
   * The order of segments within the returned list is unspecified, but each segment is guaranteed to appear in the list
   * only once.
   */
  protected static List<DataSegment> findInputSegments(
      String dataSource,
      TaskActionClient actionClient,
      List<Interval> intervalsToRead,
      FirehoseFactory firehoseFactory
  ) throws IOException
  {
    if (firehoseFactory instanceof IngestSegmentFirehoseFactory) {
      // intervalsToRead is ignored here.
      final List<WindowedSegmentId> inputSegments = ((IngestSegmentFirehoseFactory) firehoseFactory).getSegments();
      if (inputSegments == null) {
        final Interval inputInterval = Preconditions.checkNotNull(
            ((IngestSegmentFirehoseFactory) firehoseFactory).getInterval(),
            "input interval"
        );

        return ImmutableList.copyOf(
            actionClient.submit(
                new RetrieveUsedSegmentsAction(dataSource, inputInterval, null, Segments.ONLY_VISIBLE)
            )
        );
      } else {
        final List<String> inputSegmentIds =
            inputSegments.stream().map(WindowedSegmentId::getSegmentId).collect(Collectors.toList());
        final Collection<DataSegment> dataSegmentsInIntervals = actionClient.submit(
            new RetrieveUsedSegmentsAction(
                dataSource,
                null,
                inputSegments.stream()
                             .flatMap(windowedSegmentId -> windowedSegmentId.getIntervals().stream())
                             .collect(Collectors.toSet()),
                Segments.ONLY_VISIBLE
            )
        );
        return dataSegmentsInIntervals.stream()
                                      .filter(segment -> inputSegmentIds.contains(segment.getId().toString()))
                                      .collect(Collectors.toList());
      }
    } else {
      return ImmutableList.copyOf(
          actionClient.submit(
              new RetrieveUsedSegmentsAction(dataSource, null, intervalsToRead, Segments.ONLY_VISIBLE)
          )
      );
    }
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
}
