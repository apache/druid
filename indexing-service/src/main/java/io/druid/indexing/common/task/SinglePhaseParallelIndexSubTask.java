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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.appenderator.CountingActionBasedSegmentAllocator;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SurrogateLockListAction;
import io.druid.indexing.common.actions.SurrogateLockTryAcquireAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIOConfig;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.common.task.IndexTask.ShardSpecs;
import io.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.JodaUtils;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.DruidMetrics;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.BaseAppenderatorDriver;
import io.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.codehaus.plexus.util.FileUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A worker task of {@link SinglePhaseParallelIndexSupervisorTask}. Similar to {@link IndexTask}, but this task
 * generates and pushes segments, and reports them to the {@link SinglePhaseParallelIndexSupervisorTask} instead of
 * publishing on its own.
 */
public class SinglePhaseParallelIndexSubTask extends AbstractTask
{
  private static final Logger log = new Logger(SinglePhaseParallelIndexSubTask.class);
  private static final String TYPE = "index_single_phase_sub";

  private final int numAttempts;
  private final SinglePhaseParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  private final IndexingServiceClient indexingServiceClient;
  private final IndexTaskClientFactory<SinglePhaseParallelIndexTaskClient> taskClientFactory;

  @JsonCreator
  public SinglePhaseParallelIndexSubTask(
      // id shouldn't be null except when this task is created by SinglePhaseParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable final String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final SinglePhaseParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject IndexingServiceClient indexingServiceClient,
      @JacksonInject IndexTaskClientFactory<SinglePhaseParallelIndexTaskClient> taskClientFactory
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        ingestionSchema.getDataSchema().getDataSource(),
        context
    );

    if (ingestionSchema.getTuningConfig().isForceGuaranteedRollup()) {
      throw new UnsupportedOperationException("Guaranteed rollup is not supported");
    }

    this.numAttempts = numAttempts;
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.indexingServiceClient = indexingServiceClient;
    this.taskClientFactory = taskClientFactory;
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    final Optional<SortedSet<Interval>> intervals = ingestionSchema.getDataSchema()
                                                                   .getGranularitySpec()
                                                                   .bucketIntervals();

    return !intervals.isPresent() || checkLockAcquired(taskActionClient, intervals.get());
  }

  private boolean checkLockAcquired(TaskActionClient actionClient, SortedSet<Interval> intervals)
  {
    try {
      tryAcquireExclusiveSurrogateLocks(actionClient, intervals);
      return true;
    }
    catch (Exception e) {
      log.error(e, "Failed to acquire locks for intervals[%s]", intervals);
      return false;
    }
  }

  @JsonProperty
  public int getNumAttempts()
  {
    return numAttempts;
  }

  @JsonProperty("spec")
  public SinglePhaseParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @JsonProperty
  public String getSupervisorTaskId()
  {
    return supervisorTaskId;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    final boolean determineIntervals = !ingestionSchema.getDataSchema()
                                                       .getGranularitySpec()
                                                       .bucketIntervals()
                                                       .isPresent();

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

    if (firehoseFactory instanceof IngestSegmentFirehoseFactory) {
      // pass toolbox to Firehose
      ((IngestSegmentFirehoseFactory) firehoseFactory).setTaskToolbox(toolbox);
    }

    final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();
    // Firehose temporary directory is automatically removed when this IndexTask completes.
    FileUtils.forceMkdir(firehoseTempDir);

    final IndexTask.ShardSpecs shardSpecs = determineShardSpecs(firehoseFactory, firehoseTempDir);

    final DataSchema dataSchema;
    final Map<Interval, String> versions;
    if (determineIntervals) {
      final SortedSet<Interval> intervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
      intervals.addAll(shardSpecs.getIntervals());
      final Map<Interval, TaskLock> locks = tryAcquireExclusiveSurrogateLocks(toolbox.getTaskActionClient(), intervals);
      versions = locks.entrySet().stream()
                      .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getVersion()));

      dataSchema = ingestionSchema.getDataSchema().withGranularitySpec(
          ingestionSchema.getDataSchema()
                         .getGranularitySpec()
                         .withIntervals(
                             JodaUtils.condenseIntervals(
                                 shardSpecs.getIntervals()
                             )
                         )
      );
    } else {
      versions = toolbox.getTaskActionClient().submit(new SurrogateLockListAction(supervisorTaskId))
          .stream()
          .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));
      dataSchema = ingestionSchema.getDataSchema();
    }

    final List<DataSegment> pushedSegments = generateAndPushSegments(
        toolbox,
        dataSchema,
        shardSpecs,
        versions,
        firehoseFactory,
        firehoseTempDir
    );
    final SinglePhaseParallelIndexTaskClient taskClient = taskClientFactory.build(
        new ClientBasedTaskInfoProvider(indexingServiceClient),
        getId(),
        1, // always use a single http thread
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );
    taskClient.report(supervisorTaskId, pushedSegments);

    return TaskStatus.success(getId());
  }

  private Map<Interval, TaskLock> tryAcquireExclusiveSurrogateLocks(
      TaskActionClient client,
      SortedSet<Interval> intervals
  )
      throws IOException
  {
    final Map<Interval, TaskLock> lockMap = new HashMap<>();
    for (Interval interval : Tasks.computeCompactIntervals(intervals)) {
      final TaskLock lock = Preconditions.checkNotNull(
          client.submit(new SurrogateLockTryAcquireAction(TaskLockType.EXCLUSIVE, interval, supervisorTaskId)),
          "Cannot acquire a lock for interval[%s]", interval
      );
      lockMap.put(interval, lock);
    }
    return lockMap;
  }

  /**
   * Determines intervals and shardSpecs for input data.  This method first checks that it must determine intervals and
   * shardSpecs by itself.  Intervals must be determined if they are not specified in {@link GranularitySpec}.
   * ShardSpecs must be determined if the perfect rollup must be guaranteed even though the number of shards is not
   * specified in {@link IndexTask.IndexTuningConfig}.
   * <P/>
   * If both intervals and shardSpecs don't have to be determined, this method simply returns {@link IndexTask.ShardSpecs} for the
   * given intervals.  Here, if {@link IndexTask.IndexTuningConfig#numShards} is not specified, {@link NumberedShardSpec} is used.
   * <p/>
   * If one of intervals or shardSpecs need to be determined, this method reads the entire input for determining one of
   * them.  If the perfect rollup must be guaranteed, {@link HashBasedNumberedShardSpec} is used for hash partitioning
   * of input data.  In the future we may want to also support single-dimension partitioning.
   *
   * @return generated {@link IndexTask.ShardSpecs} representing a map of intervals and corresponding shard specs
   */
  private IndexTask.ShardSpecs determineShardSpecs(
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir
  ) throws IOException
  {
    final IndexTask.IndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();

    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();

    // Must determine intervals if unknown, since we acquire all locks before processing any data.
    final boolean determineIntervals = !granularitySpec.bucketIntervals().isPresent();

    // Must determine partitions if # of shards is not provided.
    final boolean determineNumPartitions = tuningConfig.getNumShards() == null;

    // if we were given number of shards per interval and the intervals, we don't need to scan the data
    if (!determineNumPartitions && !determineIntervals) {
      log.info("Skipping determine partition scan");
      return createShardSpecWithoutInputScan(granularitySpec);
    } else {
      // determine intervals containing data
      return createShardSpecsFromInput(
          ingestionSchema,
          firehoseFactory,
          firehoseTempDir,
          granularitySpec,
          determineIntervals
      );
    }
  }

  private static IndexTask.ShardSpecs createShardSpecWithoutInputScan(GranularitySpec granularitySpec)
  {
    final Map<Interval, List<ShardSpec>> shardSpecs = new HashMap<>();
    final SortedSet<Interval> intervals = granularitySpec.bucketIntervals().get();

    for (Interval interval : intervals) {
      shardSpecs.put(interval, ImmutableList.of());
    }

    return new IndexTask.ShardSpecs(shardSpecs);
  }

  private static IndexTask.ShardSpecs createShardSpecsFromInput(
      SinglePhaseParallelIndexIngestionSpec ingestionSchema,
      FirehoseFactory firehoseFactory,
      File firehoseTempDir,
      GranularitySpec granularitySpec,
      boolean determineIntervals
  ) throws IOException
  {
    log.info("Determining intervals");
    long determineShardSpecsStartMillis = System.currentTimeMillis();

    final List<Interval> intervals = collectIntervalsAndShardSpecs(
        ingestionSchema,
        firehoseFactory,
        firehoseTempDir,
        granularitySpec,
        determineIntervals
    );

    final Map<Interval, List<ShardSpec>> intervalToShardSpecs = intervals
        .stream()
        .collect(Collectors.toMap(Function.identity(), i -> ImmutableList.of()));
    log.info("Found intervals and shardSpecs in %,dms", System.currentTimeMillis() - determineShardSpecsStartMillis);

    return new IndexTask.ShardSpecs(intervalToShardSpecs);
  }

  private static List<Interval> collectIntervalsAndShardSpecs(
      SinglePhaseParallelIndexIngestionSpec ingestionSchema,
      FirehoseFactory firehoseFactory,
      File firehoseTempDir,
      GranularitySpec granularitySpec,
      boolean determineIntervals
  ) throws IOException
  {
    final List<Interval> intervals = new ArrayList<>();
    int thrownAway = 0;
    int unparseable = 0;

    try (
        final Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser(), firehoseTempDir)
    ) {
      while (firehose.hasMore()) {
        try {
          final InputRow inputRow = firehose.nextRow();

          // The null inputRow means the caller must skip this row.
          if (inputRow == null) {
            continue;
          }

          final Interval interval;
          if (determineIntervals) {
            interval = granularitySpec.getSegmentGranularity().bucket(inputRow.getTimestamp());
          } else {
            final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
            if (!optInterval.isPresent()) {
              thrownAway++;
              continue;
            }
            interval = optInterval.get();
          }

          if (!intervals.contains(interval)) {
            intervals.add(interval);
          }
        }
        catch (ParseException e) {
          if (ingestionSchema.getTuningConfig().isReportParseExceptions()) {
            throw e;
          } else {
            unparseable++;
          }
        }
      }
    }

    // These metrics are reported in generateAndPushSegments()
    if (thrownAway > 0) {
      log.warn("Unable to find a matching interval for [%,d] events", thrownAway);
    }
    if (unparseable > 0) {
      log.warn("Unable to parse [%,d] events", unparseable);
    }

    intervals.sort(Comparators.intervalsByStartThenEnd());
    return intervals;
  }

  /**
   * This method reads input data row by row and adds the read row to a proper segment using {@link BaseAppenderatorDriver}.
   * If there is no segment for the row, a new one is created.  Segments can be published in the middle of reading inputs
   * if one of below conditions are satisfied.
   *
   * <ul>
   * <li>
   * If the number of rows in a segment exceeds {@link IndexTask.IndexTuningConfig#targetPartitionSize}
   * </li>
   * <li>
   * If the number of rows added to {@link BaseAppenderatorDriver} so far exceeds {@link IndexTask.IndexTuningConfig#maxTotalRows}
   * </li>
   * </ul>
   *
   * At the end of this method, all the remaining segments are published.
   *
   * @return true if generated segments are successfully published, otherwise false
   */
  private List<DataSegment> generateAndPushSegments(
      final TaskToolbox toolbox,
      final DataSchema dataSchema,
      final ShardSpecs shardSpecs,
      final Map<Interval, String> versions,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir
  ) throws IOException, InterruptedException
  {
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema, new RealtimeIOConfig(null, null, null), null
    );
    final FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();

    if (toolbox.getMonitorScheduler() != null) {
      toolbox.getMonitorScheduler().addMonitor(
          new RealtimeMetricsMonitor(
              ImmutableList.of(fireDepartmentForMetrics),
              ImmutableMap.of(DruidMetrics.TASK_ID, new String[]{getId()})
          )
      );
    }

    final IndexIOConfig ioConfig = ingestionSchema.getIOConfig();
    final IndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final long pushTimeout = tuningConfig.getPushTimeout();

    final SegmentAllocator segmentAllocator;
    if (ioConfig.isAppendToExisting()) {
      segmentAllocator = new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), dataSchema);
    } else {
      segmentAllocator = new CountingActionBasedSegmentAllocator(
          toolbox.getTaskActionClient(),
          getDataSource(),
          granularitySpec,
          shardSpecs.getMap(),
          versions
      );
    }

    try (
        final Appenderator appenderator = newAppenderator(fireDepartmentMetrics, toolbox, dataSchema, tuningConfig);
        final BatchAppenderatorDriver driver = newDriver(appenderator, toolbox, segmentAllocator);
        final Firehose firehose = firehoseFactory.connect(dataSchema.getParser(), firehoseTempDir)
    ) {
      driver.startJob();

      final List<DataSegment> pushedSegments = new ArrayList<>();

      while (firehose.hasMore()) {
        try {
          final InputRow inputRow = firehose.nextRow();

          if (inputRow == null) {
            fireDepartmentMetrics.incrementThrownAway();
            continue;
          }

          final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
          if (!optInterval.isPresent()) {
            fireDepartmentMetrics.incrementThrownAway();
            continue;
          }

          // Segments are created as needed, using a single sequence name. They may be allocated from the overlord
          // (in append mode) or may be created on our own authority (in overwrite mode).
          final String sequenceName = getId();
          final AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName);

          if (addResult.isOk()) {
            if (exceedMaxRowsInSegment(addResult.getNumRowsInSegment(), tuningConfig) ||
                exceedMaxRowsInAppenderator(addResult.getTotalNumRowsInAppenderator(), tuningConfig)) {
              // There can be some segments waiting for being published even though any rows won't be added to them.
              // If those segments are not published here, the available space in appenderator will be kept to be small
              // which makes the size of segments smaller.
              final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
              pushedSegments.addAll(pushed.getSegments());
              log.info("Pushed segments[%s]", pushed.getSegments());
            }
          } else {
            throw new ISE("Failed to add a row with timestamp[%s]", inputRow.getTimestamp());
          }

          fireDepartmentMetrics.incrementProcessed();
        }
        catch (ParseException e) {
          if (tuningConfig.isReportParseExceptions()) {
            throw e;
          } else {
            fireDepartmentMetrics.incrementUnparseable();
          }
        }
      }

      final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
      pushedSegments.addAll(pushed.getSegments());
      log.info("Pushed segments[%s]", pushed.getSegments());

      return pushedSegments;
    }
    catch (TimeoutException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private static boolean exceedMaxRowsInSegment(int numRowsInSegment, IndexTuningConfig indexTuningConfig)
  {
    // maxRowsInSegment should be null if numShards is set in indexTuningConfig
    final Integer maxRowsInSegment = indexTuningConfig.getTargetPartitionSize();
    return maxRowsInSegment != null && maxRowsInSegment <= numRowsInSegment;
  }

  private static boolean exceedMaxRowsInAppenderator(long numRowsInAppenderator, IndexTuningConfig indexTuningConfig)
  {
    // maxRowsInAppenderator should be null if numShards is set in indexTuningConfig
    final Long maxRowsInAppenderator = indexTuningConfig.getMaxTotalRows();
    return maxRowsInAppenderator != null && maxRowsInAppenderator <= numRowsInAppenderator;
  }

  private static Appenderator newAppenderator(
      FireDepartmentMetrics metrics,
      TaskToolbox toolbox,
      DataSchema dataSchema,
      IndexTuningConfig tuningConfig
  )
  {
    return Appenderators.createOffline(
        dataSchema,
        tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMergerV9()
    );
  }

  private static BatchAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final SegmentAllocator segmentAllocator
  )
  {
    return new BatchAppenderatorDriver(
        appenderator,
        segmentAllocator,
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient())
    );
  }
}
