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
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.SurrogateAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIOConfig;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
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
import org.codehaus.plexus.util.FileUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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
    final boolean explicitIntervals = ingestionSchema.getDataSchema()
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

    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    final Map<Interval, String> versions;

    if (explicitIntervals) {
      versions = toolbox.getTaskActionClient().submit(new SurrogateAction<>(supervisorTaskId, new LockListAction()))
                        .stream()
                        .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));
    } else {
      versions = null;
    }

    final List<DataSegment> pushedSegments = generateAndPushSegments(
        toolbox,
        dataSchema,
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
          client.submit(
              new SurrogateAction<>(supervisorTaskId, new LockTryAcquireAction(TaskLockType.EXCLUSIVE, interval))
          ),
          "Cannot acquire a lock for interval[%s]", interval
      );
      lockMap.put(interval, lock);
    }
    return lockMap;
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
      @Nullable final Map<Interval, String> versions,
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
    final boolean explicitIntervals = granularitySpec.bucketIntervals().isPresent();

    final SegmentAllocator segmentAllocator;
    if (ioConfig.isAppendToExisting() || !explicitIntervals) {
      segmentAllocator = new ActionBasedSegmentAllocator(
          toolbox.getTaskActionClient(),
          dataSchema,
          (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> new SurrogateAction<>(
              supervisorTaskId,
              new SegmentAllocateAction(
                  schema.getDataSource(),
                  row.getTimestamp(),
                  schema.getGranularitySpec().getQueryGranularity(),
                  schema.getGranularitySpec().getSegmentGranularity(),
                  sequenceName,
                  previousSegmentId,
                  skipSegmentLineageCheck
              )
          )
      );
    } else {
      segmentAllocator = new CountingActionBasedSegmentAllocator(
          toolbox.getTaskActionClient(),
          getDataSource(),
          granularitySpec,
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

          if (!Intervals.ETERNITY.contains(inputRow.getTimestamp())) {
            final String errorMsg = StringUtils.format(
                "Encountered row with timestamp that cannot be represented as a long: [%s]",
                inputRow
            );
            throw new ParseException(errorMsg);
          }

          if (explicitIntervals) {
            final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
            if (!optInterval.isPresent()) {
              fireDepartmentMetrics.incrementThrownAway();
              continue;
            }
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
      throw new RuntimeException(e);
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
