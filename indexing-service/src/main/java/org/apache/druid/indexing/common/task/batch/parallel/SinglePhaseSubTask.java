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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SurrogateAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.BatchAppenderators;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.SegmentLockHelper;
import org.apache.druid.indexing.common.task.SegmentLockHelper.OverwritingRootGenerationPartitions;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.RealtimeMetricsMonitor;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.BaseAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedOverwritingShardSpecFactory;
import org.apache.druid.timeline.partition.NumberedShardSpecFactory;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * The worker task of {@link SinglePhaseParallelIndexTaskRunner}. Similar to {@link IndexTask}, but this task
 * generates and pushes segments, and reports them to the {@link SinglePhaseParallelIndexTaskRunner} instead of
 * publishing on its own.
 */
public class SinglePhaseSubTask extends AbstractBatchIndexTask
{
  public static final String TYPE = "single_phase_sub_task";

  private static final Logger LOG = new Logger(SinglePhaseSubTask.class);

  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  private final IndexingServiceClient indexingServiceClient;
  private final IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory;
  private final AppenderatorsManager appenderatorsManager;

  /**
   * If intervals are missing in the granularitySpec, parallel index task runs in "dynamic locking mode".
   * In this mode, sub tasks ask new locks whenever they see a new row which is not covered by existing locks.
   * If this task is overwriting existing segments, then we should know this task is changing segment granularity
   * in advance to know what types of lock we should use. However, if intervals are missing, we can't know
   * the segment granularity of existing segments until the task reads all data because we don't know what segments
   * are going to be overwritten. As a result, we assume that segment granularity is going to be changed if intervals
   * are missing and force to use timeChunk lock.
   *
   * This variable is initialized in the constructor and used in {@link #run} to log that timeChunk lock was enforced
   * in the task logs.
   */
  private final boolean missingIntervalsInOverwriteMode;

  @JsonCreator
  public SinglePhaseSubTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable final String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject IndexingServiceClient indexingServiceClient,
      @JacksonInject IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
      @JacksonInject AppenderatorsManager appenderatorsManager
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
    this.appenderatorsManager = appenderatorsManager;
    this.missingIntervalsInOverwriteMode = !ingestionSchema.getIOConfig().isAppendToExisting()
                                           && !ingestionSchema.getDataSchema()
                                                              .getGranularitySpec()
                                                              .bucketIntervals()
                                                              .isPresent();
    if (missingIntervalsInOverwriteMode) {
      addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
    }
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
  public boolean isReady(TaskActionClient taskActionClient) throws IOException
  {
    return determineLockGranularityAndTryLock(taskActionClient, ingestionSchema.getDataSchema().getGranularitySpec());
  }

  @JsonProperty
  public int getNumAttempts()
  {
    return numAttempts;
  }

  @JsonProperty("spec")
  public ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @JsonProperty
  public String getSupervisorTaskId()
  {
    return supervisorTaskId;
  }

  @Override
  public TaskStatus runTask(final TaskToolbox toolbox) throws Exception
  {
    if (missingIntervalsInOverwriteMode) {
      LOG.warn(
          "Intervals are missing in granularitySpec while this task is potentially overwriting existing segments. "
          + "Forced to use timeChunk lock."
      );
    }
    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

    final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();
    // Firehose temporary directory is automatically removed when this IndexTask completes.
    FileUtils.forceMkdir(firehoseTempDir);

    final ParallelIndexSupervisorTaskClient taskClient = taskClientFactory.build(
        new ClientBasedTaskInfoProvider(indexingServiceClient),
        getId(),
        1, // always use a single http thread
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );
    final Set<DataSegment> pushedSegments = generateAndPushSegments(
        toolbox,
        taskClient,
        firehoseFactory,
        firehoseTempDir
    );

    // Find inputSegments overshadowed by pushedSegments
    final Set<DataSegment> allSegments = new HashSet<>(getSegmentLockHelper().getLockedExistingSegments());
    allSegments.addAll(pushedSegments);
    final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(allSegments);
    final Set<DataSegment> oldSegments = timeline.findFullyOvershadowed()
                                                 .stream()
                                                 .flatMap(holder -> holder.getObject().stream())
                                                 .map(PartitionChunk::getObject)
                                                 .collect(Collectors.toSet());
    taskClient.report(supervisorTaskId, new PushedSegmentsReport(getId(), oldSegments, pushedSegments));

    return TaskStatus.success(getId());
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return !ingestionSchema.getIOConfig().isAppendToExisting();
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return findInputSegments(
        getDataSource(),
        taskActionClient,
        intervals,
        ingestionSchema.getIOConfig().getFirehoseFactory()
    );
  }

  @Override
  public boolean isPerfectRollup()
  {
    return false;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
    if (granularitySpec instanceof ArbitraryGranularitySpec) {
      return null;
    } else {
      return granularitySpec.getSegmentGranularity();
    }
  }

  @VisibleForTesting
  SegmentAllocator createSegmentAllocator(TaskToolbox toolbox, ParallelIndexSupervisorTaskClient taskClient)
  {
    return new WrappingSegmentAllocator(toolbox, taskClient);
  }

  private class WrappingSegmentAllocator implements SegmentAllocator
  {
    private final TaskToolbox toolbox;
    private final ParallelIndexSupervisorTaskClient taskClient;

    private SegmentAllocator internalAllocator;

    private WrappingSegmentAllocator(TaskToolbox toolbox, ParallelIndexSupervisorTaskClient taskClient)
    {
      this.toolbox = toolbox;
      this.taskClient = taskClient;
    }

    @Override
    public SegmentIdWithShardSpec allocate(
        InputRow row,
        String sequenceName,
        String previousSegmentId,
        boolean skipSegmentLineageCheck
    ) throws IOException
    {
      if (internalAllocator == null) {
        internalAllocator = createSegmentAllocator();
      }
      return internalAllocator.allocate(row, sequenceName, previousSegmentId, skipSegmentLineageCheck);
    }

    private SegmentAllocator createSegmentAllocator()
    {
      final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
      final SegmentLockHelper segmentLockHelper = getSegmentLockHelper();
      if (ingestionSchema.getIOConfig().isAppendToExisting() || isUseSegmentLock()) {
        return new ActionBasedSegmentAllocator(
            toolbox.getTaskActionClient(),
            ingestionSchema.getDataSchema(),
            (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> {
              final Interval interval = granularitySpec
                  .bucketInterval(row.getTimestamp())
                  .or(granularitySpec.getSegmentGranularity().bucket(row.getTimestamp()));

              final ShardSpecFactory shardSpecFactory;
              if (segmentLockHelper.hasOverwritingRootGenerationPartition(interval)) {
                final OverwritingRootGenerationPartitions overwritingSegmentMeta = segmentLockHelper
                    .getOverwritingRootGenerationPartition(interval);
                if (overwritingSegmentMeta == null) {
                  throw new ISE("Can't find overwritingSegmentMeta for interval[%s]", interval);
                }
                shardSpecFactory = new NumberedOverwritingShardSpecFactory(
                    overwritingSegmentMeta.getStartRootPartitionId(),
                    overwritingSegmentMeta.getEndRootPartitionId(),
                    overwritingSegmentMeta.getMinorVersionForNewSegments()
                );
              } else {
                shardSpecFactory = NumberedShardSpecFactory.instance();
              }

              return new SurrogateAction<>(
                  supervisorTaskId,
                  new SegmentAllocateAction(
                      schema.getDataSource(),
                      row.getTimestamp(),
                      schema.getGranularitySpec().getQueryGranularity(),
                      schema.getGranularitySpec().getSegmentGranularity(),
                      sequenceName,
                      previousSegmentId,
                      skipSegmentLineageCheck,
                      shardSpecFactory,
                      isUseSegmentLock() ? LockGranularity.SEGMENT : LockGranularity.TIME_CHUNK
                  )
              );
            }
        );
      } else {
        return (row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> taskClient.allocateSegment(
            supervisorTaskId,
            row.getTimestamp()
        );
      }
    }
  }

  /**
   * This method reads input data row by row and adds the read row to a proper segment using {@link BaseAppenderatorDriver}.
   * If there is no segment for the row, a new one is created.  Segments can be published in the middle of reading inputs
   * if one of below conditions are satisfied.
   *
   * <ul>
   * <li>
   * If the number of rows in a segment exceeds {@link DynamicPartitionsSpec#maxRowsPerSegment}
   * </li>
   * <li>
   * If the number of rows added to {@link BaseAppenderatorDriver} so far exceeds {@link DynamicPartitionsSpec#maxTotalRows}
   * </li>
   * </ul>
   *
   * At the end of this method, all the remaining segments are published.
   *
   * @return true if generated segments are successfully published, otherwise false
   */
  private Set<DataSegment> generateAndPushSegments(
      final TaskToolbox toolbox,
      final ParallelIndexSupervisorTaskClient taskClient,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir
  ) throws IOException, InterruptedException
  {
    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final FireDepartment fireDepartmentForMetrics =
        new FireDepartment(dataSchema, new RealtimeIOConfig(null, null), null);
    final FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();

    if (toolbox.getMonitorScheduler() != null) {
      toolbox.getMonitorScheduler().addMonitor(
          new RealtimeMetricsMonitor(
              Collections.singletonList(fireDepartmentForMetrics),
              Collections.singletonMap(DruidMetrics.TASK_ID, new String[]{getId()})
          )
      );
    }

    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final DynamicPartitionsSpec partitionsSpec = (DynamicPartitionsSpec) tuningConfig.getGivenOrDefaultPartitionsSpec();
    final long pushTimeout = tuningConfig.getPushTimeout();
    final boolean explicitIntervals = granularitySpec.bucketIntervals().isPresent();
    final SegmentAllocator segmentAllocator = createSegmentAllocator(toolbox, taskClient);

    final Appenderator appenderator = BatchAppenderators.newAppenderator(
        getId(),
        appenderatorsManager,
        fireDepartmentMetrics,
        toolbox,
        dataSchema,
        tuningConfig,
        getContextValue(Tasks.STORE_COMPACTION_STATE_KEY, Tasks.DEFAULT_STORE_COMPACTION_STATE)
    );
    boolean exceptionOccurred = false;
    try (
        final BatchAppenderatorDriver driver = BatchAppenderators.newDriver(appenderator, toolbox, segmentAllocator);
        final Firehose firehose = firehoseFactory.connect(dataSchema.getParser(), firehoseTempDir)
    ) {
      driver.startJob();

      final Set<DataSegment> pushedSegments = new HashSet<>();

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
            final boolean isPushRequired = addResult.isPushRequired(
                partitionsSpec.getMaxRowsPerSegment(),
                partitionsSpec.getMaxTotalRowsOr(DynamicPartitionsSpec.DEFAULT_MAX_TOTAL_ROWS)
            );
            if (isPushRequired) {
              // There can be some segments waiting for being published even though any rows won't be added to them.
              // If those segments are not published here, the available space in appenderator will be kept to be small
              // which makes the size of segments smaller.
              final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
              pushedSegments.addAll(pushed.getSegments());
              LOG.info("Pushed segments[%s]", pushed.getSegments());
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
      LOG.info("Pushed segments[%s]", pushed.getSegments());
      appenderator.close();

      return pushedSegments;
    }
    catch (TimeoutException | ExecutionException e) {
      exceptionOccurred = true;
      throw new RuntimeException(e);
    }
    catch (Exception e) {
      exceptionOccurred = true;
      throw e;
    }
    finally {
      if (exceptionOccurred) {
        appenderator.closeNow();
      } else {
        appenderator.close();
      }
    }
  }
}
