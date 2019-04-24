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
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentListUsedAction;
import org.apache.druid.indexing.common.actions.SurrogateAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.RealtimeMetricsMonitor;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.BaseAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedOverwritingShardSpecFactory;
import org.apache.druid.timeline.partition.NumberedShardSpecFactory;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A worker task of {@link ParallelIndexSupervisorTask}. Similar to {@link IndexTask}, but this task
 * generates and pushes segments, and reports them to the {@link ParallelIndexSupervisorTask} instead of
 * publishing on its own.
 */
public class ParallelIndexSubTask extends AbstractTask
{
  public static final String TYPE = "index_sub";

  private static final Logger log = new Logger(ParallelIndexSubTask.class);

  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  private final IndexingServiceClient indexingServiceClient;
  private final IndexTaskClientFactory<ParallelIndexTaskClient> taskClientFactory;

  @JsonCreator
  public ParallelIndexSubTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable final String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject IndexingServiceClient indexingServiceClient,
      @JacksonInject IndexTaskClientFactory<ParallelIndexTaskClient> taskClientFactory
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
      return tryLockWithIntervals(actionClient, new ArrayList<>(intervals));
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
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

    final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();
    // Firehose temporary directory is automatically removed when this IndexTask completes.
    FileUtils.forceMkdir(firehoseTempDir);

    final ParallelIndexTaskClient taskClient = taskClientFactory.build(
        new ClientBasedTaskInfoProvider(indexingServiceClient),
        getId(),
        1, // always use a single http thread
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );
    final List<DataSegment> pushedSegments = generateAndPushSegments(
        toolbox,
        taskClient,
        firehoseFactory,
        firehoseTempDir
    );
    taskClient.report(supervisorTaskId, pushedSegments);

    return TaskStatus.success(getId());
  }

  @Override
  public boolean requireLockInputSegments()
  {
    return !ingestionSchema.getIOConfig().isAppendToExisting();
  }

  @Override
  public List<DataSegment> findInputSegments(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return taskActionClient.submit(new SegmentListUsedAction(getDataSource(), null, intervals));
  }

  @Override
  public boolean changeSegmentGranularity(List<Interval> intervalOfExistingSegments)
  {
    final Granularity segmentGranularity = ingestionSchema.getDataSchema().getGranularitySpec().getSegmentGranularity();
    return intervalOfExistingSegments.stream().anyMatch(interval -> !segmentGranularity.match(interval));
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity(Interval interval)
  {
    return ingestionSchema.getDataSchema().getGranularitySpec().getSegmentGranularity();
  }

  @VisibleForTesting
  SegmentAllocator createSegmentAllocator(
      TaskToolbox toolbox,
      ParallelIndexTaskClient taskClient
  )
  {
    return new WrappingSegmentAllocator(toolbox, taskClient);
  }

  private class WrappingSegmentAllocator implements SegmentAllocator
  {
    private final TaskToolbox toolbox;
    private final ParallelIndexTaskClient taskClient;

    /**
     * This internalAllocator is initialized lazily to make sure that {@link #isChangeSegmentGranularity()} is called
     * after the lock is properly acquired. Note that locks can be acquired after the task is started if the interval is
     * not specified in {@link GranularitySpec}.
     */
    private SegmentAllocator internalAllocator;

    private WrappingSegmentAllocator(
        TaskToolbox toolbox,
        ParallelIndexTaskClient taskClient
    )
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
      // TODO: what if intervals are missing?
      if (ingestionSchema.getIOConfig().isAppendToExisting() || !isChangeSegmentGranularity()) {
        return new ActionBasedSegmentAllocator(
            toolbox.getTaskActionClient(),
            ingestionSchema.getDataSchema(),
            (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> {
              final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
              final Interval interval = granularitySpec
                  .bucketInterval(row.getTimestamp())
                  .or(granularitySpec.getSegmentGranularity().bucket(row.getTimestamp()));
              final ShardSpecFactory shardSpecFactory;
              if (isOverwriteMode() && !isChangeSegmentGranularity()) {
                final OverwritingSegmentMeta overwritingSegmentMeta = getOverwritingSegmentMeta(interval);
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
                      shardSpecFactory
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
   * If the number of rows in a segment exceeds {@link ParallelIndexTuningConfig#maxRowsPerSegment}
   * </li>
   * <li>
   * If the number of rows added to {@link BaseAppenderatorDriver} so far exceeds {@link ParallelIndexTuningConfig#maxTotalRows}
   * </li>
   * </ul>
   *
   * At the end of this method, all the remaining segments are published.
   *
   * @return true if generated segments are successfully published, otherwise false
   */
  private List<DataSegment> generateAndPushSegments(
      final TaskToolbox toolbox,
      final ParallelIndexTaskClient taskClient,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir
  ) throws IOException, InterruptedException
  {
    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final FireDepartment fireDepartmentForMetrics =
        new FireDepartment(dataSchema, new RealtimeIOConfig(null, null, null), null);
    final FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();

    if (toolbox.getMonitorScheduler() != null) {
      toolbox.getMonitorScheduler().addMonitor(
          new RealtimeMetricsMonitor(
              Collections.singletonList(fireDepartmentForMetrics),
              Collections.singletonMap(DruidMetrics.TASK_ID, new String[]{getId()})
          )
      );
    }

    // Initialize maxRowsPerSegment and maxTotalRows lazily
    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    @Nullable final Integer maxRowsPerSegment = IndexTask.getValidMaxRowsPerSegment(tuningConfig);
    @Nullable final Long maxTotalRows = IndexTask.getValidMaxTotalRows(tuningConfig);
    final long pushTimeout = tuningConfig.getPushTimeout();
    final boolean explicitIntervals = granularitySpec.bucketIntervals().isPresent();
    final SegmentAllocator segmentAllocator = createSegmentAllocator(toolbox, taskClient);

    try (
        final Appenderator appenderator = newAppenderator(fireDepartmentMetrics, toolbox, dataSchema, tuningConfig);
        final BatchAppenderatorDriver driver = newDriver(appenderator, toolbox, segmentAllocator);
        final Firehose firehose = firehoseFactory.connect(dataSchema.getParser(), firehoseTempDir)
    ) {
      driver.startJob(null);

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
          } else {
            final Granularity segmentGranularity = findSegmentGranularity(granularitySpec);
            final Interval timeChunk = segmentGranularity.bucket(inputRow.getTimestamp());
            if (!tryLockWithIntervals(toolbox.getTaskActionClient(), Collections.singletonList(timeChunk))) {
              throw new ISE("Failed to get locks for interval[%s]", timeChunk);
            }
          }

          // Segments are created as needed, using a single sequence name. They may be allocated from the overlord
          // (in append mode) or may be created on our own authority (in overwrite mode).
          final String sequenceName = getId();
          final AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName);

          if (addResult.isOk()) {
            if (addResult.isPushRequired(maxRowsPerSegment, maxTotalRows)) {
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

  private static Granularity findSegmentGranularity(GranularitySpec granularitySpec)
  {
    if (granularitySpec instanceof UniformGranularitySpec) {
      return granularitySpec.getSegmentGranularity();
    } else {
      return Granularities.ALL;
    }
  }

  private static Appenderator newAppenderator(
      FireDepartmentMetrics metrics,
      TaskToolbox toolbox,
      DataSchema dataSchema,
      ParallelIndexTuningConfig tuningConfig
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
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller()
    );
  }
}
