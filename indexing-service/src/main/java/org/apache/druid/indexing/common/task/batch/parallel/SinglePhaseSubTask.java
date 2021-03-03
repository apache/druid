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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SurrogateTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.BatchAppenderators;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.SegmentAllocators;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.RealtimeMetricsMonitor;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.BaseAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
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

/**
 * The worker task of {@link SinglePhaseParallelIndexTaskRunner}. Similar to {@link IndexTask}, but this task
 * generates and pushes segments, and reports them to the {@link SinglePhaseParallelIndexTaskRunner} instead of
 * publishing on its own.
 */
public class SinglePhaseSubTask extends AbstractBatchIndexTask
{
  public static final String TYPE = "single_phase_sub_task";
  public static final String OLD_TYPE_NAME = "index_sub";

  private static final Logger LOG = new Logger(SinglePhaseSubTask.class);

  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;

  /**
   * If intervals are missing in the granularitySpec, parallel index task runs in "dynamic locking mode".
   * In this mode, sub tasks ask new locks whenever they see a new row which is not covered by existing locks.
   * If this task is overwriting existing segments, then we should know this task is changing segment granularity
   * in advance to know what types of lock we should use. However, if intervals are missing, we can't know
   * the segment granularity of existing segments until the task reads all data because we don't know what segments
   * are going to be overwritten. As a result, we assume that segment granularity is going to be changed if intervals
   * are missing and force to use timeChunk lock.
   * <p>
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
      @JsonProperty("context") final Map<String, Object> context
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
    this.missingIntervalsInOverwriteMode = !ingestionSchema.getIOConfig().isAppendToExisting()
                                           && ingestionSchema.getDataSchema()
                                                             .getGranularitySpec()
                                                             .inputIntervals()
                                                             .isEmpty();
    if (missingIntervalsInOverwriteMode) {
      addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
    }
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws IOException
  {
    return determineLockGranularityAndTryLock(
        new SurrogateTaskActionClient(supervisorTaskId, taskActionClient),
        ingestionSchema.getDataSchema().getGranularitySpec().inputIntervals()
    );
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
    final InputSource inputSource = ingestionSchema.getIOConfig().getNonNullInputSource(
        ingestionSchema.getDataSchema().getParser()
    );

    final ParallelIndexSupervisorTaskClient taskClient = toolbox.getSupervisorTaskClientFactory().build(
        new ClientBasedTaskInfoProvider(toolbox.getIndexingServiceClient()),
        getId(),
        1, // always use a single http thread
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );
    final Set<DataSegment> pushedSegments = generateAndPushSegments(
        toolbox,
        taskClient,
        inputSource,
        toolbox.getIndexingTmpDir()
    );

    // Find inputSegments overshadowed by pushedSegments
    final Set<DataSegment> allSegments = new HashSet<>(getTaskLockHelper().getLockedExistingSegments());
    allSegments.addAll(pushedSegments);
    final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(allSegments);
    final Set<DataSegment> oldSegments = FluentIterable.from(timeline.findFullyOvershadowed())
                                                       .transformAndConcat(TimelineObjectHolder::getObject)
                                                       .transform(PartitionChunk::getObject)
                                                       .toSet();
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
   * <p>
   * At the end of this method, all the remaining segments are published.
   *
   * @return true if generated segments are successfully published, otherwise false
   */
  private Set<DataSegment> generateAndPushSegments(
      final TaskToolbox toolbox,
      final ParallelIndexSupervisorTaskClient taskClient,
      final InputSource inputSource,
      final File tmpDir
  ) throws IOException, InterruptedException
  {
    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final FireDepartment fireDepartmentForMetrics =
        new FireDepartment(dataSchema, new RealtimeIOConfig(null, null), null);
    final FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();

    toolbox.addMonitor(
        new RealtimeMetricsMonitor(
            Collections.singletonList(fireDepartmentForMetrics),
            Collections.singletonMap(DruidMetrics.TASK_ID, new String[]{getId()})
        )
    );

    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final DynamicPartitionsSpec partitionsSpec = (DynamicPartitionsSpec) tuningConfig.getGivenOrDefaultPartitionsSpec();
    final long pushTimeout = tuningConfig.getPushTimeout();
    final boolean explicitIntervals = !granularitySpec.inputIntervals().isEmpty();
    final SegmentAllocator segmentAllocator = SegmentAllocators.forLinearPartitioning(
        toolbox,
        getId(),
        new SupervisorTaskAccess(getSupervisorTaskId(), taskClient),
        getIngestionSchema().getDataSchema(),
        getTaskLockHelper(),
        ingestionSchema.getIOConfig().isAppendToExisting(),
        partitionsSpec
    );

    final RowIngestionMeters rowIngestionMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );
    final Appenderator appenderator = BatchAppenderators.newAppenderator(
        getId(),
        toolbox.getAppenderatorsManager(),
        fireDepartmentMetrics,
        toolbox,
        dataSchema,
        tuningConfig,
        rowIngestionMeters,
        new ParseExceptionHandler(
            rowIngestionMeters,
            tuningConfig.isLogParseExceptions(),
            tuningConfig.getMaxParseExceptions(),
            tuningConfig.getMaxSavedParseExceptions()
        )
    );
    boolean exceptionOccurred = false;
    try (
        final BatchAppenderatorDriver driver = BatchAppenderators.newDriver(appenderator, toolbox, segmentAllocator);
        final CloseableIterator<InputRow> inputRowIterator = AbstractBatchIndexTask.inputSourceReader(
            tmpDir,
            dataSchema,
            inputSource,
            inputSource.needsFormat() ? ParallelIndexSupervisorTask.getInputFormat(ingestionSchema) : null,
            inputRow -> {
              if (inputRow == null) {
                return false;
              }
              if (explicitIntervals) {
                final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
                return optInterval.isPresent();
              }
              return true;
            },
            rowIngestionMeters,
            parseExceptionHandler
        )
    ) {
      driver.startJob();

      final Set<DataSegment> pushedSegments = new HashSet<>();

      while (inputRowIterator.hasNext()) {
        final InputRow inputRow = inputRowIterator.next();

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
            final SegmentsAndCommitMetadata pushed = driver.pushAllAndClear(pushTimeout);
            pushedSegments.addAll(pushed.getSegments());
            LOG.info("Pushed [%s] segments", pushed.getSegments().size());
            LOG.infoSegments(pushed.getSegments(), "Pushed segments");
          }
        } else {
          throw new ISE("Failed to add a row with timestamp[%s]", inputRow.getTimestamp());
        }

        fireDepartmentMetrics.incrementProcessed();
      }

      final SegmentsAndCommitMetadata pushed = driver.pushAllAndClear(pushTimeout);
      pushedSegments.addAll(pushed.getSegments());
      LOG.info("Pushed [%s] segments", pushed.getSegments().size());
      LOG.infoSegments(pushed.getSegments(), "Pushed segments");
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
