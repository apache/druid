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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.indexing.common.task.BatchAppenderators;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.InputSourceProcessor;
import org.apache.druid.indexing.common.task.SegmentAllocatorForBatch;
import org.apache.druid.indexing.common.task.SequenceNameFunction;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.IndexTaskInputRowIteratorBuilder;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.worker.shuffle.ShuffleDataSegmentPusher;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.timeline.DataSegment;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Base class for parallel indexing perfect rollup worker partial segment generate tasks.
 */
abstract class PartialSegmentGenerateTask<T extends GeneratedPartitionsReport> extends PerfectRollupWorkerTask
{
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  private final IndexTaskInputRowIteratorBuilder inputRowIteratorBuilder;

  @MonotonicNonNull
  private RowIngestionMeters buildSegmentsMeters;

  @MonotonicNonNull
  private ParseExceptionHandler parseExceptionHandler;

  PartialSegmentGenerateTask(
      String id,
      String groupId,
      TaskResource taskResource,
      String supervisorTaskId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      IndexTaskInputRowIteratorBuilder inputRowIteratorBuilder
  )
  {
    super(
        id,
        groupId,
        taskResource,
        ingestionSchema.getDataSchema(),
        ingestionSchema.getTuningConfig(),
        context,
        supervisorTaskId
    );

    Preconditions.checkArgument(
        !ingestionSchema.getDataSchema().getGranularitySpec().inputIntervals().isEmpty(),
        "Missing intervals in granularitySpec"
    );
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.inputRowIteratorBuilder = inputRowIteratorBuilder;
  }

  @Override
  public final TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    InputSource inputSource = ingestionSchema.getIOConfig().getNonNullInputSource(toolbox);

    final ParallelIndexSupervisorTaskClient taskClient = toolbox.getSupervisorTaskClientProvider().build(
        supervisorTaskId,
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );

    final List<DataSegment> segments = generateSegments(
        toolbox,
        taskClient,
        inputSource,
        toolbox.getIndexingTmpDir()
    );

    TaskReport.ReportMap taskReport = getTaskCompletionReports(getNumSegmentsRead(inputSource));

    taskClient.report(createGeneratedPartitionsReport(toolbox, segments, taskReport));

    return TaskStatus.success(getId());
  }

  /**
   * @return {@link SegmentAllocator} suitable for the desired segment partitioning strategy.
   */
  abstract SegmentAllocatorForBatch createSegmentAllocator(
      TaskToolbox toolbox,
      ParallelIndexSupervisorTaskClient taskClient
  ) throws IOException;

  /**
   * @return {@link GeneratedPartitionsReport} suitable for the desired segment partitioning strategy.
   */
  abstract T createGeneratedPartitionsReport(
      TaskToolbox toolbox,
      List<DataSegment> segments,
      TaskReport.ReportMap taskReport
  );

  private Long getNumSegmentsRead(InputSource inputSource)
  {
    if (inputSource instanceof DruidInputSource) {
      List<WindowedSegmentId> segments = ((DruidInputSource) inputSource).getSegmentIds();
      if (segments != null) {
        return (long) segments.size();
      }
    }

    return null;
  }

  private List<DataSegment> generateSegments(
      final TaskToolbox toolbox,
      final ParallelIndexSupervisorTaskClient taskClient,
      final InputSource inputSource,
      final File tmpDir
  ) throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema,
        new RealtimeIOConfig(null, null),
        null
    );
    final FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    buildSegmentsMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();

    TaskRealtimeMetricsMonitor metricsMonitor = TaskRealtimeMetricsMonitorBuilder.build(
        this,
        fireDepartmentForMetrics,
        buildSegmentsMeters
    );
    toolbox.addMonitor(metricsMonitor);

    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final PartitionsSpec partitionsSpec = tuningConfig.getGivenOrDefaultPartitionsSpec();
    final long pushTimeout = tuningConfig.getPushTimeout();

    final SegmentAllocatorForBatch segmentAllocator = createSegmentAllocator(toolbox, taskClient);
    final SequenceNameFunction sequenceNameFunction = segmentAllocator.getSequenceNameFunction();

    parseExceptionHandler = new ParseExceptionHandler(
        buildSegmentsMeters,
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );
    final boolean useMaxMemoryEstimates = getContextValue(
        Tasks.USE_MAX_MEMORY_ESTIMATES,
        Tasks.DEFAULT_USE_MAX_MEMORY_ESTIMATES
    );
    final Appenderator appenderator = BatchAppenderators.newAppenderator(
        getId(),
        toolbox.getAppenderatorsManager(),
        fireDepartmentMetrics,
        toolbox,
        dataSchema,
        tuningConfig,
        new ShuffleDataSegmentPusher(supervisorTaskId, getId(), toolbox.getIntermediaryDataManager()),
        buildSegmentsMeters,
        parseExceptionHandler,
        useMaxMemoryEstimates
    );
    boolean exceptionOccurred = false;
    try (final BatchAppenderatorDriver driver = BatchAppenderators.newDriver(appenderator, toolbox, segmentAllocator)) {
      driver.startJob();

      final SegmentsAndCommitMetadata pushed = InputSourceProcessor.process(
          dataSchema,
          driver,
          partitionsSpec,
          inputSource,
          inputSource.needsFormat() ? ParallelIndexSupervisorTask.getInputFormat(ingestionSchema) : null,
          tmpDir,
          sequenceNameFunction,
          inputRowIteratorBuilder,
          buildSegmentsMeters,
          parseExceptionHandler,
          pushTimeout
      );
      return pushed.getSegments();
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
      toolbox.removeMonitor(metricsMonitor);
    }
  }

  /**
   * Generate an IngestionStatsAndErrorsTaskReport for the task.
   */
  private TaskReport.ReportMap getTaskCompletionReports(Long segmentsRead)
  {
    return buildIngestionStatsReport(
        IngestionState.COMPLETED,
        "",
        segmentsRead,
        null
    );
  }

  @Override
  protected Map<String, Object> getTaskCompletionUnparseableEvents()
  {
    Map<String, Object> unparseableEventsMap = new HashMap<>();
    List<ParseExceptionReport> parseExceptionMessages = IndexTaskUtils.getReportListFromSavedParseExceptions(
        parseExceptionHandler.getSavedParseExceptionReports()
    );

    if (parseExceptionMessages != null) {
      unparseableEventsMap.put(RowIngestionMeters.BUILD_SEGMENTS, parseExceptionMessages);
    } else {
      unparseableEventsMap.put(RowIngestionMeters.BUILD_SEGMENTS, ImmutableList.of());
    }

    return unparseableEventsMap;
  }

  @Override
  protected Map<String, Object> getTaskCompletionRowStats()
  {
    return Collections.singletonMap(
        RowIngestionMeters.BUILD_SEGMENTS,
        buildSegmentsMeters.getTotals()
    );
  }
}
