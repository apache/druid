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

import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.BatchAppenderators;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.InputSourceProcessor;
import org.apache.druid.indexing.common.task.SegmentAllocatorForBatch;
import org.apache.druid.indexing.common.task.SequenceNameFunction;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.IndexTaskInputRowIteratorBuilder;
import org.apache.druid.indexing.worker.shuffle.ShuffleDataSegmentPusher;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.RealtimeMetricsMonitor;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
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
        context
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

    final List<DataSegment> segments = generateSegments(
        toolbox,
        taskClient,
        inputSource,
        toolbox.getIndexingTmpDir()
    );
    taskClient.report(supervisorTaskId, createGeneratedPartitionsReport(toolbox, segments));

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
      List<DataSegment> segments
  );

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
    final RowIngestionMeters buildSegmentsMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();

    toolbox.addMonitor(
        new RealtimeMetricsMonitor(
            Collections.singletonList(fireDepartmentForMetrics),
            Collections.singletonMap(DruidMetrics.TASK_ID, new String[]{getId()})
        )
    );

    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final PartitionsSpec partitionsSpec = tuningConfig.getGivenOrDefaultPartitionsSpec();
    final long pushTimeout = tuningConfig.getPushTimeout();

    final SegmentAllocatorForBatch segmentAllocator = createSegmentAllocator(toolbox, taskClient);
    final SequenceNameFunction sequenceNameFunction = segmentAllocator.getSequenceNameFunction();

    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        buildSegmentsMeters,
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
        new ShuffleDataSegmentPusher(supervisorTaskId, getId(), toolbox.getIntermediaryDataManager()),
        buildSegmentsMeters,
        parseExceptionHandler
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
    }
  }
}
