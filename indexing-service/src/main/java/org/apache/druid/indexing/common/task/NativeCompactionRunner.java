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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.input.WindowedSegmentId;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NativeCompactionRunner implements CompactionRunner
{
  public static final String TYPE = "native";

  private static final Logger log = new Logger(NativeCompactionRunner.class);
  private static final boolean STORE_COMPACTION_STATE = true;

  @JsonIgnore
  private final SegmentCacheManagerFactory segmentCacheManagerFactory;

  @JsonIgnore
  private final CurrentSubTaskHolder currentSubTaskHolder = new CurrentSubTaskHolder(
      (taskObject, config) -> {
        final ParallelIndexSupervisorTask indexTask = (ParallelIndexSupervisorTask) taskObject;
        indexTask.stopGracefully(config);
      });

  @JsonCreator
  public NativeCompactionRunner(@JacksonInject SegmentCacheManagerFactory segmentCacheManagerFactory)
  {
    this.segmentCacheManagerFactory = segmentCacheManagerFactory;
  }

  @Override
  public CurrentSubTaskHolder getCurrentSubTaskHolder()
  {
    return currentSubTaskHolder;
  }

  @Override
  public CompactionConfigValidationResult validateCompactionTask(
      CompactionTask compactionTask,
      Map<Interval, DataSchema> intervalDataSchemaMap
  )
  {
    // Virtual columns in filter rules are not supported by native compaction
    if (compactionTask.getTransformSpec() != null
        && compactionTask.getTransformSpec().getVirtualColumns() != null
        && compactionTask.getTransformSpec().getVirtualColumns().getVirtualColumns().length > 0) {
      return CompactionConfigValidationResult.failure(
          "Virtual columns in filter rules are not supported by the Native compaction engine. Use MSQ compaction engine instead."
      );
    }
    return CompactionConfigValidationResult.success();
  }

  /**
   * Generate {@link ParallelIndexIngestionSpec} from input dataschemas.
   *
   * @return an empty list if input segments don't exist. Otherwise, a generated ingestionSpec.
   */
  @VisibleForTesting
  static List<ParallelIndexIngestionSpec> createIngestionSpecs(
      Map<Interval, DataSchema> intervalDataSchemaMap,
      final TaskToolbox toolbox,
      final CompactionIOConfig ioConfig,
      final PartitionConfigurationManager partitionConfigurationManager,
      final CoordinatorClient coordinatorClient,
      final SegmentCacheManagerFactory segmentCacheManagerFactory
  )
  {
    final CompactionTask.CompactionTuningConfig compactionTuningConfig = partitionConfigurationManager.computeTuningConfig();

    return intervalDataSchemaMap.entrySet().stream().map((dataSchema) -> new ParallelIndexIngestionSpec(
                                        dataSchema.getValue(),
                                        createIoConfig(
                                            toolbox,
                                            dataSchema.getValue(),
                                            dataSchema.getKey(),
                                            coordinatorClient,
                                            segmentCacheManagerFactory,
                                            ioConfig
                                        ),
                                        compactionTuningConfig
                                    )

    ).collect(Collectors.toList());
  }

  /**
   * When using {@link SpecificSegmentsSpec}, resolves specific segment IDs that belong to the given interval
   * and returns them as {@link WindowedSegmentId} objects. Returns null for interval-based compaction.
   */
  @Nullable
  private static List<WindowedSegmentId> resolveSegmentIdsForInterval(
      CompactionInputSpec inputSpec,
      String dataSource,
      Interval interval
  )
  {
    if (!(inputSpec instanceof SpecificSegmentsSpec)) {
      return null;
    }
    SpecificSegmentsSpec spec = (SpecificSegmentsSpec) inputSpec;
    List<WindowedSegmentId> segmentIds = new ArrayList<>();
    for (String segmentIdStr : spec.getSegments()) {
      SegmentId segmentId = SegmentId.tryParse(dataSource, segmentIdStr);
      if (segmentId != null && interval.contains(segmentId.getInterval())) {
        segmentIds.add(new WindowedSegmentId(
            segmentIdStr,
            Collections.singletonList(segmentId.getInterval())
        ));
      }
    }
    return segmentIds.isEmpty() ? null : segmentIds;
  }

  private String createIndexTaskSpecId(String taskId, int i)
  {
    return StringUtils.format("%s_%d", taskId, i);
  }


  private static ParallelIndexIOConfig createIoConfig(
      TaskToolbox toolbox,
      DataSchema dataSchema,
      Interval interval,
      CoordinatorClient coordinatorClient,
      SegmentCacheManagerFactory segmentCacheManagerFactory,
      CompactionIOConfig compactionIOConfig
  )
  {
    // Resolve specific segment IDs for minor compaction if using SpecificSegmentsSpec
    final List<WindowedSegmentId> segmentIds = resolveSegmentIdsForInterval(
        compactionIOConfig.getInputSpec(),
        dataSchema.getDataSource(),
        interval
    );

    final Interval inputInterval;
    if (segmentIds != null && !segmentIds.isEmpty()) {
      // When compacting specific segments, use segment IDs instead of interval
      inputInterval = null;
    } else {
      inputInterval = interval;
    }

    if (inputInterval != null && !compactionIOConfig.isAllowNonAlignedInterval()) {
      // Validate interval alignment only when using interval-based input (not segment-ID mode).
      final Granularity segmentGranularity = dataSchema.getGranularitySpec().getSegmentGranularity();
      final Interval widenedInterval = Intervals.utc(
          segmentGranularity.bucketStart(inputInterval.getStart()).getMillis(),
          segmentGranularity.bucketEnd(inputInterval.getEnd().minus(1)).getMillis()
      );

      if (!inputInterval.equals(widenedInterval)) {
        throw new IAE(
            "Interval[%s] to compact is not aligned with segmentGranularity[%s]",
            inputInterval,
            segmentGranularity
        );
      }
    }

    return new ParallelIndexIOConfig(
        new DruidInputSource(
            dataSchema.getDataSource(),
            inputInterval,
            segmentIds,
            null,
            null,
            null,
            toolbox.getIndexIO(),
            coordinatorClient,
            segmentCacheManagerFactory,
            toolbox.getConfig()
        ).withTaskToolbox(toolbox),
        null,
        false,
        compactionIOConfig.isDropExisting()
    );
  }

  @Override
  public TaskStatus runCompactionTasks(
      CompactionTask compactionTask,
      Map<Interval, DataSchema> intervalDataSchemaMap,
      TaskToolbox taskToolbox
  ) throws Exception
  {
    final PartitionConfigurationManager partitionConfigurationManager =
        new NativeCompactionRunner.PartitionConfigurationManager(compactionTask.getTuningConfig());

    final List<ParallelIndexIngestionSpec> ingestionSpecs = createIngestionSpecs(
        intervalDataSchemaMap,
        taskToolbox,
        compactionTask.getIoConfig(),
        partitionConfigurationManager,
        taskToolbox.getCoordinatorClient(),
        segmentCacheManagerFactory
    );

    List<ParallelIndexSupervisorTask> subtasks = IntStream
        .range(0, ingestionSpecs.size())
        .mapToObj(i -> {
          // The ID of SubtaskSpecs is used as the base sequenceName in segment allocation protocol.
          // The indexing tasks generated by the compaction task should use different sequenceNames
          // so that they can allocate valid segment IDs with no duplication.
          ParallelIndexIngestionSpec ingestionSpec = ingestionSpecs.get(i);
          final String baseSequenceName = createIndexTaskSpecId(compactionTask.getId(), i);
          return newTask(compactionTask, baseSequenceName, ingestionSpec);
        })
        .collect(Collectors.toList());

    if (subtasks.isEmpty()) {
      String msg = StringUtils.format(
          "Can't find segments from inputSpec[%s], nothing to do.",
          compactionTask.getIoConfig().getInputSpec()
      );
      log.warn(msg);
      return TaskStatus.failure(compactionTask.getId(), msg);
    }
    return runParallelIndexSubtasks(
        subtasks,
        taskToolbox,
        currentSubTaskHolder,
        compactionTask.getId()
    );
  }

  private TaskStatus runParallelIndexSubtasks(
      List<ParallelIndexSupervisorTask> tasks,
      TaskToolbox toolbox,
      CurrentSubTaskHolder currentSubTaskHolder,
      String compactionTaskId
  )
      throws JsonProcessingException
  {
    final int totalNumSpecs = tasks.size();
    log.info("Generated [%d] compaction task specs", totalNumSpecs);

    int failCnt = 0;
    final TaskReport.ReportMap completionReports = new TaskReport.ReportMap();
    for (int i = 0; i < tasks.size(); i++) {
      ParallelIndexSupervisorTask eachSpec = tasks.get(i);
      final String json = toolbox.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(eachSpec);
      if (!currentSubTaskHolder.setTask(eachSpec)) {
        String errMsg = "Task was asked to stop. Finish as failed.";
        log.info(errMsg);
        return TaskStatus.failure(compactionTaskId, errMsg);
      }
      try {
        if (eachSpec.isReady(toolbox.getTaskActionClient())) {
          log.info("Running indexSpec: " + json);
          final TaskStatus eachResult = eachSpec.run(toolbox);
          if (!eachResult.isSuccess()) {
            failCnt++;
            log.warn("Failed to run indexSpec: [%s].\nTrying the next indexSpec.", json);
          }

          String reportKeySuffix = "_" + i;
          Optional.ofNullable(eachSpec.getCompletionReports())
                  .ifPresent(reports -> completionReports.putAll(
                      CollectionUtils.mapKeys(reports, key -> key + reportKeySuffix)));
        } else {
          failCnt++;
          log.warn("indexSpec is not ready: [%s].\nTrying the next indexSpec.", json);
        }
      }
      catch (Exception e) {
        failCnt++;
        log.warn(e, "Failed to run indexSpec: [%s].\nTrying the next indexSpec.", json);
      }
    }

    String msg = StringUtils.format(
        "Ran [%d] specs, [%d] succeeded, [%d] failed",
        totalNumSpecs,
        totalNumSpecs - failCnt,
        failCnt
    );

    toolbox.getTaskReportFileWriter().write(compactionTaskId, completionReports);
    log.info(msg);
    return failCnt == 0 ? TaskStatus.success(compactionTaskId) : TaskStatus.failure(compactionTaskId, msg);
  }

  private ParallelIndexSupervisorTask newTask(
      CompactionTask compactionTask,
      String baseSequenceName,
      ParallelIndexIngestionSpec ingestionSpec
  )
  {
    return new ParallelIndexSupervisorTask(
        compactionTask.getId(),
        compactionTask.getGroupId(),
        compactionTask.getTaskResource(),
        ingestionSpec,
        baseSequenceName,
        createContextForSubtask(compactionTask),
        true
    );
  }

  Map<String, Object> createContextForSubtask(CompactionTask compactionTask)
  {
    final Map<String, Object> newContext = new HashMap<>(compactionTask.getContext());
    newContext.put(CompactionTask.CTX_KEY_APPENDERATOR_TRACKING_TASK_ID, compactionTask.getId());
    newContext.putIfAbsent(CompactSegments.STORE_COMPACTION_STATE_KEY, STORE_COMPACTION_STATE);
    // Set the priority of the compaction task.
    newContext.put(Tasks.PRIORITY_KEY, compactionTask.getPriority());

    // Pass specific segment IDs to sub-tasks when using SpecificSegmentsSpec.
    // This ensures sub-tasks lock only the specified segments, not all segments in the interval.
    if (compactionTask.getIoConfig().getInputSpec() instanceof SpecificSegmentsSpec) {
      SpecificSegmentsSpec specificSpec = (SpecificSegmentsSpec) compactionTask.getIoConfig().getInputSpec();
      newContext.put(CompactionTask.CTX_KEY_SPECIFIC_SEGMENTS_TO_COMPACT, specificSpec.getSegments());
    }
    return newContext;
  }

  @VisibleForTesting
  static class PartitionConfigurationManager
  {
    private final CompactionTask.CompactionTuningConfig tuningConfig;

    PartitionConfigurationManager(@Nullable CompactionTask.CompactionTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
    }

    @Nullable
    CompactionTask.CompactionTuningConfig computeTuningConfig()
    {
      CompactionTask.CompactionTuningConfig newTuningConfig = tuningConfig == null
                                               ? CompactionTask.CompactionTuningConfig.defaultConfig()
                                               : tuningConfig;
      PartitionsSpec partitionsSpec = newTuningConfig.getGivenOrDefaultPartitionsSpec();
      if (partitionsSpec instanceof DynamicPartitionsSpec) {
        final DynamicPartitionsSpec dynamicPartitionsSpec = (DynamicPartitionsSpec) partitionsSpec;
        partitionsSpec = new DynamicPartitionsSpec(
            dynamicPartitionsSpec.getMaxRowsPerSegment(),
            dynamicPartitionsSpec.getMaxTotalRowsOr(DynamicPartitionsSpec.DEFAULT_COMPACTION_MAX_TOTAL_ROWS)
        );
      }
      return newTuningConfig.withPartitionsSpec(partitionsSpec);
    }
  }
}
