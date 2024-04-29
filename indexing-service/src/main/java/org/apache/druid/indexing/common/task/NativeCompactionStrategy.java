package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.utils.CollectionUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.druid.indexing.common.task.CompactionTask.CTX_KEY_APPENDERATOR_TRACKING_TASK_ID;

public class NativeCompactionStrategy implements CompactionStrategy
{

  private static final Logger log = new Logger(NativeCompactionStrategy.class);
  public static final String type = "NATIVE";
  private static final boolean STORE_COMPACTION_STATE = true;

  @JsonCreator
  public NativeCompactionStrategy()
  {
  }

  /**
   * Generate {@link ParallelIndexIngestionSpec} from input dataschemas.
   *
   * @return an empty list if input segments don't exist. Otherwise, a generated ingestionSpec.
   */
  @VisibleForTesting
  static List<ParallelIndexIngestionSpec> createIngestionSpecs(
      List<NonnullPair<Interval, DataSchema>> dataschemas,
      final TaskToolbox toolbox,
      final CompactionIOConfig ioConfig,
      final CompactionTask.PartitionConfigurationManager partitionConfigurationManager,
      final CoordinatorClient coordinatorClient,
      final SegmentCacheManagerFactory segmentCacheManagerFactory
  )
  {
    final CompactionTask.CompactionTuningConfig compactionTuningConfig = partitionConfigurationManager.computeTuningConfig();

    return dataschemas.stream().map((dataSchema) -> new ParallelIndexIngestionSpec(
                                        dataSchema.rhs,
                                        createIoConfig(
                                            toolbox,
                                            dataSchema.rhs,
                                            dataSchema.lhs,
                                            coordinatorClient,
                                            segmentCacheManagerFactory,
                                            ioConfig
                                        ),
                                        compactionTuningConfig
                                    )

    ).collect(Collectors.toList());
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
    if (!compactionIOConfig.isAllowNonAlignedInterval()) {
      // Validate interval alignment.
      final Granularity segmentGranularity = dataSchema.getGranularitySpec().getSegmentGranularity();
      final Interval widenedInterval = Intervals.utc(
          segmentGranularity.bucketStart(interval.getStart()).getMillis(),
          segmentGranularity.bucketEnd(interval.getEnd().minus(1)).getMillis()
      );

      if (!interval.equals(widenedInterval)) {
        throw new IAE(
            "Interval[%s] to compact is not aligned with segmentGranularity[%s]",
            interval,
            segmentGranularity
        );
      }
    }

    return new ParallelIndexIOConfig(
        null,
        new DruidInputSource(
            dataSchema.getDataSource(),
            interval,
            null,
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
      TaskToolbox taskToolbox,
      List<NonnullPair<Interval, DataSchema>> dataSchemas
  ) throws JsonProcessingException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = createIngestionSpecs(
        dataSchemas,
        taskToolbox,
        compactionTask.getIoConfig(),
        compactionTask.getPartitionConfigurationManager(),
        taskToolbox.getCoordinatorClient(),
        compactionTask.getSegmentCacheManagerFactory()
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
        compactionTask.getCurrentSubTaskHolder(),
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

  @VisibleForTesting
  ParallelIndexSupervisorTask newTask(CompactionTask compactionTask, String baseSequenceName, ParallelIndexIngestionSpec ingestionSpec)
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
    newContext.put(CTX_KEY_APPENDERATOR_TRACKING_TASK_ID, compactionTask.getId());
    newContext.putIfAbsent(CompactSegments.STORE_COMPACTION_STATE_KEY, STORE_COMPACTION_STATE);
    // Set the priority of the compaction task.
    newContext.put(Tasks.PRIORITY_KEY, compactionTask.getPriority());
    return newContext;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return type;
  }
}
