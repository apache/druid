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

import org.apache.druid.data.input.InputSplit;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistributionMerger;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringSketchMerger;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;

/**
 * {@link ParallelIndexTaskRunner} for the phase to determine distribution of dimension values in
 * multi-phase parallel indexing.
 */
class PartialDimensionDistributionParallelIndexTaskRunner
    extends InputSourceSplitParallelIndexTaskRunner<PartialDimensionDistributionTask, DimensionDistributionReport>
{
  private static final Logger log = new Logger(PartialDimensionDistributionParallelIndexTaskRunner.class);
  private static final String PHASE_NAME = "partial dimension distribution";

  private final ExecutorService executor = Execs.singleThreaded("DimDistributionWriter-%s");

  /**
   * Phaser to await the processing of all reports submitted to the {@link #executor}.
   */
  private final Phaser allReportsProcessedPhaser = new Phaser(1);

  private final Map<Interval, Set<String>> intervalToTaskIds = new HashMap<>();
  private final File tempDistributionsDir;

  PartialDimensionDistributionParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      String baseSubtaskSpecName,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context
  )
  {
    super(
        toolbox,
        taskId,
        groupId,
        baseSubtaskSpecName,
        ingestionSchema,
        context
    );
    this.tempDistributionsDir = createDistributionsDir();
  }

  @Override
  public String getName()
  {
    return PHASE_NAME;
  }

  @Override
  public void collectReport(DimensionDistributionReport report)
  {
    // Send an empty report to TaskMonitor
    super.collectReport(new DimensionDistributionReport(report.getTaskId(), null));

    // Extract the distributions in a separate thread to unblock HTTP requests
    if (executor.isShutdown()) {
      // this should never happen
      throw new ISE("Executor is already shutdown. Cannot process more reports.");
    }

    allReportsProcessedPhaser.register();
    executor.submit(() -> extractDistributionsFromReport(report));
  }

  /**
   * Map from an interval to PartitionBoundaries calculated by applying the target
   * row size on the final StringDistribution. The final distribution for an
   * interval is obtained by merging the distributions reported by all the
   * sub-tasks for that interval.
   */
  public Map<Interval, PartitionBoundaries> getIntervalToPartitionBoundaries(
      DimensionRangePartitionsSpec partitionsSpec
  )
  {
    waitToProcessPendingReports();

    // Do not proceed if a shutdown has been requested
    if (getStopReason() != null) {
      throw new ISE("DimensionDistributionPhaseRunner has been stopped. %s", getStopReason());
    }

    // Merge distributions only from succeeded sub-tasks
    final Set<String> succeededTaskIds = super.getReports().keySet();
    final Map<Interval, PartitionBoundaries> intervalToPartitions = new HashMap<>();
    intervalToTaskIds.forEach(
        (interval, subTaskIds) -> {
          final File intervalDir = getIntervalDistributionDir(interval);
          final StringDistributionMerger merger = new StringSketchMerger();
          subTaskIds
              .stream()
              .filter(succeededTaskIds::contains)
              .map(subTaskId -> readDistributionFromFile(intervalDir, subTaskId))
              .forEach(merger::merge);
          final StringDistribution mergedDistribution = merger.getResult();

          final PartitionBoundaries partitions;
          Integer targetRowsPerSegment = partitionsSpec.getTargetRowsPerSegment();
          if (targetRowsPerSegment == null) {
            partitions = mergedDistribution.getEvenPartitionsByMaxSize(partitionsSpec.getMaxRowsPerSegment());
          } else {
            partitions = mergedDistribution.getEvenPartitionsByTargetSize(targetRowsPerSegment);
          }

          intervalToPartitions.put(interval, partitions);
        }
    );

    cleanupDistributionsDir();
    return intervalToPartitions;
  }

  /**
   * Extracts the distributions from the given report and writes them to the task
   * temp directory.
   * <p>
   * This method is not thread-safe as the reports are processed by a single-threaded executor.
   */
  private void extractDistributionsFromReport(DimensionDistributionReport report)
  {
    try {
      log.debug("Started writing distributions from task [%s]", report.getTaskId());

      final Map<Interval, StringDistribution> distributions = report.getIntervalToDistribution();
      if (distributions == null || distributions.isEmpty()) {
        log.debug("No dimension distribution received from task [%s]", report.getTaskId());
        return;
      }
      distributions.forEach(
          (interval, distribution) -> {
            Set<String> taskIds = intervalToTaskIds.computeIfAbsent(interval, i -> new HashSet<>());
            final String subTaskId = report.getTaskId();
            if (!taskIds.contains(subTaskId)) {
              writeDistributionToFile(interval, subTaskId, distribution);
              taskIds.add(subTaskId);
            }
          }
      );

      log.debug("Finished writing distributions from task [%s]", report.getTaskId());
    }
    finally {
      allReportsProcessedPhaser.arriveAndDeregister();
    }
  }

  /**
   * Writes the given distribution to the task temp directory.
   * <p>
   * If this operation fails, it requests a graceful shutdown of the runner via
   * {@link #stopGracefully(String)}.
   */
  private void writeDistributionToFile(
      Interval interval,
      String subTaskId,
      StringDistribution distribution
  )
  {
    try {
      File intervalDir = getIntervalDistributionDir(interval);
      FileUtils.mkdirp(intervalDir);

      File distributionJsonFile = getDistributionJsonFile(intervalDir, subTaskId);
      getToolbox().getJsonMapper().writeValue(distributionJsonFile, distribution);
    }
    catch (IOException e) {
      String errorMsg = StringUtils.format(
          "Exception while writing distribution file for interval [%s], task [%s]",
          interval,
          subTaskId
      );
      stopGracefully(errorMsg);
      throw new ISE(e, errorMsg);
    }
  }

  private StringDistribution readDistributionFromFile(File intervalDir, String subTaskId)
  {
    try {
      File distributionJsonFile = getDistributionJsonFile(intervalDir, subTaskId);
      return getToolbox().getJsonMapper().readValue(distributionJsonFile, StringDistribution.class);
    }
    catch (IOException e) {
      throw new ISE(e, "Error while reading distribution for interval [%s], task [%s]",
                    intervalDir.getName(), subTaskId
      );
    }
  }

  private File getIntervalDistributionDir(Interval interval)
  {
    return new File(tempDistributionsDir, toIntervalString(interval));
  }

  private File getDistributionJsonFile(File intervalDir, String subTaskId)
  {
    return new File(intervalDir, subTaskId);
  }

  /**
   * Waits for distributions from pending reports (if any) to be extracted.
   */
  private void waitToProcessPendingReports()
  {
    log.info("Waiting to extract distributions from sub-task reports.");
    try {
      allReportsProcessedPhaser.arriveAndAwaitAdvance();
      executor.shutdownNow();
    }
    catch (Exception e) {
      throw new ISE(e, "Exception while waiting to extract distributions.");
    }
  }

  private File createDistributionsDir()
  {
    File taskTempDir = getToolbox().getConfig().getTaskTempDir(getTaskId());
    File distributionsDir = new File(taskTempDir, "dimension_distributions");
    try {
      FileUtils.mkdirp(distributionsDir);
      return distributionsDir;
    }
    catch (IOException e) {
      throw new ISE(e, "Could not create temp distribution directory.");
    }
  }

  private void cleanupDistributionsDir()
  {
    try {
      FileUtils.deleteDirectory(tempDistributionsDir);
    }
    catch (IOException e) {
      log.warn(e, "Could not delete temp distribution directory.");
    }
  }

  private String toIntervalString(Interval interval)
  {
    return
        new DateTime(interval.getStartMillis(), interval.getChronology())
        + "_"
        + new DateTime(interval.getEndMillis(), interval.getChronology());
  }

  @Override
  SubTaskSpec<PartialDimensionDistributionTask> createSubTaskSpec(
      String id,
      String groupId,
      String supervisorTaskId,
      Map<String, Object> context,
      InputSplit split,
      ParallelIndexIngestionSpec subTaskIngestionSpec
  )
  {
    return new SubTaskSpec<PartialDimensionDistributionTask>(
        id,
        groupId,
        supervisorTaskId,
        context,
        split
    )
    {
      @Override
      public PartialDimensionDistributionTask newSubTask(int numAttempts)
      {
        return new PartialDimensionDistributionTask(
            null,
            getGroupId(),
            null,
            getSupervisorTaskId(),
            id,
            numAttempts,
            subTaskIngestionSpec,
            getContext()
        );
      }
    };
  }
}
