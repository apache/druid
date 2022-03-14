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
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistributionMerger;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringSketchMerger;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CollectionUtils;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Interval;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link ParallelIndexTaskRunner} for the phase to determine distribution of dimension values in
 * multi-phase parallel indexing.
 */
class PartialDimensionDistributionParallelIndexTaskRunner
    extends InputSourceSplitParallelIndexTaskRunner<PartialDimensionDistributionTask, DimensionDistributionReport>
{
  private static final Logger log = new Logger(PartialDimensionDistributionParallelIndexTaskRunner.class);
  private static final String PHASE_NAME = "partial dimension distribution";

  private final ExecutorService executor = Execs.singleThreaded("DimDistributionMerger-%s");
  private final ConcurrentHashMap<Interval, StringDistributionMerger> intervalToDistributionMerger
      = new ConcurrentHashMap<>();
  private final AtomicLong totalSketchHeapSize = new AtomicLong();

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
  }

  @Override
  public String getName()
  {
    return PHASE_NAME;
  }

  @Override
  public void collectReport(DimensionDistributionReport report)
  {
    // TODO: keep an md5 hash of the report to implement the same validation as in taskMonitor

    // Send an empty report to TaskMonitor
    super.collectReport(new DimensionDistributionReport(report.getTaskId(), null));

    // Update the distributions in a separate thread to unblock HTTP requests
    if (executor.isShutdown()) {
      // this should never happen
      throw new ISE("Executor is already shutdown. Cannot merge more reports.");
    }
    executor.submit(() -> extractDistributionsFromReport(report));
  }

  /**
   * Map from an Interval to StringDistribution obtained by merging all the
   * StringDistributions reported by different sub-tasks for that Interval.
   */
  public Map<Interval, StringDistribution> getIntervalToDistribution()
  {
    waitForDistributionsToMerge();
    return CollectionUtils.mapValues(
        intervalToDistributionMerger,
        StringDistributionMerger::getResult
    );
  }

  /**
   * Extracts the distributions from the given report and merges them to the
   * distributions in memory.
   */
  private void extractDistributionsFromReport(DimensionDistributionReport report)
  {
    log.debug("Started merging distributions from Task ID [%s]", report.getTaskId());
    report.getIntervalToDistribution().forEach(
        (interval, distribution) -> intervalToDistributionMerger.compute(interval, (i, existingMerger) -> {
          final long oldSketchSize;
          final StringDistributionMerger merger;
          if (existingMerger == null) {
            merger = new StringSketchMerger();
            oldSketchSize = 0L;
          } else {
            merger = existingMerger;
            oldSketchSize = existingMerger.getResult().sizeInBytes();
          }

          merger.merge(distribution);

          final long newSketchSize = merger.getResult().sizeInBytes();
          totalSketchHeapSize.addAndGet(newSketchSize - oldSketchSize);

          return merger;
        })
    );

    log.debug("Finished merging distributions from Task ID [%s]", report.getTaskId());
    validateSketchesHeapSize();
  }

  /**
   * Validates the total heap size of all the distribution sketches. The task is
   * failed if the total size exceeds the threshold.
   */
  private void validateSketchesHeapSize()
  {
    // TODO: finalize the correct value of maxSketchHeapSize to use here
    final long maxSketchHeapSize = JvmUtils.getRuntimeInfo().getTotalHeapSizeBytes() / 6L;
    if (totalSketchHeapSize.get() > maxSketchHeapSize) {
      final String errorMsg = String.format(
          "Too many interval distributions to process [%s]. Estimated Size [%s], Max Heap Size [%s].\n"
          + "Try one of the following:\n"
          + "(1) Increase the task JVM heap size.\n"
          + "(2) Reduce time range of input dataset.\n"
          + "(3) Use a coarser segment granularity.",
          intervalToDistributionMerger.size(),
          totalSketchHeapSize.get(),
          maxSketchHeapSize
      );
      log.error(errorMsg);

      // This code can be executed in two cases:
      // Case 1: There are sub-tasks pending execution
      //   Calling stopGracefully() fails this phase and the supervisor task.
      // Case 2: All sub-tasks have finished
      //   Calling stopGracefully() has no effect. This is okay as all task reports
      //   have been received and process is still running fine.
      // TODO: include the correct error message in the task status
      stopGracefully(errorMsg);
    }
  }

  /**
   * Waits for distributions from pending reports (if any) to be merged.
   */
  private void waitForDistributionsToMerge()
  {
    log.info("Waiting for distributions to be merged");
    try {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      throw new ISE(e, "Interrupted while waiting for distributions to merge");
    }
    finally {
      if (!executor.isTerminated()) {
        log.error("Executor was not terminated. There are pending distributions to be merged.");
      }
    }
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
