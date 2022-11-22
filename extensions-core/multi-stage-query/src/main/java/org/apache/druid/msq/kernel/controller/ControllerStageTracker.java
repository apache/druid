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

package org.apache.druid.msq.kernel.controller;

import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.error.InsertTimeNullFault;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.TooManyPartitionsFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;

import javax.annotation.Nullable;
import java.util.List;
import java.util.TreeMap;

/**
 * Controller-side state machine for each stage. Used by {@link ControllerQueryKernel} to form the overall state
 * machine for an entire query.
 *
 * Package-private: stage trackers are an internal implementation detail of {@link ControllerQueryKernel}, not meant
 * for separate use.
 */
class ControllerStageTracker
{
  private final StageDefinition stageDef;
  private final int workerCount;

  private final WorkerInputs workerInputs;
  private final IntSet workersWithReportedKeyStatistics = new IntAVLTreeSet();
  private final IntSet workersWithResultsComplete = new IntAVLTreeSet();

  private ControllerStagePhase phase = ControllerStagePhase.NEW;

  @Nullable
  public final CompleteKeyStatisticsInformation completeKeyStatisticsInformation;

  // Result partitions and where they can be read from.
  @Nullable
  private ReadablePartitions resultPartitions;

  // Boundaries for the result partitions. Only set if this stage is shuffling.
  @Nullable
  private ClusterByPartitions resultPartitionBoundaries;

  @Nullable
  private Object resultObject;

  @Nullable // Set if phase is FAILED
  private MSQFault failureReason;

  private ControllerStageTracker(
      final StageDefinition stageDef,
      final WorkerInputs workerInputs
  )
  {
    this.stageDef = stageDef;
    this.workerCount = workerInputs.workerCount();
    this.workerInputs = workerInputs;

    if (stageDef.mustGatherResultKeyStatistics()) {
      this.completeKeyStatisticsInformation =
          new CompleteKeyStatisticsInformation(new TreeMap<>(), false, 0);
    } else {
      this.completeKeyStatisticsInformation = null;
      generateResultPartitionsAndBoundariesWithoutKeyStatistics();
    }
  }

  /**
   * Given a stage definition and number of workers to available per stage, this method creates a stage tracker.
   * This method determines the actual number of workers to use (which in turn depends on the input slices and
   * the assignment strategy)
   */
  static ControllerStageTracker create(
      final StageDefinition stageDef,
      final Int2IntMap stageWorkerCountMap,
      final InputSpecSlicer slicer,
      final WorkerAssignmentStrategy assignmentStrategy
  )
  {
    final WorkerInputs workerInputs = WorkerInputs.create(stageDef, stageWorkerCountMap, slicer, assignmentStrategy);
    return new ControllerStageTracker(stageDef, workerInputs);
  }

  /**
   * StageDefinition associated with the stage represented by this tracker
   */
  StageDefinition getStageDefinition()
  {
    return stageDef;
  }

  /**
   * The phase this stage tracker is in.
   */
  ControllerStagePhase getPhase()
  {
    return phase;
  }

  /**
   * Whether partitions for the results of this stage have been set.
   */
  boolean hasResultPartitions()
  {
    return resultPartitions != null;
  }

  /**
   * Partitions for the results of the stage associated with this tracker.
   */
  ReadablePartitions getResultPartitions()
  {
    if (resultPartitions == null) {
      throw new ISE("Result partition information is not ready yet");
    } else {
      return resultPartitions;
    }
  }

  /**
   * @return Partition boundaries for the results of this stage
   */
  ClusterByPartitions getResultPartitionBoundaries()
  {
    if (!getStageDefinition().doesShuffle()) {
      throw new ISE("Result partition information is not relevant to this stage because it does not shuffle");
    } else if (resultPartitionBoundaries == null) {
      throw new ISE("Result partition information is not ready yet");
    } else {
      return resultPartitionBoundaries;
    }
  }

  /**
   * Whether the result key statistics collector for this stage has encountered any multi-valued input at
   * any key position.
   *
   * This method exists because {@link org.apache.druid.timeline.partition.DimensionRangeShardSpec} does not
   * support partitioning on multi-valued strings, so we need to know if any multi-valued strings exist in order
   * to decide whether we can use this kind of shard spec.
   */
  boolean collectorEncounteredAnyMultiValueField()
  {
    if (completeKeyStatisticsInformation == null) {
      throw new ISE("Stage does not gather result key statistics");
    } else if (workersWithReportedKeyStatistics.size() != workerCount) {
      throw new ISE("Result key statistics are not ready");
    } else {
      return completeKeyStatisticsInformation.hasMultipleValues();
    }
  }

  /**
   * @return Result object associated with this stage
   */
  Object getResultObject()
  {
    if (phase == ControllerStagePhase.FINISHED) {
      throw new ISE("Result object has been cleaned up prematurely");
    } else if (phase != ControllerStagePhase.RESULTS_READY) {
      throw new ISE("Result object is not ready yet");
    } else if (resultObject == null) {
      throw new NullPointerException("resultObject was unexpectedly null");
    } else {
      return resultObject;
    }
  }

  /**
   * Marks that the stage is no longer NEW and has started reading inputs (and doing work)
   */
  void start()
  {
    transitionTo(ControllerStagePhase.READING_INPUT);
  }

  /**
   * Marks that the stage is finished and its results must not be used as they could have cleaned up.
   */
  void finish()
  {
    transitionTo(ControllerStagePhase.FINISHED);
  }

  /**
   * Inputs to each worker for this particular stage.
   */
  WorkerInputs getWorkerInputs()
  {
    return workerInputs;
  }

  /**
   * Returns the merged key statistics.
   */
  @Nullable
  public CompleteKeyStatisticsInformation getCompleteKeyStatisticsInformation()
  {
    return completeKeyStatisticsInformation;
  }

  /**
   * Adds result key statistics for a particular worker number. If statistics have already been added for this worker,
   * then this call ignores the new ones and does nothing.
   *
   * @param workerNumber the worker
   * @param partialKeyStatisticsInformation partial key statistics
   */
  ControllerStagePhase addPartialKeyStatisticsForWorker(
      final int workerNumber,
      final PartialKeyStatisticsInformation partialKeyStatisticsInformation
  )
  {
    if (phase != ControllerStagePhase.READING_INPUT) {
      throw new ISE("Cannot add result key statistics from stage [%s]", phase);
    }
    if (!stageDef.mustGatherResultKeyStatistics() || !stageDef.doesShuffle() || completeKeyStatisticsInformation == null) {
      throw new ISE("Stage does not gather result key statistics");
    }

    if (workerNumber < 0 || workerNumber >= workerCount) {
      throw new IAE("Invalid workerNumber [%s]", workerNumber);
    }

    try {
      if (workersWithReportedKeyStatistics.add(workerNumber)) {

        if (partialKeyStatisticsInformation.getTimeSegments().contains(null)) {
          // Time should not contain null value
          failForReason(InsertTimeNullFault.instance());
          return getPhase();
        }

        completeKeyStatisticsInformation.mergePartialInformation(workerNumber, partialKeyStatisticsInformation);

        if (workersWithReportedKeyStatistics.size() == workerCount) {
          // All workers have sent the partial key statistics information.
          // Transition to MERGING_STATISTICS state to queue fetch clustering statistics from workers.
          transitionTo(ControllerStagePhase.MERGING_STATISTICS);

        }
      }
    }
    catch (Exception e) {
      // If this op fails, we're in an inconsistent state and must cancel the stage.
      fail();
      throw e;
    }
    return getPhase();
  }

  /**
   * Sets the {@link #resultPartitions} and {@link #resultPartitionBoundaries} and transitions the phase to POST_READING.
   */
  void setClusterByPartitionBoundaries(ClusterByPartitions clusterByPartitions)
  {
    if (resultPartitions != null) {
      throw new ISE("Result partitions have already been generated");
    }

    if (!stageDef.mustGatherResultKeyStatistics()) {
      throw new ISE("Result partitions does not require key statistics, should not have set partition boundries here");
    }

    if (!ControllerStagePhase.MERGING_STATISTICS.equals(getPhase())) {
      throw new ISE("Cannot set partition boundires from key statistics from stage [%s]", getPhase());
    }

    this.resultPartitionBoundaries = clusterByPartitions;
    this.resultPartitions = ReadablePartitions.striped(
        stageDef.getStageNumber(),
        workerCount,
        clusterByPartitions.size()
    );

    transitionTo(ControllerStagePhase.POST_READING);
  }

  /**
   * Accepts and sets the results that each worker produces for this particular stage
   *
   * @return true if the results for this stage have been gathered from all the workers, else false
   */
  @SuppressWarnings("unchecked")
  boolean setResultsCompleteForWorker(final int workerNumber, final Object resultObject)
  {
    if (workerNumber < 0 || workerNumber >= workerCount) {
      throw new IAE("Invalid workerNumber [%s]", workerNumber);
    }

    if (resultObject == null) {
      throw new NullPointerException("resultObject must not be null");
    }

    // This is unidirectional flow of data. While this works in the current state of MSQ where partial fault tolerance
    // is implemented and a query flows in one direction only, rolling back of workers' state and query kernel's
    // phase should be allowed to fully support fault tolerance in cases such as:
    //  1. Rolling back worker's state in case it fails (and then retries)
    //  2. Rolling back query kernel's phase in case the results are lost (and needs workers to retry the computation)
    if (workersWithResultsComplete.add(workerNumber)) {
      if (this.resultObject == null) {
        this.resultObject = resultObject;
      } else {
        //noinspection unchecked
        this.resultObject = getStageDefinition().getProcessorFactory()
                                                .mergeAccumulatedResult(this.resultObject, resultObject);
      }
    }

    if (workersWithResultsComplete.size() == workerCount) {
      transitionTo(ControllerStagePhase.RESULTS_READY);
      return true;
    }
    return false;
  }

  /**
   * Reason for failure of this stage.
   */
  MSQFault getFailureReason()
  {
    if (phase != ControllerStagePhase.FAILED) {
      throw new ISE("No failure");
    }

    return failureReason;
  }

  /**
   * Marks the stage as failed for no particular reason.
   */
  void fail()
  {
    failForReason(UnknownFault.forMessage(null));
  }

  /**
   * Sets {@link #resultPartitions} (always) and {@link #resultPartitionBoundaries} without using key statistics.
   *
   * If {@link StageDefinition#mustGatherResultKeyStatistics()} is true, this method should not be called.
   */
  private void generateResultPartitionsAndBoundariesWithoutKeyStatistics()
  {
    if (resultPartitions != null) {
      throw new ISE("Result partitions have already been generated");
    }

    final int stageNumber = stageDef.getStageNumber();

    if (stageDef.doesShuffle()) {
      if (stageDef.mustGatherResultKeyStatistics()) {
        throw new ISE("Cannot generate result partitions without key statistics");
      }

      final Either<Long, ClusterByPartitions> maybeResultPartitionBoundaries =
          stageDef.generatePartitionsForShuffle(null);

      if (maybeResultPartitionBoundaries.isError()) {
        failForReason(new TooManyPartitionsFault(stageDef.getMaxPartitionCount()));
        return;
      }

      resultPartitionBoundaries = maybeResultPartitionBoundaries.valueOrThrow();
      resultPartitions = ReadablePartitions.striped(
          stageNumber,
          workerCount,
          resultPartitionBoundaries.size()
      );
    } else {
      // No reshuffling: retain partitioning from nonbroadcast inputs.
      final Int2IntSortedMap partitionToWorkerMap = new Int2IntAVLTreeMap();
      for (int workerNumber : workerInputs.workers()) {
        final List<InputSlice> slices = workerInputs.inputsForWorker(workerNumber);
        for (int inputNumber = 0; inputNumber < slices.size(); inputNumber++) {
          final InputSlice slice = slices.get(inputNumber);

          if (slice instanceof StageInputSlice && !stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
            final StageInputSlice stageInputSlice = (StageInputSlice) slice;
            for (final ReadablePartition partition : stageInputSlice.getPartitions()) {
              partitionToWorkerMap.put(partition.getPartitionNumber(), workerNumber);
            }
          }
        }
      }

      resultPartitions = ReadablePartitions.collected(stageNumber, partitionToWorkerMap);
    }
  }

  /**
   * Marks the stage as failed and sets the reason for the same.
   *
   * @param fault reason why this stage has failed
   */
  void failForReason(final MSQFault fault)
  {
    transitionTo(ControllerStagePhase.FAILED);

    this.failureReason = fault;
  }

  void transitionTo(final ControllerStagePhase newPhase)
  {
    if (newPhase.canTransitionFrom(phase)) {
      phase = newPhase;
    } else {
      throw new IAE("Cannot transition from [%s] to [%s]", phase, newPhase);
    }
  }
}
