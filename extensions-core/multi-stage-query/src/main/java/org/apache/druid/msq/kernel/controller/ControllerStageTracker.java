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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
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
import java.util.stream.IntStream;
import java.util.TreeMap;

/**
 * Controller-side state machine for each stage. Used by {@link ControllerQueryKernel} to form the overall state
 * machine for an entire query.
 * <p>
 * Package-private: stage trackers are an internal implementation detail of {@link ControllerQueryKernel}, not meant
 * for separate use.
 */
class ControllerStageTracker
{
  private static final Logger log = new Logger(ControllerStageTracker.class);
  private final StageDefinition stageDef;

  private final int workerCount;

  private final WorkerInputs workerInputs;

  // worker-> workerStagePhase
  private final Int2ObjectMap<WorkerStagePhase> workerToPhase = new Int2ObjectOpenHashMap<>();

  private final IntSet workersWithResultKeyStatistics = new IntAVLTreeSet();
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

    initializeWorkerState(workerCount);

    if (stageDef.mustGatherResultKeyStatistics()) {
      this.completeKeyStatisticsInformation =
          new CompleteKeyStatisticsInformation(new TreeMap<>(), false, 0);
    } else {
      this.completeKeyStatisticsInformation = null;
      generateResultPartitionsAndBoundariesWithoutKeyStatistics();
    }
  }

  /**
   * Initalized stage for each worker to {@link WorkerStagePhase#NEW}
   *
   * @param workerCount
   */
  private void initializeWorkerState(int workerCount)
  {
    IntStream.range(0, workerCount).forEach(wokerNumber -> workerToPhase.put(wokerNumber, WorkerStagePhase.NEW));
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
   * Get workers which need to be sent partition boundaries
   *
   * @return
   */
  IntSet getWorkersToSendParitionBoundaries()
  {
    if (!getStageDefinition().doesShuffle()) {
      throw new ISE("Result partition information is not relevant to this stage because it does not shuffle");
    }
    IntAVLTreeSet workers = new IntAVLTreeSet();
    for (Integer worker : workerToPhase.keySet()) {
      if (WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES.equals(workerToPhase.get(worker))) {
        workers.add(worker);
      }
    }
    return workers;
  }

  /**
   * Indicates that the work order for worker has been sent. Transistions the state to {@link WorkerStagePhase#READING_INPUT}
   * if no more work orders need to be sent.
   *
   * @param worker
   */
  void workOrderSentForWorker(int worker)
  {

    workerToPhase.compute(worker, (wk, state) -> {
      if (state == null) {
        throw new ISE("Worker[%d] not found for stage[%s]", wk, stageDef.getStageNumber());
      }
      if (!WorkerStagePhase.READING_INPUT.canTransitionFrom(state)) {
        throw new ISE(
            "Worker[%d] cannot transistion from state[%s] to state[%s] while sending work order",
            worker,
            state,
            WorkerStagePhase.READING_INPUT
        );
      }
      return WorkerStagePhase.READING_INPUT;
    });
    if (phase != ControllerStagePhase.READING_INPUT) {
      if (allWorkOrdersSent()) {
        // if no more work orders need to be sent, change state to reading input from retrying.
        transitionTo(ControllerStagePhase.READING_INPUT);
      }
    }

  }

  /**
   * Indicates that the partition boundaries for worker has been sent.
   *
   * @param worker
   */
  void partitionBoundariesSentForWorker(int worker)
  {

    workerToPhase.compute(worker, (wk, state) -> {
      if (state == null) {
        throw new ISE("Worker[%d] not found for stage[%s]", wk, stageDef.getStageNumber());
      }
      if (!WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT.canTransitionFrom(state)) {
        throw new ISE(
            "Worker[%d] cannot transistion from state[%s] to state[%s] while sending partition boundaries",
            worker,
            state,
            WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT
        );
      }
      return WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT;
    });

  }


  /**
   * Whether the result key statistics collector for this stage has encountered any multi-valued input at
   * any key position.
   * <p>
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

    WorkerStagePhase currentPhase = workerToPhase.get(workerNumber);

    if (currentPhase == null) {
      throw new ISE("Worker[%d] not found for stage[%s]", workerNumber, stageDef.getStageNumber());
    }

    try {
      if (WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES.canTransitionFrom(currentPhase)) {
        workerToPhase.put(workerNumber, WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES);

        // if stats already received for worker, do not update the sketch.
        if (workersWithReportedKeyStatistics.add(workerNumber)) {
          if (partialKeyStatisticsInformation.getTimeSegments().contains(null)) {
            // Time should not contain null value
            failForReason(InsertTimeNullFault.instance());
            return getPhase();
          }
          completeKeyStatisticsInformation.mergePartialInformation(workerNumber, partialKeyStatisticsInformation);

        }

        if (allPartitionStatisticsPresent()) {
          // All workers have sent the partial key statistics information.
          // Transition to MERGING_STATISTICS state to queue fetch clustering statistics from workers.
          if (phase != ControllerStagePhase.FAILED) {
            transitionTo(ControllerStagePhase.MERGING_STATISTICS);
          }
        }
      } else {
        throw new ISE(
            "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
            workerNumber,
            (stageDef.getStageNumber()),
            WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES,
            currentPhase

        );
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

    WorkerStagePhase currentPhase = workerToPhase.get(workerNumber);
    if (currentPhase == null) {
      throw new ISE("Worker[%d] not found for stage[%s]", workerNumber, stageDef.getStageNumber());
    }

    if (WorkerStagePhase.RESULTS_READY.canTransitionFrom(currentPhase)) {

      if (stageDef.mustGatherResultKeyStatistics() && currentPhase == WorkerStagePhase.READING_INPUT) {
        throw new ISE(
            "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
            workerNumber,
            (stageDef.getStageNumber()),
            WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT,
            currentPhase
        );
      }
      workerToPhase.put(workerNumber, WorkerStagePhase.RESULTS_READY);
      if (this.resultObject == null) {
        this.resultObject = resultObject;
      } else {
        //noinspection unchecked
        this.resultObject = getStageDefinition().getProcessorFactory()
                                                .mergeAccumulatedResult(this.resultObject, resultObject);
      }
    } else {
      throw new ISE(
          "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
          workerNumber,
          (stageDef.getStageNumber()),
          stageDef.mustGatherResultKeyStatistics()
          ? WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT
          : WorkerStagePhase.READING_INPUT,
          currentPhase

      );
    }

    if (allResultsPresent()) {
      transitionTo(ControllerStagePhase.RESULTS_READY);
      return true;
    }
    return false;
  }

  private boolean allResultsPresent()
  {
    return workerToPhase.values()
                        .stream()
                        .filter(stagePhase -> stagePhase.equals(WorkerStagePhase.RESULTS_READY))
                        .count() == workerCount;
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
      // In case of retrying workers, we are perfectly fine using the partition boundaries generated before the retry
      // took place. Hence, ignoring the request to generate result partitions.
      log.debug("Partition boundaries already generated for stage %d", stageDef.getStageNumber());
      return;
    }

    final int stageNumber = stageDef.getStageNumber();

    if (stageDef.doesShuffle()) {
      if (stageDef.mustGatherResultKeyStatistics() && !allPartitionStatisticsPresent()) {
        throw new ISE("Cannot generate result partitions without all worker key statistics");
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
   * True if all partitions stats are present for a shuffling stage which require statistics, else false.
   * If the stage does not gather result statistics, we return a true.
   */
  private boolean allPartitionStatisticsPresent()
  {
    if (!stageDef.mustGatherResultKeyStatistics()) {
      return true;
    }
    return workerToPhase.values()
                        .stream()
                        .filter(stagePhase -> stagePhase.equals(WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES)
                                              || stagePhase.equals(WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT)
                                              || stagePhase.equals(WorkerStagePhase.RESULTS_READY))
                        .count()
           == workerCount;
  }

  /**
   * True if all work orders are sent else false.
   */
  private boolean allWorkOrdersSent()
  {
    return workerToPhase.values()
                        .stream()
                        .filter(stagePhase -> stagePhase.equals(WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES)
                                              || stagePhase.equals(WorkerStagePhase.READING_INPUT)
                                              || stagePhase.equals(WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT)
                                              || stagePhase.equals(WorkerStagePhase.RESULTS_READY)
                        )
                        .count()
           == workerCount;
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

  private void transitionTo(final ControllerStagePhase newPhase)
  {
    if (newPhase.canTransitionFrom(phase)) {
      phase = newPhase;
    } else {
      throw new IAE("Cannot transition from [%s] to [%s]", phase, newPhase);
    }
  }

  /**
   * Retry true if the worker needs to be retried based on state else returns false.
   *
   * @param workerNumber
   */
  public boolean retryIfNeeded(int workerNumber)
  {
    if (phase.equals(ControllerStagePhase.FINISHED) || phase.equals(ControllerStagePhase.RESULTS_READY)) {
      // do nothing
      return false;
    }
    if (!isTrackingWorker(workerNumber)) {
      // not tracking this worker
      return false;
    }

    if (workerToPhase.get(workerNumber).equals(WorkerStagePhase.RESULTS_READY)
        || workerToPhase.get(workerNumber).equals(WorkerStagePhase.FINISHED)) {
      // do nothing
      return false;
    }
    workerToPhase.put(workerNumber, WorkerStagePhase.NEW);
    transitionTo(ControllerStagePhase.RETRYING);
    return true;
  }


  private boolean isTrackingWorker(int workerNumber)
  {
    return workerToPhase.get(workerNumber) != null;
  }

}
