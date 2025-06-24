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
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.indexing.error.InsertTimeNullFault;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.TooManyPartitionsFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.GlobalSortShuffleSpec;
import org.apache.druid.msq.kernel.ShuffleKind;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final long STATIC_TIME_CHUNK_FOR_PARALLEL_MERGE = Granularities.ALL.bucketStart(-1);
  private final StageDefinition stageDef;

  private final int workerCount;

  private final WorkerInputs workerInputs;

  // worker-> workerStagePhase
  // Controller keeps track of the stage with this map.
  // Currently, we rely on the serial nature of the state machine to keep things in sync between the controller and the worker.
  // So the worker state in the controller can go out of sync with the actual worker state.


  private final Int2ObjectMap<ControllerWorkerStagePhase> workerToPhase = new Int2ObjectOpenHashMap<>();

  // workers which have reported partial key information.
  private final IntSet workerReportedPartialKeyInformation = new IntAVLTreeSet();

  // workers from which key collector is fetched.
  private final IntSet workersFromWhichKeyCollectorFetched = new IntAVLTreeSet();
  private final int maxRetainedPartitionSketchBytes;
  private ControllerStagePhase phase = ControllerStagePhase.NEW;

  @Nullable
  public final CompleteKeyStatisticsInformation completeKeyStatisticsInformation;

  // Result partitions and where they can be read from.
  @Nullable
  private ReadablePartitions resultPartitions;

  // Boundaries for the result partitions. Only set if this stage is shuffling.
  @Nullable
  private ClusterByPartitions resultPartitionBoundaries;


  // created when mergingStatsForTimeChunk is called. Should be cleared once timeChunkToBoundaries is set for the timechunk
  private final Map<Long, ClusterByStatisticsCollector> timeChunkToCollector = new HashMap<>();
  private final Map<Long, ClusterByPartitions> timeChunkToBoundaries = new TreeMap<>();
  long totalPartitionCount;


  // states used for tracking worker to timechunks and vice versa so that we know when to generate partition boundaries for (timeChunk,worker)
  private Map<Integer, Set<Long>> workerToRemainingTimeChunks = null;
  private Map<Long, Set<Integer>> timeChunkToRemainingWorkers = null;

  @Nullable
  private Object resultObject;

  @Nullable // Set if phase is FAILED
  private MSQFault failureReason;

  private ControllerStageTracker(
      final StageDefinition stageDef,
      final WorkerInputs workerInputs,
      final int maxRetainedPartitionSketchBytes
  )
  {
    this.stageDef = stageDef;
    this.workerCount = workerInputs.workerCount();
    this.workerInputs = workerInputs;
    this.maxRetainedPartitionSketchBytes = maxRetainedPartitionSketchBytes;

    initializeWorkerState(workerInputs.workers());

    if (stageDef.mustGatherResultKeyStatistics()) {
      this.completeKeyStatisticsInformation =
          new CompleteKeyStatisticsInformation(new TreeMap<>(), false, 0);
    } else {
      this.completeKeyStatisticsInformation = null;
      generateResultPartitionsAndBoundariesWithoutKeyStatistics();
    }
  }

  /**
   * Initialize stage for each worker to {@link ControllerWorkerStagePhase#NEW}.
   */
  private void initializeWorkerState(IntSet workers)
  {
    for (int workerNumber : workers) {
      workerToPhase.put(workerNumber, ControllerWorkerStagePhase.NEW);
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
      final WorkerAssignmentStrategy assignmentStrategy,
      final int maxRetainedPartitionSketchBytes,
      final long maxInputBytesPerWorker
  )
  {
    final WorkerInputs workerInputs = WorkerInputs.create(
        stageDef,
        stageWorkerCountMap,
        slicer,
        assignmentStrategy,
        maxInputBytesPerWorker
    );

    return new ControllerStageTracker(
        stageDef,
        workerInputs,
        maxRetainedPartitionSketchBytes
    );
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
  IntSet getWorkersToSendPartitionBoundaries()
  {
    if (!getStageDefinition().doesShuffle()) {
      throw new ISE("Result partition information is not relevant to this stage because it does not shuffle");
    }
    IntAVLTreeSet workers = new IntAVLTreeSet();
    for (int worker : workerToPhase.keySet()) {
      if (ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES.equals(workerToPhase.get(worker))) {
        workers.add(worker);
      }
    }
    return workers;
  }

  /**
   * Indicates that the work order for worker has been sent. Transitions the state to {@link ControllerWorkerStagePhase#READING_INPUT}
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
      if (!ControllerWorkerStagePhase.READING_INPUT.canTransitionFrom(state)) {
        throw new ISE(
            "Worker[%d] cannot transistion from state[%s] to state[%s] while sending work order",
            worker,
            state,
            ControllerWorkerStagePhase.READING_INPUT
        );
      }
      return ControllerWorkerStagePhase.READING_INPUT;
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
      if (!ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT.canTransitionFrom(state)) {
        throw new ISE(
            "Worker[%d] cannot transistion from state[%s] to state[%s] while sending partition boundaries",
            worker,
            state,
            ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT
        );
      }
      return ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT;
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
    } else if (workerReportedPartialKeyInformation.size() != workerCount) {
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
    if (!phase.isSuccess()) {
      throw new ISE("Result object for stage[%s] is not ready yet", stageDef.getId());
    } else if (resultObject == null) {
      throw new NullPointerException(
          StringUtils.format(
              "Result object for stage[%s] was unexpectedly null",
              stageDef.getId()
          )
      );
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
   * Adds partial key statistics information for a particular worker number. If information is already added for this worker,
   * then this call ignores the new information.
   *
   * @param workerNumber                    the worker
   * @param partialKeyStatisticsInformation partial key statistics
   */
  void addPartialKeyInformationForWorker(
      final int workerNumber,
      final PartialKeyStatisticsInformation partialKeyStatisticsInformation
  )
  {
    if (!stageDef.mustGatherResultKeyStatistics()
        || !stageDef.doesShuffle()
        || completeKeyStatisticsInformation == null) {
      throw new ISE("Stage does not gather result key statistics");
    }

    if (!workerInputs.workers().contains(workerNumber)) {
      throw new IAE("Invalid workerNumber [%s]", workerNumber);
    }

    ControllerWorkerStagePhase currentPhase = workerToPhase.get(workerNumber);

    if (currentPhase == null) {
      throw new ISE("Worker[%d] not found for stage[%s]", workerNumber, stageDef.getStageNumber());
    }

    try {
      if (ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_ALL_KEY_STATS_TO_BE_FETCHED.canTransitionFrom(currentPhase)) {
        workerToPhase.put(workerNumber, ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_ALL_KEY_STATS_TO_BE_FETCHED);

        // if partial key stats already received for worker, do not update the sketch.
        if (workerReportedPartialKeyInformation.add(workerNumber)) {
          if (partialKeyStatisticsInformation.getTimeSegments().contains(null)) {
            // Time should not contain null value
            failForReason(InsertTimeNullFault.instance());
            return;
          }
          completeKeyStatisticsInformation.mergePartialInformation(workerNumber, partialKeyStatisticsInformation);
        }

        if (resultPartitions != null) {
          // we already have result partitions. No need to fetch the stats from worker
          // can happen in case of worker retry
          workerToPhase.put(
              workerNumber,
              ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES
          );
        }


        if (workersFromWhichKeyCollectorFetched.contains(workerNumber)) {
          // we already have fetched the key collector from this worker. No need to fetch it again.
          // can happen in case of worker retry
          workerToPhase.put(
              workerNumber,
              ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES
          );
        }


        if (allPartialKeyInformationFetched()) {
          completeKeyStatisticsInformation.complete();
          if (workerToRemainingTimeChunks == null && timeChunkToRemainingWorkers == null) {
            initializeTimeChunkWorkerTrackers();
          }
          // All workers have sent the partial key statistics information.
          // Transition to MERGING_STATISTICS state to queue fetch clustering statistics from workers.
          if (phase != ControllerStagePhase.FAILED) {
            transitionTo(ControllerStagePhase.MERGING_STATISTICS);
          }
          // if all the results have been fetched, we can straight way transition to post reading.
          if (allResultsStatsFetched()) {
            if (phase != ControllerStagePhase.FAILED) {
              transitionTo(ControllerStagePhase.POST_READING);
            }
          }
        }
      } else {
        throw new ISE(
            "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
            workerNumber,
            (stageDef.getStageNumber()),
            ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_ALL_KEY_STATS_TO_BE_FETCHED,
            currentPhase

        );
      }
    }
    catch (Exception e) {
      // If this op fails, we're in an inconsistent state and must cancel the stage.
      fail();
      throw e;
    }
  }

  private void initializeTimeChunkWorkerTrackers()
  {
    workerToRemainingTimeChunks = new HashMap<>();
    timeChunkToRemainingWorkers = new HashMap<>();
    completeKeyStatisticsInformation.getTimeSegmentVsWorkerMap().forEach((timeChunk, workers) -> {
      for (int worker : workers) {
        this.workerToRemainingTimeChunks.compute(worker, (wk, timeChunks) -> {
          if (timeChunks == null) {
            timeChunks = new HashSet<>();
          }
          timeChunks.add(timeChunk);
          return timeChunks;
        });
      }
      timeChunkToRemainingWorkers.put(timeChunk, workers);
    });
  }


  /**
   * Merges the  {@link ClusterByStatisticsSnapshot} for the worker, time chunk with the stage {@link ClusterByStatisticsCollector} being
   * tracked at {@link #timeChunkToCollector} for the same time chunk. This method is called when
   * {@link ClusterStatisticsMergeMode#SEQUENTIAL} is chosen eventually.
   * <br></br>
   * <br></br>
   * If all the stats from the worker are merged, we transition the worker to {@link ControllerWorkerStagePhase#PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES};
   * <br></br>
   * If all the stats from all the workers are merged, we transition the stage to {@link ControllerStagePhase#POST_READING}
   */
  void mergeClusterByStatisticsCollectorForTimeChunk(
      int workerNumber,
      Long timeChunk,
      ClusterByStatisticsSnapshot clusterByStatisticsSnapshot
  )
  {
    if (!stageDef.mustGatherResultKeyStatistics()
        || !stageDef.doesShuffle()) {
      throw new ISE("Stage does not gather result key statistics");
    }

    if (!workerInputs.workers().contains(workerNumber)) {
      throw new IAE("Invalid workerNumber [%s]", workerNumber);
    }

    if (completeKeyStatisticsInformation == null || !completeKeyStatisticsInformation.isComplete()) {
      throw new ISE(
          "Cannot merge worker[%d] time chunk until all the key information is received for stage[%d]",
          workerNumber,
          stageDef.getStageNumber()
      );
    }

    ControllerWorkerStagePhase workerStagePhase = workerToPhase.get(workerNumber);

    if (workerStagePhase == null) {
      throw new ISE("Worker[%d] not found for stage[%s]", workerNumber, stageDef.getStageNumber());
    }

    // only merge in case this worker has remaining time chunks
    workerToRemainingTimeChunks.computeIfPresent(workerNumber, (wk, timeChunks) -> {
      if (timeChunks.remove(timeChunk)) {

        // merge the key collector
        timeChunkToCollector.compute(
            timeChunk,
            (ignored, collector) -> {
              if (collector == null) {
                collector = stageDef.createResultKeyStatisticsCollector(maxRetainedPartitionSketchBytes);
              }
              collector.addAll(clusterByStatisticsSnapshot);
              return collector;
            }
        );

        // if work for one time chunk is finished, generate the ClusterByPartitions for that timeChunk and clear the collector so that we free up controller memory.
        timeChunkToRemainingWorkers.compute(timeChunk, (tc, workers) -> {
          if (workers == null || workers.isEmpty()) {
            throw new ISE(
                "Remaining workers should not be empty until all the work is finished for time chunk[%d] for stage[%d]",
                timeChunk,
                stageDef.getStageNumber()
            );
          }
          workers.remove(workerNumber);
          if (workers.isEmpty()) {
            // generate partition boundaries since all work is finished for the time chunk
            log.info(
                "Generating partition boundaries from stage: %d of time chunk: [%s] GMT",
                stageDef.getStageNumber(),
                new Date(timeChunk)
            );
            ClusterByStatisticsCollector collector = timeChunkToCollector.get(tc);
            Either<Long, ClusterByPartitions> countOrPartitions =
                stageDef.generatePartitionBoundariesForShuffle(collector);
            totalPartitionCount += getPartitionCountFromEither(countOrPartitions);
            if (totalPartitionCount > stageDef.getMaxPartitionCount()) {
              failForReason(new TooManyPartitionsFault(stageDef.getMaxPartitionCount()));
              return null;
            }
            timeChunkToBoundaries.put(tc, countOrPartitions.valueOrThrow());

            // clear the collector to give back memory
            collector.clear();
            timeChunkToCollector.remove(tc);
            return null;
          }
          return workers;
        });
      }
      return timeChunks.isEmpty() ? null : timeChunks;
    });


    // if all time chunks for worker are taken care off transition worker.
    if (workerToRemainingTimeChunks.get(workerNumber) == null) {
      // adding worker to a set so that we do not fetch the worker collectors again.
      workersFromWhichKeyCollectorFetched.add(workerNumber);
      if (ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES.canTransitionFrom(
          workerStagePhase)) {
        workerToPhase.put(workerNumber, ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES);
      } else {
        throw new ISE(
            "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
            workerNumber,
            (stageDef.getStageNumber()),
            ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES,
            workerStagePhase

        );
      }
    }


    // if all time chunks have the partition boundaries, merge them to set resultPartitionBoundaries
    if (workerToRemainingTimeChunks.isEmpty()) {
      if (resultPartitionBoundaries == null) {
        timeChunkToBoundaries.forEach((ignored, partitions) -> {
          if (resultPartitionBoundaries == null) {
            resultPartitionBoundaries = partitions;
          } else {
            abutAndAppendPartitionBoundaries(resultPartitionBoundaries.ranges(), partitions.ranges());
          }
        });
        timeChunkToBoundaries.clear();
        setClusterByPartitionBoundaries(resultPartitionBoundaries);
      } else {
        // we already have result partitions. We can safely transition to POST READING and submit the result boundaries to the workers.
        transitionTo(ControllerStagePhase.POST_READING);
      }
    }

  }

  /**
   * Merges the entire {@link ClusterByStatisticsSnapshot} for the worker with the stage {@link ClusterByStatisticsCollector} being
   * tracked at {@link #timeChunkToCollector} with key {@link ControllerStageTracker#STATIC_TIME_CHUNK_FOR_PARALLEL_MERGE}. This method is called when
   * {@link ClusterStatisticsMergeMode#PARALLEL} is chosen eventually.
   * <br></br>
   * <br></br>
   * If all the stats from the worker are merged, we transition the worker to {@link ControllerWorkerStagePhase#PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES}.
   * <br></br>
   * If all the stats from all the workers are merged, we transition the stage to {@link ControllerStagePhase#POST_READING}.
   */

  void mergeClusterByStatisticsCollectorForAllTimeChunks(
      int workerNumber,
      ClusterByStatisticsSnapshot clusterByStatsSnapshot
  )
  {
    if (!stageDef.mustGatherResultKeyStatistics()
        || !stageDef.doesShuffle()) {
      throw new ISE("Stage does not gather result key statistics");
    }

    if (!workerInputs.workers().contains(workerNumber)) {
      throw new IAE("Invalid workerNumber [%s]", workerNumber);
    }

    ControllerWorkerStagePhase workerStagePhase = workerToPhase.get(workerNumber);

    if (workerStagePhase == null) {
      throw new ISE("Worker[%d] not found for stage[%s]", workerNumber, stageDef.getStageNumber());
    }


    // To prevent the case where we do not fetch the collector twice, like when worker is retried, we should be okay with the
    // older collector from the previous run of the worker.

    if (workersFromWhichKeyCollectorFetched.add(workerNumber)) {
      // in case of parallel merge we use the "ALL" granularity start time to put the sketches
      timeChunkToCollector.compute(
          STATIC_TIME_CHUNK_FOR_PARALLEL_MERGE,
          (timeChunk, stats) -> {
            if (stats == null) {
              stats = stageDef.createResultKeyStatisticsCollector(maxRetainedPartitionSketchBytes);
            }
            stats.addAll(clusterByStatsSnapshot);
            return stats;
          }
      );
    } else {
      log.debug("Already have key collector for worker[%d] stage[%d]", workerNumber, stageDef.getStageNumber());
    }

    if (ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES.canTransitionFrom(workerStagePhase)) {
      workerToPhase.put(workerNumber, ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES);
    } else {
      throw new ISE(
          "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
          workerNumber,
          (stageDef.getStageNumber()),
          ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES,
          workerStagePhase

      );
    }

    if (allResultsStatsFetched()) {
      if (completeKeyStatisticsInformation == null || !completeKeyStatisticsInformation.isComplete()) {
        throw new ISE(
            "Cannot generate partition boundaries until all the key information is received for worker[%d] stage[%d]",
            workerNumber,
            stageDef.getStageNumber()
        );
      }
      if (resultPartitions == null) {
        final ClusterByStatisticsCollector collector = timeChunkToCollector.get(STATIC_TIME_CHUNK_FOR_PARALLEL_MERGE);
        Either<Long, ClusterByPartitions> countOrPartitions = stageDef.generatePartitionBoundariesForShuffle(collector);
        totalPartitionCount += getPartitionCountFromEither(countOrPartitions);
        if (totalPartitionCount > stageDef.getMaxPartitionCount()) {
          failForReason(new TooManyPartitionsFault(stageDef.getMaxPartitionCount()));
          return;
        }
        resultPartitionBoundaries = countOrPartitions.valueOrThrow();
        setClusterByPartitionBoundaries(resultPartitionBoundaries);
      } else {
        log.debug("Already have result partitions for stage[%d]", stageDef.getStageNumber());
      }
      timeChunkToCollector.computeIfPresent(
          STATIC_TIME_CHUNK_FOR_PARALLEL_MERGE,
          (key, collector) -> collector.clear()
      );
      timeChunkToCollector.clear();

    }
  }

  /**
   * Returns true if all {@link ClusterByStatisticsSnapshot} are fetched from each worker else false.
   */
  private boolean allResultsStatsFetched()
  {
    return workerToPhase.values().stream()
                        .filter(stagePhase -> stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES)
                                              || stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT)
                                              || stagePhase.equals(ControllerWorkerStagePhase.RESULTS_READY))
                        .count()
           == workerCount;
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
      throw new ISE("Cannot set partition boundaries from key statistics from stage [%s]", getPhase());
    }

    this.resultPartitionBoundaries = clusterByPartitions;
    this.resultPartitions = ReadablePartitions.striped(
        stageDef.getStageNumber(),
        workerInputs.workers(),
        clusterByPartitions.size()
    );

    transitionTo(ControllerStagePhase.POST_READING);
  }

  /**
   * Transitions phase directly from {@link ControllerStagePhase#READING_INPUT} to
   * {@link ControllerStagePhase#POST_READING}, skipping {@link ControllerStagePhase#MERGING_STATISTICS}.
   * This method is used for stages that sort but do not need to gather result key statistics.
   */
  void setDoneReadingInputForWorker(final int workerNumber)
  {
    if (stageDef.mustGatherResultKeyStatistics()) {
      throw DruidException.defensive(
          "Cannot setDoneReadingInput for stage[%s], it should send partial key information instead",
          stageDef.getId()
      );
    }

    if (!stageDef.doesSortDuringShuffle()) {
      throw DruidException.defensive("Cannot setDoneReadingInput for stage[%s], it is not sorting", stageDef.getId());
    }

    if (!workerInputs.workers().contains(workerNumber)) {
      throw new IAE("Invalid workerNumber[%s] for stage[%s]", workerNumber, stageDef.getId());
    }

    ControllerWorkerStagePhase currentPhase = workerToPhase.get(workerNumber);

    if (currentPhase == null) {
      throw new ISE("Worker[%d] not found for stage[%s]", workerNumber, stageDef.getId());
    }

    try {
      if (ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT.canTransitionFrom(currentPhase)) {
        workerToPhase.put(workerNumber, ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT);

        if (allWorkersDoneReadingInput()) {
          transitionTo(ControllerStagePhase.POST_READING);
        }
      } else {
        throw new ISE(
            "Worker[%d] for stage[%d] expected to be in phase that can transition to[%s]. Found phase[%s]",
            workerNumber,
            stageDef.getStageNumber(),
            ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT,
            currentPhase
        );
      }
    }
    catch (Exception e) {
      // If this op fails, we're in an inconsistent state and must cancel the stage.
      fail();
      throw e;
    }
  }

  /**
   * Accepts and sets the results that each worker produces for this particular stage
   *
   * @return true if the results for this stage have been gathered from all the workers, else false
   */
  @SuppressWarnings("unchecked")
  boolean setResultsCompleteForWorker(final int workerNumber, final Object resultObject)
  {
    if (!workerInputs.workers().contains(workerNumber)) {
      throw new IAE("Invalid workerNumber [%s]", workerNumber);
    }

    if (resultObject == null) {
      throw new NullPointerException("resultObject must not be null");
    }

    ControllerWorkerStagePhase currentPhase = workerToPhase.get(workerNumber);
    if (currentPhase == null) {
      throw new ISE("Worker[%d] not found for stage[%s]", workerNumber, stageDef.getStageNumber());
    }

    if (ControllerWorkerStagePhase.RESULTS_READY.canTransitionFrom(currentPhase)) {

      if (stageDef.mustGatherResultKeyStatistics() && currentPhase == ControllerWorkerStagePhase.READING_INPUT) {
        throw new ISE(
            "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
            workerNumber,
            (stageDef.getStageNumber()),
            ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT,
            currentPhase
        );
      }
      workerToPhase.put(workerNumber, ControllerWorkerStagePhase.RESULTS_READY);
      if (this.resultObject == null) {
        this.resultObject = resultObject;
      } else {
        //noinspection unchecked
        this.resultObject = getStageDefinition().getProcessor()
                                                .mergeAccumulatedResult(this.resultObject, resultObject);
      }
    } else {
      throw new ISE(
          "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
          workerNumber,
          (stageDef.getStageNumber()),
          stageDef.mustGatherResultKeyStatistics()
          ? ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT
          : ControllerWorkerStagePhase.READING_INPUT,
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
                        .filter(stagePhase -> stagePhase.equals(ControllerWorkerStagePhase.RESULTS_READY))
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
   * Sets {@link #resultPartitions} (always) and {@link #resultPartitionBoundaries} (if doing a global sort) without
   * using key statistics. Called by the constructor.
   *
   * If {@link StageDefinition#mustGatherResultKeyStatistics()} is true, this method must not be called.
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
      final ShuffleSpec shuffleSpec = stageDef.getShuffleSpec();

      if (shuffleSpec.kind() == ShuffleKind.GLOBAL_SORT) {
        if (((GlobalSortShuffleSpec) shuffleSpec).mustGatherResultKeyStatistics()
            && !allPartialKeyInformationFetched()) {
          throw new ISE("Cannot generate result partitions without all worker key statistics");
        }

        final Either<Long, ClusterByPartitions> maybeResultPartitionBoundaries =
            stageDef.generatePartitionBoundariesForShuffle(null);

        if (maybeResultPartitionBoundaries.isError()) {
          failForReason(new TooManyPartitionsFault(stageDef.getMaxPartitionCount()));
          return;
        }

        resultPartitionBoundaries = maybeResultPartitionBoundaries.valueOrThrow();
        resultPartitions = ReadablePartitions.striped(
            stageNumber,
            workerInputs.workers(),
            resultPartitionBoundaries.size()
        );
      } else {
        if (shuffleSpec.kind() == ShuffleKind.MIX) {
          resultPartitionBoundaries = ClusterByPartitions.oneUniversalPartition();
        }
        resultPartitions = ReadablePartitions.striped(
            stageNumber,
            workerInputs.workers(),
            shuffleSpec.partitionCount()
        );
      }
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
   * True if all {@link PartialKeyStatisticsInformation} are present for a shuffling stage which require statistics, else false.
   * If the stage does not gather result statistics, we return a true.
   */
  public boolean allPartialKeyInformationFetched()
  {
    if (!stageDef.mustGatherResultKeyStatistics()) {
      return true;
    }
    return workerToPhase.values()
                        .stream()
                        .filter(stagePhase -> stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_ALL_KEY_STATS_TO_BE_FETCHED)
                                              || stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_FETCHING_ALL_KEY_STATS)
                                              || stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES)
                                              || stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT)
                                              || stagePhase.equals(ControllerWorkerStagePhase.RESULTS_READY))
                        .count()
           == workerCount;
  }

  /**
   * True if all workers are done reading their inputs.
   */
  public boolean allWorkersDoneReadingInput()
  {
    for (final ControllerWorkerStagePhase phase : workerToPhase.values()) {
      if (phase != ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT
          && phase != ControllerWorkerStagePhase.RESULTS_READY) {
        return false;
      }
    }

    return true;
  }

  /**
   * True if all {@link org.apache.druid.msq.kernel.WorkOrder} are sent else false.
   */
  private boolean allWorkOrdersSent()
  {
    return workerToPhase.values()
                        .stream()
                        .filter(stagePhase ->
                                    stagePhase.equals(ControllerWorkerStagePhase.READING_INPUT)
                                    || stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_ALL_KEY_STATS_TO_BE_FETCHED)
                                    || stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_FETCHING_ALL_KEY_STATS)
                                    || stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES)
                                    || stagePhase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT)
                                    || stagePhase.equals(ControllerWorkerStagePhase.RESULTS_READY)
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
      throw new IAE("Cannot transition stage[%s] from[%s] to[%s]", stageDef.getId(), phase, newPhase);
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

    if (workerToPhase.get(workerNumber).equals(ControllerWorkerStagePhase.RESULTS_READY)
        || workerToPhase.get(workerNumber).equals(ControllerWorkerStagePhase.FINISHED)) {
      // do nothing
      return false;
    }
    workerToPhase.put(workerNumber, ControllerWorkerStagePhase.NEW);
    transitionTo(ControllerStagePhase.RETRYING);
    return true;
  }


  private boolean isTrackingWorker(int workerNumber)
  {
    return workerToPhase.get(workerNumber) != null;
  }

  /**
   * Returns the workers who are ready with {@link ClusterByStatisticsSnapshot}
   */
  public Set<Integer> getWorkersToFetchClusterStatisticsFrom()
  {
    Set<Integer> workersToFetchStats = new HashSet<>();
    workerToPhase.forEach((worker, phase) -> {
      if (phase.equals(ControllerWorkerStagePhase.PRESHUFFLE_WAITING_FOR_ALL_KEY_STATS_TO_BE_FETCHED)) {
        workersToFetchStats.add(worker);
      }
    });
    return workersToFetchStats;
  }

  /**
   * Transitions the worker to {@link ControllerWorkerStagePhase#PRESHUFFLE_FETCHING_ALL_KEY_STATS) indicating fetching has begun.
   */
  public void startFetchingStatsFromWorker(int worker)
  {
    ControllerWorkerStagePhase workerStagePhase = workerToPhase.get(worker);
    if (ControllerWorkerStagePhase.PRESHUFFLE_FETCHING_ALL_KEY_STATS.canTransitionFrom(workerStagePhase)) {
      workerToPhase.put(worker, ControllerWorkerStagePhase.PRESHUFFLE_FETCHING_ALL_KEY_STATS);
    } else {
      throw new ISE(
          "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
          worker,
          (stageDef.getStageNumber()),
          ControllerWorkerStagePhase.PRESHUFFLE_FETCHING_ALL_KEY_STATS,
          workerStagePhase

      );
    }
  }

  /**
   * Takes a list of sorted {@link ClusterByPartitions} {@param timeSketchPartitions} and adds it to a sorted list
   * {@param finalPartitionBoundaries}. If {@param finalPartitionBoundaries} is not empty, the end time of the last
   * partition of {@param finalPartitionBoundaries} is changed to abut with the starting time of the first partition
   * of {@param timeSketchPartitions}.
   * <p>
   * This is used to make the partitions generated continuous.
   */
  private void abutAndAppendPartitionBoundaries(
      List<ClusterByPartition> finalPartitionBoundaries,
      List<ClusterByPartition> timeSketchPartitions
  )
  {
    if (!finalPartitionBoundaries.isEmpty()) {
      // Stitch up the end time of the last partition with the start time of the first partition.
      ClusterByPartition clusterByPartition = finalPartitionBoundaries.remove(finalPartitionBoundaries.size() - 1);
      finalPartitionBoundaries.add(new ClusterByPartition(
          clusterByPartition.getStart(),
          timeSketchPartitions.get(0).getStart()
      ));
    }
    finalPartitionBoundaries.addAll(timeSketchPartitions);
  }

  /**
   * Gets the partition size from an {@link Either}. If it is an error, the long denotes the number of partitions
   * (in the case of creating too many partitions), otherwise checks the size of the list.
   */
  private static long getPartitionCountFromEither(Either<Long, ClusterByPartitions> either)
  {
    if (either.isError()) {
      return either.error();
    } else {
      return either.valueOrThrow().size();
    }
  }
}
