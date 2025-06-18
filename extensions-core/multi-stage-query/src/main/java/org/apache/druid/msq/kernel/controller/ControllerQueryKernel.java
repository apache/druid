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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.ExtraInfoHolder;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.exec.QueryValidator;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Kernel for the controller of a multi-stage query.
 * <p>
 * Instances of this class are state machines for query execution. Kernels do not do any RPC or deal with any data.
 * This separation of decision-making from the "real world" allows the decision-making to live in one,
 * easy-to-follow place.
 *
 * @see org.apache.druid.msq.kernel.worker.WorkerStageKernel state machine on the worker side
 */
public class ControllerQueryKernel
{
  private static final Logger log = new Logger(ControllerQueryKernel.class);

  private final QueryDefinition queryDef;
  private final ControllerQueryKernelConfig config;

  /**
   * Stage ID -> tracker for that stage. An extension of the state of this kernel.
   */
  private final Map<StageId, ControllerStageTracker> stageTrackers = new HashMap<>();

  /**
   * Stage ID -> stages that flow *into* that stage. Computed by {@link ControllerQueryKernelUtils#computeStageInflowMap}.
   */
  private final ImmutableMap<StageId, Set<StageId>> inflowMap;

  /**
   * Stage ID -> stages that *depend on* that stage. Computed by {@link ControllerQueryKernelUtils#computeStageOutflowMap}.
   */
  private final ImmutableMap<StageId, Set<StageId>> outflowMap;

  /**
   * Maintains a running map of (stageId -> pending inflow stages) which need to be completed to provision the stage
   * corresponding to the stageId. After initializing, if the value of the entry becomes an empty set, it is removed
   * from the map, and the removed entry is added to {@link #stageGroupQueue}.
   */
  private final Map<StageId, SortedSet<StageId>> pendingInflowMap;

  /**
   * Maintains a running count of (stageId -> outflow stages pending on its results). After initializing, if
   * the value of the entry becomes an empty set, it is removed from the map and the removed entry is added to
   * {@link #effectivelyFinishedStages}.
   */
  private final Map<StageId, SortedSet<StageId>> pendingOutflowMap;

  /**
   * Stage groups, in the order that we will run them. Each group is a set of stages that internally uses
   * {@link OutputChannelMode#MEMORY} for communication. (The final stage may use a different
   * {@link OutputChannelMode}. In particular, if a stage group has a single stage, it may use any
   * {@link OutputChannelMode}.)
   */
  private final Queue<StageGroup> stageGroupQueue;

  /**
   * Tracks those stages which are ready to begin executing. Populated by {@link #registerStagePhaseChange}.
   */
  private final Set<StageId> readyToRunStages = new HashSet<>();

  /**
   * Tracks the stageIds which can be finished. Once returned by {@link #getEffectivelyFinishedStageIds()}, it gets
   * cleared and not tracked anymore in this Set.
   */
  private final Set<StageId> effectivelyFinishedStages = new HashSet<>();

  /**
   * Map<StageId, Map <WorkerNumber, WorkOrder>>
   * Stores the work order per worker per stage so that we can retrieve that in case of worker retry
   */
  private final Map<StageId, Int2ObjectMap<WorkOrder>> stageWorkOrders = new HashMap<>();

  /**
   * Tracks the output channel mode for each stage.
   */
  private final Map<StageId, OutputChannelMode> stageOutputChannelModes = new HashMap<>();

  /**
   * {@link MSQFault#getErrorCode()} which are retried.
   */
  private static final Set<String> RETRIABLE_ERROR_CODES = ImmutableSet.of(
      CanceledFault.CODE,
      UnknownFault.CODE,
      WorkerRpcFailedFault.CODE
  );

  public ControllerQueryKernel(
      final QueryDefinition queryDef,
      final ControllerQueryKernelConfig config
  )
  {
    this.queryDef = queryDef;
    this.config = config;
    this.inflowMap = ImmutableMap.copyOf(ControllerQueryKernelUtils.computeStageInflowMap(queryDef));
    this.outflowMap = ImmutableMap.copyOf(ControllerQueryKernelUtils.computeStageOutflowMap(queryDef));

    // pendingInflowMap and pendingOutflowMap are wholly separate from inflowMap, so we can edit the Sets.
    this.pendingInflowMap = ControllerQueryKernelUtils.computeStageInflowMap(queryDef);
    this.pendingOutflowMap = ControllerQueryKernelUtils.computeStageOutflowMap(queryDef);

    this.stageGroupQueue = new ArrayDeque<>(ControllerQueryKernelUtils.computeStageGroups(queryDef, config));
    initializeReadyToRunStages();
  }

  /**
   * Creates new kernels, if they can be initialized, and returns the tracked kernels which are in NEW phase
   */
  public List<StageId> createAndGetNewStageIds(
      final InputSpecSlicerFactory slicerFactory,
      final WorkerAssignmentStrategy assignmentStrategy,
      final long maxInputBytesPerWorker
  )
  {
    createNewKernels(
        slicerFactory,
        assignmentStrategy,
        maxInputBytesPerWorker
    );

    return stageTrackers.values()
                        .stream()
                        .filter(controllerStageTracker -> controllerStageTracker.getPhase() == ControllerStagePhase.NEW)
                        .map(stageTracker -> stageTracker.getStageDefinition().getId())
                        .collect(Collectors.toList());
  }

  /**
   * @return Stage kernels in this query kernel which can be safely cleaned up and marked as FINISHED. This returns the
   * kernel corresponding to a particular stage only once, to reduce the number of stages to iterate through.
   * It is expectant of the caller to eventually mark the stage as {@link ControllerStagePhase#FINISHED} after fetching
   * the stage tracker
   */
  public List<StageId> getEffectivelyFinishedStageIds()
  {
    return ImmutableList.copyOf(effectivelyFinishedStages);
  }

  /**
   * Returns all the kernels which have been initialized and are being tracked
   */
  public List<StageId> getActiveStages()
  {
    return ImmutableList.copyOf(stageTrackers.keySet());
  }

  /**
   * Returns the number of stages that are active and in non-terminal phases.
   */
  public int getNonTerminalActiveStageCount()
  {
    int n = 0;

    for (final ControllerStageTracker tracker : stageTrackers.values()) {
      if (!tracker.getPhase().isTerminal() && tracker.getPhase() != ControllerStagePhase.RESULTS_READY) {
        n++;
      }
    }

    return n;
  }

  /**
   * Returns a stage's kernel corresponding to a particular stage number
   */
  public StageId getStageId(final int stageNumber)
  {
    return new StageId(queryDef.getQueryId(), stageNumber);
  }

  /**
   * Returns true if query needs no further processing, i.e. if final stage is successful or if any of the stages have
   * been failed
   */
  public boolean isDone()
  {
    return isSuccess()
           || stageTrackers.values().stream().anyMatch(tracker -> tracker.getPhase() == ControllerStagePhase.FAILED);
  }

  /**
   * Marks all the successful terminal stages to completion, so that the queryKernel shows a canonical view of
   * phases of the stages once it completes
   */
  public void markSuccessfulTerminalStagesAsFinished()
  {
    for (final StageId stageId : getActiveStages()) {
      ControllerStagePhase phase = getStagePhase(stageId);
      // While the following conditional is redundant currently, it makes logical sense to mark all the "successful
      // terminal phases" to FINISHED at the end, hence the if clause. Inside the conditional, depending on the
      // terminal phase it resides in, we synthetically mark it to completion (and therefore we need to check which
      // stage it is precisely in)
      if (phase.isSuccess()) {
        if (phase == ControllerStagePhase.RESULTS_READY) {
          finishStage(stageId, false);
        }
      }
    }
  }

  /**
   * Returns true if all the stages comprising the query definition have been successful in producing their results.
   */
  public boolean isSuccess()
  {
    return stageTrackers.size() == queryDef.getStageDefinitions().size()
           && stageTrackers.values()
                           .stream()
                           .allMatch(tracker -> tracker.getPhase() == ControllerStagePhase.FINISHED);
  }

  /**
   * Creates a list of work orders, corresponding to each worker, for a particular stageNumber
   */
  public Int2ObjectMap<WorkOrder> createWorkOrders(
      final int stageNumber,
      @Nullable final Int2ObjectMap<Object> extraInfos
  )
  {
    final Int2ObjectMap<WorkOrder> workerToWorkOrder = new Int2ObjectAVLTreeMap<>();
    final ControllerStageTracker stageKernel = getStageTrackerOrThrow(getStageId(stageNumber));
    final WorkerInputs workerInputs = stageKernel.getWorkerInputs();
    final OutputChannelMode outputChannelMode = stageOutputChannelModes.get(stageKernel.getStageDefinition().getId());

    for (int workerNumber : workerInputs.workers()) {
      final Object extraInfo = extraInfos != null ? extraInfos.get(workerNumber) : null;

      //noinspection unchecked
      final ExtraInfoHolder<?> extraInfoHolder =
          stageKernel.getStageDefinition().getProcessor().makeExtraInfoHolder(extraInfo);

      final WorkOrder workOrder = new WorkOrder(
          queryDef,
          stageNumber,
          workerNumber,
          workerInputs.inputsForWorker(workerNumber),
          extraInfoHolder,
          config.getWorkerIds(),
          outputChannelMode,
          config.getWorkerContextMap()
      );

      QueryValidator.validateWorkOrder(workOrder);
      workerToWorkOrder.put(workerNumber, workOrder);
    }
    stageWorkOrders.put(new StageId(queryDef.getQueryId(), stageNumber), workerToWorkOrder);
    return workerToWorkOrder;
  }

  private void createNewKernels(
      final InputSpecSlicerFactory slicerFactory,
      final WorkerAssignmentStrategy assignmentStrategy,
      final long maxInputBytesPerWorker
  )
  {
    StageGroup stageGroup;

    while ((stageGroup = stageGroupQueue.peek()) != null) {
      if (readyToRunStages.contains(stageGroup.first())
          && getNonTerminalActiveStageCount() + stageGroup.size() <= config.getMaxConcurrentStages()) {
        // There is room to launch this stage group.
        stageGroupQueue.poll();

        for (final StageId stageId : stageGroup.stageIds()) {
          // Create a tracker for this stage.
          stageTrackers.put(
              stageId,
              createStageTracker(
                  stageId,
                  slicerFactory,
                  assignmentStrategy,
                  maxInputBytesPerWorker
              )
          );

          // Store output channel mode.
          stageOutputChannelModes.put(
              stageId,
              stageGroup.stageOutputChannelMode(stageId)
          );
        }

        stageGroup.stageIds().forEach(readyToRunStages::remove);
      } else {
        break;
      }
    }
  }

  private ControllerStageTracker createStageTracker(
      final StageId stageId,
      final InputSpecSlicerFactory slicerFactory,
      final WorkerAssignmentStrategy assignmentStrategy,
      final long maxInputBytesPerWorker
  )
  {
    final Int2IntMap stageWorkerCountMap = new Int2IntAVLTreeMap();
    final Int2ObjectMap<ReadablePartitions> stagePartitionsMap = new Int2ObjectAVLTreeMap<>();
    final Int2ObjectMap<OutputChannelMode> stageOutputChannelModeMap = new Int2ObjectAVLTreeMap<>();

    for (final ControllerStageTracker stageTracker : stageTrackers.values()) {
      final int stageNumber = stageTracker.getStageDefinition().getStageNumber();
      stageWorkerCountMap.put(stageNumber, stageTracker.getWorkerInputs().workerCount());

      if (stageTracker.hasResultPartitions()) {
        stagePartitionsMap.put(stageNumber, stageTracker.getResultPartitions());
      }

      final OutputChannelMode outputChannelMode =
          stageOutputChannelModes.get(stageTracker.getStageDefinition().getId());

      if (outputChannelMode != null) {
        stageOutputChannelModeMap.put(stageNumber, outputChannelMode);
      }
    }

    return ControllerStageTracker.create(
        getStageDefinition(stageId),
        stageWorkerCountMap,
        slicerFactory.makeSlicer(stagePartitionsMap, stageOutputChannelModeMap),
        assignmentStrategy,
        config.getMaxRetainedPartitionSketchBytes(),
        maxInputBytesPerWorker
    );
  }

  /**
   * Called by the constructor. Initializes {@link #readyToRunStages} and removes any ready-to-run stages from
   * the {@link #pendingInflowMap}.
   */
  private void initializeReadyToRunStages()
  {
    final List<StageId> readyStages = new ArrayList<>();
    final Iterator<Map.Entry<StageId, SortedSet<StageId>>> pendingInflowIterator =
        pendingInflowMap.entrySet().iterator();

    while (pendingInflowIterator.hasNext()) {
      final Map.Entry<StageId, SortedSet<StageId>> stageToInflowStages = pendingInflowIterator.next();
      if (stageToInflowStages.getValue().isEmpty()) {
        readyStages.add(stageToInflowStages.getKey());
        pendingInflowIterator.remove();
      }
    }

    readyToRunStages.addAll(readyStages);
  }

  /**
   * Returns the definition of a given stage.
   *
   * @throws NullPointerException if there is no stage with the given ID
   */
  public StageDefinition getStageDefinition(final StageId stageId)
  {
    return queryDef.getStageDefinition(stageId);
  }

  /**
   * Returns the {@link OutputChannelMode} for a given stage.
   *
   * @throws IllegalStateException if there is no stage with the given ID
   */
  public OutputChannelMode getStageOutputChannelMode(final StageId stageId)
  {
    final OutputChannelMode outputChannelMode = stageOutputChannelModes.get(stageId);
    if (outputChannelMode == null) {
      throw new ISE("No such stage[%s]", stageId);
    }

    return outputChannelMode;
  }

  /**
   * Whether query results are readable.
   */
  public boolean canReadQueryResults()
  {
    final StageId finalStageId = queryDef.getFinalStageDefinition().getId();
    final ControllerStageTracker stageTracker = stageTrackers.get(finalStageId);
    if (stageTracker == null) {
      return false;
    } else {
      final OutputChannelMode outputChannelMode = stageOutputChannelModes.get(finalStageId);
      if (outputChannelMode == OutputChannelMode.MEMORY) {
        return stageTracker.getPhase().isRunning();
      } else {
        return stageTracker.getPhase() == ControllerStagePhase.RESULTS_READY;
      }
    }
  }

  // Following section contains the methods which delegate to appropriate stage kernel

  /**
   * Delegates call to {@link ControllerStageTracker#getPhase()}
   */
  public ControllerStagePhase getStagePhase(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).getPhase();
  }

  /**
   * Returns whether a particular stage is finished. Stages can finish early if their outputs are no longer needed.
   */
  public boolean isStageFinished(final StageId stageId)
  {
    return getStagePhase(stageId) == ControllerStagePhase.FINISHED;
  }

  /**
   * Delegates call to {@link ControllerStageTracker#hasResultPartitions()}
   */
  public boolean doesStageHaveResultPartitions(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).hasResultPartitions();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getResultPartitions()}
   */
  public ReadablePartitions getResultPartitionsForStage(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).getResultPartitions();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getWorkersToSendPartitionBoundaries()}
   */
  public IntSet getWorkersToSendPartitionBoundaries(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).getWorkersToSendPartitionBoundaries();
  }

  /**
   * Delegates call to {@link ControllerQueryKernel#workOrdersSentForWorker(StageId, int)}
   */
  public void workOrdersSentForWorker(final StageId stageId, int worker)
  {
    doWithStageTracker(stageId, stageTracker -> stageTracker.workOrderSentForWorker(worker));
  }

  /**
   * Delegates call to {@link ControllerStageTracker#partitionBoundariesSentForWorker(int)} ()}
   */
  public void partitionBoundariesSentForWorker(final StageId stageId, int worker)
  {
    doWithStageTracker(stageId, stageTracker -> stageTracker.partitionBoundariesSentForWorker(worker));
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getResultPartitionBoundaries()}
   */
  public ClusterByPartitions getResultPartitionBoundariesForStage(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).getResultPartitionBoundaries();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getCompleteKeyStatisticsInformation()}
   */
  public CompleteKeyStatisticsInformation getCompleteKeyStatisticsInformation(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).getCompleteKeyStatisticsInformation();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#collectorEncounteredAnyMultiValueField()}
   */
  public boolean hasStageCollectorEncounteredAnyMultiValueField(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).collectorEncounteredAnyMultiValueField();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getResultObject()}
   */
  public Object getResultObjectForStage(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).getResultObject();
  }

  /**
   * Checks if the stage can be started, delegates call to {@link ControllerStageTracker#start()} for internal phase
   * transition and registers the transition in this queryKernel. Work orders need to be created via {@link ControllerQueryKernel#createWorkOrders(int, Int2ObjectMap)} before calling this method.
   */
  public void startStage(final StageId stageId)
  {
    if (stageWorkOrders.get(stageId) == null) {
      throw new ISE("Work order not present for stage[%s]", stageId);
    }

    doWithStageTracker(stageId, stageTracker -> {
      if (stageTracker.getPhase() != ControllerStagePhase.NEW) {
        throw new ISE("Cannot start the stage: [%s]", stageId);
      }

      stageTracker.start();
    });
  }

  /**
   * Checks if the stage can be finished, delegates call to {@link ControllerStageTracker#finish()} for internal phase
   * transition and registers the transition in this query kernel
   * <p>
   * If the method is called with strict = true, we confirm if the stage can be marked as finished or else
   * throw illegal argument exception
   */
  public void finishStage(final StageId stageId, final boolean strict)
  {
    if (strict && !effectivelyFinishedStages.contains(stageId)) {
      throw new IAE("Cannot mark the stage: [%s] finished", stageId);
    }
    doWithStageTracker(stageId, stageTracker -> {
      stageTracker.finish();
      effectivelyFinishedStages.remove(stageId);
    });
    stageWorkOrders.remove(stageId);
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getWorkerInputs()}
   */
  public WorkerInputs getWorkerInputsForStage(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).getWorkerInputs();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#addPartialKeyInformationForWorker(int, PartialKeyStatisticsInformation)}.
   * If calling this causes transition for the stage kernel, then this gets registered in this query kernel
   */
  public void addPartialKeyStatisticsForStageAndWorker(
      final StageId stageId,
      final int workerNumber,
      final PartialKeyStatisticsInformation partialKeyStatisticsInformation
  )
  {
    doWithStageTracker(stageId, stageTracker ->
        stageTracker.addPartialKeyInformationForWorker(workerNumber, partialKeyStatisticsInformation));
  }

  /**
   * Delegates call to {@link ControllerStageTracker#addPartialKeyInformationForWorker(int, PartialKeyStatisticsInformation)}.
   * If calling this causes transition for the stage kernel, then this gets registered in this query kernel
   */
  public void setDoneReadingInputForStageAndWorker(final StageId stageId, final int workerNumber)
  {
    doWithStageTracker(stageId, stageTracker -> stageTracker.setDoneReadingInputForWorker(workerNumber));
  }

  /**
   * Delegates call to {@link ControllerStageTracker#setResultsCompleteForWorker(int, Object)}. If calling this causes
   * transition for the stage kernel, then this gets registered in this query kernel
   */
  public void setResultsCompleteForStageAndWorker(
      final StageId stageId,
      final int workerNumber,
      final Object resultObject
  )
  {
    doWithStageTracker(stageId, stageTracker -> stageTracker.setResultsCompleteForWorker(workerNumber, resultObject));
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getFailureReason()}
   */
  public MSQFault getFailureReasonForStage(final StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).getFailureReason();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#fail()} and registers this transition to FAILED in this query kernel
   */
  public void failStage(final StageId stageId)
  {
    doWithStageTracker(stageId, ControllerStageTracker::fail);
  }

  /**
   * Returns the set of all worker numbers that have participated in work done so far by this query.
   */
  public IntSet getAllParticipatingWorkers()
  {
    final IntSet retVal = new IntAVLTreeSet();

    for (final ControllerStageTracker tracker : stageTrackers.values()) {
      retVal.addAll(tracker.getWorkerInputs().workers());
    }

    return retVal;
  }

  /**
   * Fetches and returns the stage kernel corresponding to the provided stage id, else throws {@link IAE}
   */
  private ControllerStageTracker getStageTrackerOrThrow(StageId stageId)
  {
    ControllerStageTracker stageTracker = stageTrackers.get(stageId);
    if (stageTracker == null) {
      throw new IAE("Cannot find kernel corresponding to stage [%s] in query [%s]", stageId, queryDef.getQueryId());
    }
    return stageTracker;
  }

  private WorkOrder getWorkOrder(int workerNumber, StageId stageId)
  {
    Int2ObjectMap<WorkOrder> stageWorkOrder = stageWorkOrders.get(stageId);

    if (stageWorkOrder == null) {
      throw new ISE("Stage[%d] work orders not found", stageId.getStageNumber());
    }

    WorkOrder workOrder = stageWorkOrder.get(workerNumber);
    if (workOrder == null) {
      throw new ISE("Work order for worker[%d] not found for stage[%d]", workerNumber, stageId.getStageNumber());
    }
    return workOrder;
  }

  /**
   * Whether a given stage is ready to stream results to consumer stages upon transition to "newPhase".
   */
  private boolean readyToReadResults(final StageId stageId, final ControllerStagePhase newPhase)
  {
    if (stageOutputChannelModes.get(stageId) == OutputChannelMode.MEMORY) {
      if (getStageDefinition(stageId).doesSortDuringShuffle()) {
        // Sorting stages start producing output when they finish reading their input.
        return newPhase.isDoneReadingInput();
      } else {
        // Non-sorting stages start producing output immediately.
        return newPhase == ControllerStagePhase.NEW;
      }
    } else {
      return newPhase == ControllerStagePhase.RESULTS_READY;
    }
  }

  private void doWithStageTracker(final StageId stageId, final Consumer<ControllerStageTracker> fn)
  {
    final ControllerStageTracker stageTracker = getStageTrackerOrThrow(stageId);
    final ControllerStagePhase phase = stageTracker.getPhase();
    fn.accept(stageTracker);

    if (phase != stageTracker.getPhase()) {
      registerStagePhaseChange(stageId, stageTracker.getPhase());
    }
  }

  /**
   * Whenever a stage kernel changes its phase, the change must be "registered" by calling this method with the stageId
   * and the new phase.
   */
  private void registerStagePhaseChange(final StageId stageId, final ControllerStagePhase newPhase)
  {
    if (readyToReadResults(stageId, newPhase)) {
      // Once results from a stage are readable, remove this stage from pendingInflowMap and potentially mark
      // dependent stages as ready to run.
      for (StageId dependentStageId : outflowMap.get(stageId)) {
        if (!pendingInflowMap.containsKey(dependentStageId)) {
          continue;
        }
        pendingInflowMap.get(dependentStageId).remove(stageId);
        // Check the dependent stage. If it has no dependencies left, it can be marked as to be initialized
        if (pendingInflowMap.get(dependentStageId).isEmpty()) {
          readyToRunStages.add(dependentStageId);
          pendingInflowMap.remove(dependentStageId);
        }
      }
    }

    if (newPhase.isSuccess() || (!config.isFaultTolerant() && newPhase.isDoneReadingInput())) {
      // Once a stage no longer needs its input, we consider marking input stages as finished.
      for (StageId inputStage : inflowMap.get(stageId)) {
        if (!pendingOutflowMap.containsKey(inputStage)) {
          continue;
        }
        pendingOutflowMap.get(inputStage).remove(stageId);
        // If no more stage is dependent on the inputStage's results, it can be safely transitioned to FINISHED
        if (pendingOutflowMap.get(inputStage).isEmpty()) {
          pendingOutflowMap.remove(inputStage);

          // Mark input stage as effectively finished, if it's ready to finish.
          // This leads to a later transition to FINISHED.
          if (ControllerStagePhase.FINISHED.canTransitionFrom(stageTrackers.get(inputStage).getPhase())) {
            effectivelyFinishedStages.add(inputStage);
          }
        }
      }
    }

    // Mark stage as effectively finished, if it has no dependencies waiting for it.
    // This leads to a later transition to FINISHED.
    final boolean hasDependentStages =
        pendingOutflowMap.containsKey(stageId) && !pendingOutflowMap.get(stageId).isEmpty();

    if (!hasDependentStages) {
      final boolean isFinalStage = queryDef.getFinalStageDefinition().getId().equals(stageId);

      if (isFinalStage && newPhase == ControllerStagePhase.RESULTS_READY) {
        // Final stage must run to completion (RESULTS_READY).
        effectivelyFinishedStages.add(stageId);
      } else if (!isFinalStage && ControllerStagePhase.FINISHED.canTransitionFrom(newPhase)) {
        // Other stages can exit early (e.g. if there is a LIMIT).
        effectivelyFinishedStages.add(stageId);
      }
    }
  }

  @VisibleForTesting
  ControllerStageTracker getControllerStageTracker(int stageNumber)
  {
    return stageTrackers.get(new StageId(queryDef.getQueryId(), stageNumber));
  }

  /**
   * Checks the {@link MSQFault#getErrorCode()} is eligible for retry.
   * <br/>
   * If yes, transitions the stage to{@link ControllerStagePhase#RETRYING} and returns all the {@link WorkOrder}
   * <br/>
   * else throw {@link MSQException}
   *
   * @param workerNumber
   * @param msqFault
   *
   * @return List of {@link WorkOrder} that needs to be retried.
   */
  public List<WorkOrder> getWorkInCaseWorkerEligibleForRetryElseThrow(int workerNumber, MSQFault msqFault)
  {
    if (isRetriableFault(msqFault)) {
      return getWorkInCaseWorkerEligibleForRetry(workerNumber);
    } else {
      throw new MSQException(msqFault);
    }
  }

  public static boolean isRetriableFault(MSQFault msqFault)
  {
    final String errorCode;
    if (msqFault instanceof WorkerFailedFault) {
      errorCode = MSQFaultUtils.getErrorCodeFromMessage((((WorkerFailedFault) msqFault).getErrorMsg()));
    } else {
      errorCode = msqFault.getErrorCode();
    }
    log.debug("Parsed out errorCode[%s] to check eligibility for retry", errorCode);
    return RETRIABLE_ERROR_CODES.contains(errorCode);
  }

  /**
   * Gets all the stages currently being tracked and filters out all effectively finished stages.
   * <br/>
   * From the remaining stages, checks if (stage,worker) needs to be retried.
   * <br/>
   * If yes adds the workOrder for that stage to the return list and transitions the stage kernel to {@link ControllerStagePhase#RETRYING}
   *
   * @param worker
   *
   * @return List of {@link WorkOrder} that needs to be retried.
   */
  private List<WorkOrder> getWorkInCaseWorkerEligibleForRetry(int worker)
  {
    List<StageId> trackedSet = new ArrayList<>(getActiveStages());
    trackedSet.removeAll(effectivelyFinishedStages);

    List<WorkOrder> workOrders = new ArrayList<>();

    for (StageId stageId : trackedSet) {
      doWithStageTracker(stageId, stageTracker -> {
        if (ControllerStagePhase.RETRYING.canTransitionFrom(stageTracker.getPhase())
            && stageTracker.retryIfNeeded(worker)) {
          workOrders.add(getWorkOrder(worker, stageId));
        }
      });
    }
    return workOrders;
  }

  /**
   * For each stage, fetches the workers who are ready with their {@link ClusterByStatisticsSnapshot}
   */
  public Map<StageId, Set<Integer>> getStagesAndWorkersToFetchClusterStats()
  {
    List<StageId> trackedSet = new ArrayList<>(getActiveStages());
    trackedSet.removeAll(getEffectivelyFinishedStageIds());

    Map<StageId, Set<Integer>> stageToWorkers = new HashMap<>();

    for (StageId stageId : trackedSet) {
      ControllerStageTracker controllerStageTracker = getStageTrackerOrThrow(stageId);
      if (controllerStageTracker.getStageDefinition().mustGatherResultKeyStatistics()) {
        stageToWorkers.put(stageId, controllerStageTracker.getWorkersToFetchClusterStatisticsFrom());
      }
    }
    return stageToWorkers;
  }


  /**
   * Delegates call to {@link ControllerStageTracker#startFetchingStatsFromWorker(int)} for each worker
   */
  public void startFetchingStatsFromWorker(StageId stageId, Set<Integer> workers)
  {
    doWithStageTracker(stageId, stageTracker -> {
      for (int worker : workers) {
        stageTracker.startFetchingStatsFromWorker(worker);
      }
    });
  }

  /**
   * Delegates call to {@link ControllerStageTracker#mergeClusterByStatisticsCollectorForAllTimeChunks(int, ClusterByStatisticsSnapshot)}
   */
  public void mergeClusterByStatisticsCollectorForAllTimeChunks(
      StageId stageId,
      int workerNumber,
      ClusterByStatisticsSnapshot clusterByStatsSnapshot
  )
  {
    doWithStageTracker(stageId, stageTracker ->
        stageTracker.mergeClusterByStatisticsCollectorForAllTimeChunks(workerNumber, clusterByStatsSnapshot));
  }

  /**
   * Delegates call to {@link ControllerStageTracker#mergeClusterByStatisticsCollectorForTimeChunk(int, Long, ClusterByStatisticsSnapshot)}
   */

  public void mergeClusterByStatisticsCollectorForTimeChunk(
      StageId stageId,
      int workerNumber,
      Long timeChunk,
      ClusterByStatisticsSnapshot clusterByStatsSnapshot
  )
  {
    doWithStageTracker(stageId, stageTracker ->
        stageTracker.mergeClusterByStatisticsCollectorForTimeChunk(workerNumber, timeChunk, clusterByStatsSnapshot));
  }

  /**
   * Delegates call to {@link ControllerStageTracker#allPartialKeyInformationFetched()}
   */
  public boolean allPartialKeyInformationPresent(StageId stageId)
  {
    return getStageTrackerOrThrow(stageId).allPartialKeyInformationFetched();
  }

  /**
   * @return {@code true} if the stage output is empty, {@code false} if the stage output is non-empty,
   * or {@code null} for stages where cluster key statistics are not gathered or is incomplete
   */
  @Nullable
  public Boolean isStageOutputEmpty(final StageId stageId)
  {
    final CompleteKeyStatisticsInformation completeKeyStatistics = getCompleteKeyStatisticsInformation(stageId);
    if (completeKeyStatistics == null || !completeKeyStatistics.isComplete()) {
      return null;
    }
    return completeKeyStatistics.getTimeSegmentVsWorkerMap().size() == 0;
  }
}
