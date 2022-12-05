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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.QueryValidator;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.kernel.ExtraInfoHolder;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Kernel for the controller of a multi-stage query.
 *
 * Instances of this class are state machines for query execution. Kernels do not do any RPC or deal with any data.
 * This separation of decision-making from the "real world" allows the decision-making to live in one,
 * easy-to-follow place.
 *
 * @see org.apache.druid.msq.kernel.worker.WorkerStageKernel state machine on the worker side
 */
public class ControllerQueryKernel
{
  private final QueryDefinition queryDef;

  /**
   * Stage ID -> tracker for that stage. An extension of the state of this kernel.
   */
  private final Map<StageId, ControllerStageTracker> stageTracker = new HashMap<>();

  /**
   * Stage ID -> stages that flow *into* that stage. Computed by {@link #computeStageInflowMap}.
   */
  private final ImmutableMap<StageId, Set<StageId>> inflowMap;

  /**
   * Stage ID -> stages that *depend on* that stage. Computed by {@link #computeStageOutflowMap}.
   */
  private final ImmutableMap<StageId, Set<StageId>> outflowMap;

  /**
   * Maintains a running map of (stageId -> pending inflow stages) which need to be completed to provision the stage
   * corresponding to the stageId. After initializing, if the value of the entry becomes an empty set, it is removed
   * from the map, and the removed entry is added to {@link #readyToRunStages}.
   */
  private final Map<StageId, Set<StageId>> pendingInflowMap;

  /**
   * Maintains a running count of (stageId -> outflow stages pending on its results). After initializing, if
   * the value of the entry becomes an empty set, it is removed from the map and the removed entry is added to
   * {@link #effectivelyFinishedStages}.
   */
  private final Map<StageId, Set<StageId>> pendingOutflowMap;

  /**
   * Tracks those stages which can be initialized safely.
   */
  private final Set<StageId> readyToRunStages = new HashSet<>();

  /**
   * Tracks the stageIds which can be finished. Once returned by {@link #getEffectivelyFinishedStageIds()}, it gets
   * cleared and not tracked anymore in this Set.
   */
  private final Set<StageId> effectivelyFinishedStages = new HashSet<>();

  public ControllerQueryKernel(final QueryDefinition queryDef)
  {
    this.queryDef = queryDef;
    this.inflowMap = ImmutableMap.copyOf(computeStageInflowMap(queryDef));
    this.outflowMap = ImmutableMap.copyOf(computeStageOutflowMap(queryDef));

    // pendingInflowMap and pendingOutflowMap are wholly separate from inflowMap, so we can edit the Sets.
    this.pendingInflowMap = computeStageInflowMap(queryDef);
    this.pendingOutflowMap = computeStageOutflowMap(queryDef);

    initializeReadyToRunStages();
  }

  /**
   * Creates new kernels, if they can be initialized, and returns the tracked kernels which are in NEW phase
   */
  public List<StageId> createAndGetNewStageIds(
      final InputSpecSlicerFactory slicerFactory,
      final WorkerAssignmentStrategy assignmentStrategy
  )
  {
    final Int2IntMap stageWorkerCountMap = new Int2IntAVLTreeMap();
    final Int2ObjectMap<ReadablePartitions> stagePartitionsMap = new Int2ObjectAVLTreeMap<>();

    for (final ControllerStageTracker stageKernel : stageTracker.values()) {
      final int stageNumber = stageKernel.getStageDefinition().getStageNumber();
      stageWorkerCountMap.put(stageNumber, stageKernel.getWorkerInputs().workerCount());

      if (stageKernel.hasResultPartitions()) {
        stagePartitionsMap.put(stageNumber, stageKernel.getResultPartitions());
      }
    }

    createNewKernels(stageWorkerCountMap, slicerFactory.makeSlicer(stagePartitionsMap), assignmentStrategy);
    return stageTracker.values()
                       .stream()
                       .filter(controllerStageTracker -> controllerStageTracker.getPhase() == ControllerStagePhase.NEW)
                       .map(stageKernel -> stageKernel.getStageDefinition().getId())
                       .collect(Collectors.toList());
  }

  /**
   * @return Stage kernels in this query kernel which can be safely cleaned up and marked as FINISHED. This returns the
   * kernel corresponding to a particular stage only once, to reduce the number of stages to iterate through.
   * It is expectant of the caller to eventually mark the stage as {@link ControllerStagePhase#FINISHED} after fetching
   * the stage kernel
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
    return ImmutableList.copyOf(stageTracker.keySet());
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
    return Optional.ofNullable(stageTracker.get(queryDef.getFinalStageDefinition().getId()))
                   .filter(tracker -> ControllerStagePhase.isSuccessfulTerminalPhase(tracker.getPhase()))
                   .isPresent()
           || stageTracker.values().stream().anyMatch(tracker -> tracker.getPhase() == ControllerStagePhase.FAILED);
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
      if (ControllerStagePhase.isSuccessfulTerminalPhase(phase)) {
        if (phase == ControllerStagePhase.RESULTS_READY) {
          finishStage(stageId, false);
        }
      }
    }
  }

  /**
   * Returns true if all the stages comprising the query definition have been sucessful in producing their results
   */
  public boolean isSuccess()
  {
    return stageTracker.size() == queryDef.getStageDefinitions().size()
           && stageTracker.values()
                          .stream()
                          .allMatch(tracker -> ControllerStagePhase.isSuccessfulTerminalPhase(tracker.getPhase()));
  }

  /**
   * Creates a list of work orders, corresponding to each worker, for a particular stageNumber
   */
  public Int2ObjectMap<WorkOrder> createWorkOrders(
      final int stageNumber,
      @Nullable final Int2ObjectMap<Object> extraInfos
  )
  {
    final Int2ObjectMap<WorkOrder> retVal = new Int2ObjectAVLTreeMap<>();
    final ControllerStageTracker stageKernel = getStageKernelOrThrow(getStageId(stageNumber));

    final WorkerInputs workerInputs = stageKernel.getWorkerInputs();
    for (int workerNumber : workerInputs.workers()) {
      final Object extraInfo = extraInfos != null ? extraInfos.get(workerNumber) : null;

      //noinspection unchecked
      final ExtraInfoHolder<?> extraInfoHolder =
          stageKernel.getStageDefinition().getProcessorFactory().makeExtraInfoHolder(extraInfo);

      final WorkOrder workOrder = new WorkOrder(
          queryDef,
          stageNumber,
          workerNumber,
          workerInputs.inputsForWorker(workerNumber),
          extraInfoHolder
      );

      QueryValidator.validateWorkOrder(workOrder);
      retVal.put(workerNumber, workOrder);
    }

    return retVal;
  }

  private void createNewKernels(
      final Int2IntMap stageWorkerCountMap,
      final InputSpecSlicer slicer,
      final WorkerAssignmentStrategy assignmentStrategy
  )
  {
    for (final StageId nextStage : readyToRunStages) {
      // Create a tracker.
      final StageDefinition stageDef = queryDef.getStageDefinition(nextStage);
      final ControllerStageTracker stageKernel = ControllerStageTracker.create(
          stageDef,
          stageWorkerCountMap,
          slicer,
          assignmentStrategy
      );
      stageTracker.put(nextStage, stageKernel);
    }

    readyToRunStages.clear();
  }

  /**
   * Called by the constructor. Initializes {@link #readyToRunStages} and removes any ready-to-run stages from
   * the {@link #pendingInflowMap}.
   */
  private void initializeReadyToRunStages()
  {
    final Iterator<Map.Entry<StageId, Set<StageId>>> pendingInflowIterator = pendingInflowMap.entrySet().iterator();

    while (pendingInflowIterator.hasNext()) {
      Map.Entry<StageId, Set<StageId>> stageToInflowStages = pendingInflowIterator.next();
      if (stageToInflowStages.getValue().size() == 0) {
        readyToRunStages.add(stageToInflowStages.getKey());
        pendingInflowIterator.remove();
      }
    }
  }

  // Following section contains the methods which delegate to appropriate stage kernel

  /**
   * Delegates call to {@link ControllerStageTracker#getStageDefinition()}
   */
  public StageDefinition getStageDefinition(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getStageDefinition();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getPhase()}
   */
  public ControllerStagePhase getStagePhase(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getPhase();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#hasResultPartitions()}
   */
  public boolean doesStageHaveResultPartitions(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).hasResultPartitions();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getResultPartitions()}
   */
  public ReadablePartitions getResultPartitionsForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getResultPartitions();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getResultPartitionBoundaries()}
   */
  public ClusterByPartitions getResultPartitionBoundariesForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getResultPartitionBoundaries();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getCompleteKeyStatisticsInformation()}
   */
  public CompleteKeyStatisticsInformation getCompleteKeyStatisticsInformation(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getCompleteKeyStatisticsInformation();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#setClusterByPartitionBoundaries(ClusterByPartitions)} ()}
   */
  public void setClusterByPartitionBoundaries(final StageId stageId, ClusterByPartitions clusterByPartitions)
  {
    getStageKernelOrThrow(stageId).setClusterByPartitionBoundaries(clusterByPartitions);
  }

  /**
   * Delegates call to {@link ControllerStageTracker#collectorEncounteredAnyMultiValueField()}
   */
  public boolean hasStageCollectorEncounteredAnyMultiValueField(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).collectorEncounteredAnyMultiValueField();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getResultObject()}
   */
  public Object getResultObjectForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getResultObject();
  }

  /**
   * Checks if the stage can be started, delegates call to {@link ControllerStageTracker#start()} for internal phase
   * transition and registers the transition in this queryKernel
   */
  public void startStage(final StageId stageId)
  {
    final ControllerStageTracker stageKernel = getStageKernelOrThrow(stageId);
    if (stageKernel.getPhase() != ControllerStagePhase.NEW) {
      throw new ISE("Cannot start the stage: [%s]", stageId);
    }
    stageKernel.start();
    transitionStageKernel(stageId, ControllerStagePhase.READING_INPUT);
  }

  /**
   * Checks if the stage can be finished, delegates call to {@link ControllerStageTracker#finish()} for internal phase
   * transition and registers the transition in this query kernel
   *
   * If the method is called with strict = true, we confirm if the stage can be marked as finished or else
   * throw illegal argument exception
   */
  public void finishStage(final StageId stageId, final boolean strict)
  {
    if (strict && !effectivelyFinishedStages.contains(stageId)) {
      throw new IAE("Cannot mark the stage: [%s] finished", stageId);
    }
    getStageKernelOrThrow(stageId).finish();
    effectivelyFinishedStages.remove(stageId);
    transitionStageKernel(stageId, ControllerStagePhase.FINISHED);
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getWorkerInputs()}
   */
  public WorkerInputs getWorkerInputsForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getWorkerInputs();
  }

  /**
   * Delegates call to {@link ControllerStageTracker#addPartialKeyStatisticsForWorker(int, PartialKeyStatisticsInformation)}.
   * If calling this causes transition for the stage kernel, then this gets registered in this query kernel
   */
  public void addPartialKeyStatisticsForStageAndWorker(
      final StageId stageId,
      final int workerNumber,
      final PartialKeyStatisticsInformation partialKeyStatisticsInformation
  )
  {
    ControllerStageTracker stageKernel = getStageKernelOrThrow(stageId);
    ControllerStagePhase newPhase = stageKernel.addPartialKeyStatisticsForWorker(
        workerNumber,
        partialKeyStatisticsInformation
    );

    // If the kernel phase has transitioned, we need to account for that.
    switch (newPhase) {
      case MERGING_STATISTICS:
      case POST_READING:
      case FAILED:
        transitionStageKernel(stageId, newPhase);
        break;
    }
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
    if (getStageKernelOrThrow(stageId).setResultsCompleteForWorker(workerNumber, resultObject)) {
      transitionStageKernel(stageId, ControllerStagePhase.RESULTS_READY);
    }
  }

  /**
   * Delegates call to {@link ControllerStageTracker#getFailureReason()}
   */
  public MSQFault getFailureReasonForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getFailureReason();
  }

  public void failStageForReason(final StageId stageId, MSQFault fault)
  {
    getStageKernelOrThrow(stageId).failForReason(fault);
    transitionStageKernel(stageId, ControllerStagePhase.FAILED);
  }

  /**
   * Delegates call to {@link ControllerStageTracker#fail()} and registers this transition to FAILED in this query kernel
   */
  public void failStage(final StageId stageId)
  {
    getStageKernelOrThrow(stageId).fail();
    transitionStageKernel(stageId, ControllerStagePhase.FAILED);
  }

  /**
   * Fetches and returns the stage kernel corresponding to the provided stage id, else throws {@link IAE}
   */
  private ControllerStageTracker getStageKernelOrThrow(StageId stageId)
  {
    ControllerStageTracker stageKernel = stageTracker.get(stageId);
    if (stageKernel == null) {
      throw new IAE("Cannot find kernel corresponding to stage [%s] in query [%s]", stageId, queryDef.getQueryId());
    }
    return stageKernel;
  }

  /**
   * Whenever a stage kernel changes it phase, the change must be "registered" by calling this method with the stageId
   * and the new phase
   */
  public void transitionStageKernel(StageId stageId, ControllerStagePhase newPhase)
  {
    Preconditions.checkArgument(
        stageTracker.containsKey(stageId),
        "Attempting to modify an unknown stageKernel"
    );

    if (newPhase == ControllerStagePhase.RESULTS_READY) {
      // Once the stage has produced its results, we remove it from all the stages depending on this stage (for its
      // output).
      for (StageId dependentStageId : outflowMap.get(stageId)) {
        if (!pendingInflowMap.containsKey(dependentStageId)) {
          continue;
        }
        pendingInflowMap.get(dependentStageId).remove(stageId);
        // Check the dependent stage. If it has no dependencies left, it can be marked as to be initialized
        if (pendingInflowMap.get(dependentStageId).size() == 0) {
          readyToRunStages.add(dependentStageId);
          pendingInflowMap.remove(dependentStageId);
        }
      }
    }

    if (ControllerStagePhase.isPostReadingPhase(newPhase)) {
      // Once the stage has consumed all the data/input from its dependent stages, we remove it from all the stages
      // whose input it was dependent on
      for (StageId inputStage : inflowMap.get(stageId)) {
        if (!pendingOutflowMap.containsKey(inputStage)) {
          continue;
        }
        pendingOutflowMap.get(inputStage).remove(stageId);
        // If no more stage is dependent on the "inputStage's" results, it can be safely transitioned to FINISHED
        if (pendingOutflowMap.get(inputStage).size() == 0) {
          effectivelyFinishedStages.add(inputStage);
          pendingOutflowMap.remove(inputStage);
        }
      }
    }
  }

  @VisibleForTesting
  ControllerStageTracker getControllerStageKernel(int stageNumber)
  {
    return stageTracker.get(new StageId(queryDef.getQueryId(), stageNumber));
  }

  /**
   * Returns a mapping of stage -> stages that flow *into* that stage.
   */
  private static Map<StageId, Set<StageId>> computeStageInflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, Set<StageId>> retVal = new HashMap<>();

    for (final StageDefinition stageDef : queryDefinition.getStageDefinitions()) {
      final StageId stageId = stageDef.getId();
      retVal.computeIfAbsent(stageId, ignored -> new HashSet<>());

      for (final int inputStageNumber : queryDefinition.getStageDefinition(stageId).getInputStageNumbers()) {
        final StageId inputStageId = new StageId(queryDefinition.getQueryId(), inputStageNumber);
        retVal.computeIfAbsent(stageId, ignored -> new HashSet<>()).add(inputStageId);
      }
    }

    return retVal;
  }

  /**
   * Returns a mapping of stage -> stages that depend on that stage.
   */
  private static Map<StageId, Set<StageId>> computeStageOutflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, Set<StageId>> retVal = new HashMap<>();

    for (final StageDefinition stageDef : queryDefinition.getStageDefinitions()) {
      final StageId stageId = stageDef.getId();
      retVal.computeIfAbsent(stageId, ignored -> new HashSet<>());

      for (final int inputStageNumber : queryDefinition.getStageDefinition(stageId).getInputStageNumbers()) {
        final StageId inputStageId = new StageId(queryDefinition.getQueryId(), inputStageNumber);
        retVal.computeIfAbsent(inputStageId, ignored -> new HashSet<>()).add(stageId);
      }
    }

    return retVal;
  }
}
