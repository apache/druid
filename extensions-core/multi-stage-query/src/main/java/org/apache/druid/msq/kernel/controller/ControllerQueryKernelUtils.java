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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Utilties for {@link ControllerQueryKernel}.
 */
public class ControllerQueryKernelUtils
{
  /**
   * Put stages from {@link QueryDefinition} into groups that must each be launched simultaneously.
   *
   * This method's goal is to maximize the usage of {@link OutputChannelMode#MEMORY} channels, subject to constraints
   * provided by {@link ControllerQueryKernelConfig#isPipeline()},
   * {@link ControllerQueryKernelConfig#getMaxConcurrentStages()}, and
   * {@link ControllerQueryKernelConfig#isFaultTolerant()}.
   *
   * An important part of the logic here is determining the output channel mode of the final stage in a group, i.e.
   * {@link StageGroup#lastStageOutputChannelMode()}.
   *
   * If the {@link StageGroup#lastStageOutputChannelMode()} is not {@link OutputChannelMode#MEMORY}, then the stage
   * group is fully executed, and fully generates its output, prior to any downstream stage groups starting.
   *
   * On the other hand, if {@link StageGroup#lastStageOutputChannelMode()} is {@link OutputChannelMode#MEMORY}, the
   * stage group executes up to such a point that the group's last stage has results ready-to-read; see
   * {@link ControllerQueryKernel#readyToReadResults(StageId, ControllerStagePhase)}. A downstream stage group, if any,
   * is started while the current group is still running. This enables them to transfer data in memory.
   *
   * Stage groups always end when some stage in them sorts during shuffle, i.e. returns true from
   * ({@link StageDefinition#doesSortDuringShuffle()}). This enables "leapfrog" execution, where a sequence
   * of sorting stages in separate groups can all run with {@link OutputChannelMode#MEMORY}, even when there are more
   * stages than the maxConcurrentStages parameter. To achieve this, we wind down upstream stage groups prior to
   * starting downstream stage groups, such that only two groups are ever running at a time.
   *
   * For example, consider a case where pipeline = true and maxConcurrentStages = 2, and the query has three stages,
   * all of which sort during shuffle. The expected return from this method is a list of 3 stage groups, each with
   * one stage, and each with {@link StageGroup#lastStageOutputChannelMode()} set to {@link OutputChannelMode#MEMORY}.
   * To stay within maxConcurrentStages = 2, execution leapfrogs in the following manner (note- not all transitions
   * are shown here, for brevity):
   *
   * <ol>
   *   <li>Stage 0 starts</li>
   *   <li>Stage 0 enters {@link ControllerStagePhase#POST_READING}, finishes sorting</li>
   *   <li>Stage 1 enters {@link ControllerStagePhase#READING_INPUT}</li>
   *   <li>Stage 1 enters {@link ControllerStagePhase#POST_READING}, finishes sorting</li>
   *   <li>Stage 0 stops, ends in {@link ControllerStagePhase#FINISHED})</li>
   *   <li>Stage 2 starts</li>
   *   <li>Stage 2 enters {@link ControllerStagePhase#POST_READING}, finishes sorting</li>
   *   <li>Stage 1 stops, ends in {@link ControllerStagePhase#FINISHED})</li>
   *   <li>Stage 2 stops and query completes</li>
   * </ol>
   *
   * When maxConcurrentStages = 2, leapfrogging is only possible with stage groups containing a single stage each.
   * When maxConcurrentStages > 2, leapfrogging can happen with larger stage groups containing longer chains.
   */
  public static List<StageGroup> computeStageGroups(
      final QueryDefinition queryDef,
      final ControllerQueryKernelConfig config
  )
  {
    final MSQDestination destination = config.getDestination();
    final List<StageGroup> stageGroups = new ArrayList<>();
    final boolean useDurableStorage = config.isDurableStorage();
    final Map<StageId, SortedSet<StageId>> inflow = computeStageInflowMap(queryDef);
    final Map<StageId, SortedSet<StageId>> outflow = computeStageOutflowMap(queryDef);
    final Set<StageId> stagesRun = new HashSet<>();

    // This loop simulates execution of all stages, such that we arrive at an order of execution that is compatible
    // with all relevant constraints.

    while (stagesRun.size() < queryDef.getStageDefinitions().size()) {
      // 1) Find all stages that are ready to run, and that cannot use MEMORY output modes. Run them as solo groups.
      boolean didRun;
      do {
        didRun = false;

        for (final StageId stageId : ImmutableList.copyOf(inflow.keySet())) {
          if (!stagesRun.contains(stageId) /* stage has not run yet */
              && inflow.get(stageId).isEmpty() /* stage data is fully available */
              && !canUseMemoryOutput(queryDef, stageId.getStageNumber(), config, outflow)) {
            stagesRun.add(stageId);
            stageGroups.add(
                new StageGroup(
                    Collections.singletonList(stageId),
                    getOutputChannelMode(
                        queryDef,
                        stageId.getStageNumber(),
                        destination.toSelectDestination(),
                        useDurableStorage,
                        false
                    )
                )
            );

            // Simulate this stage finishing.
            removeStageFlow(stageId, inflow, outflow);
            didRun = true;
          }
        }
      } while (didRun);

      // 2) Pick some stage that can use MEMORY output mode, and run that as well as all ready-to-run dependents.
      StageId currentStageId = null;
      for (final StageId stageId : ImmutableList.copyOf(inflow.keySet())) {
        if (!stagesRun.contains(stageId)
            && inflow.get(stageId).isEmpty()
            && canUseMemoryOutput(queryDef, stageId.getStageNumber(), config, outflow)) {
          currentStageId = stageId;
          break;
        }
      }

      if (currentStageId == null) {
        // Didn't find a stage that can use MEMORY output mode.
        continue;
      }

      // Found a stage that can use MEMORY output mode. Build a maximally-sized StageGroup around it.
      final List<StageId> currentStageGroup = new ArrayList<>();

      // maxStageGroupSize => largest size this stage group can be while respecting maxConcurrentStages and leaving
      // room for a priorGroup to run concurrently (if priorGroup uses MEMORY output mode).
      final int maxStageGroupSize;

      if (stageGroups.isEmpty()) {
        maxStageGroupSize = config.getMaxConcurrentStages();
      } else {
        final StageGroup priorGroup = stageGroups.get(stageGroups.size() - 1);
        if (priorGroup.lastStageOutputChannelMode() == OutputChannelMode.MEMORY) {
          // Prior group runs concurrently with this group. (Can happen when leapfrogging; see class-level Javadoc.)

          // Note: priorGroup.size() is strictly less than config.getMaxConcurrentStages(), because the prior group
          // would have its size limited by maxStageGroupSizeAllowingForDownstreamConsumer below.

          maxStageGroupSize = config.getMaxConcurrentStages() - priorGroup.size();
        } else {
          // Prior group exits before this group starts.
          maxStageGroupSize = config.getMaxConcurrentStages();
        }
      }

      OutputChannelMode currentOutputChannelMode = null;
      while (currentStageId != null) {
        final boolean canUseMemoryOuput =
            canUseMemoryOutput(queryDef, currentStageId.getStageNumber(), config, outflow);
        final Set<StageId> currentOutflow = outflow.get(currentStageId);

        // maxStageGroupSizeAllowingForDownstreamConsumer => largest size this stage group can be while still being
        // able to use MEMORY output mode. (With MEMORY output mode, the downstream consumer must run concurrently.)
        final int maxStageGroupSizeAllowingForDownstreamConsumer;

        if (queryDef.getStageDefinition(currentStageId).doesSortDuringShuffle()) {
          // When the current group sorts, there's a pipeline break, so we can leapfrog: close the prior group before
          // starting the downstream group. In this case, we only need to reserve a single concurrent-stage slot for
          // a downstream consumer.

          // Note: the only stage that can possibly sort is the final stage, because of the check below that closes
          // off the stage group if currentStageId "doesSortDuringShuffle()".

          maxStageGroupSizeAllowingForDownstreamConsumer = config.getMaxConcurrentStages() - 1;
        } else {
          // When the final stage in the current group doesn't sort, we can't leapfrog. We need to reserve a single
          // concurrent-stage slot for a downstream consumer, plus enough space to keep the priorGroup running (which
          // is accounted for in maxStageGroupSize).
          maxStageGroupSizeAllowingForDownstreamConsumer = maxStageGroupSize - 1;
        }

        currentOutputChannelMode =
            getOutputChannelMode(
                queryDef,
                currentStageId.getStageNumber(),
                config.getDestination().toSelectDestination(),
                config.isDurableStorage(),
                canUseMemoryOuput

                // Stages can only use MEMORY output mode if they have <= 1 downstream stage (checked by
                // "canUseMemoryOutput") and if that downstream stage has all of its other inputs available.
                && (currentOutflow.isEmpty() ||
                    Collections.singleton(currentStageId)
                               .equals(inflow.get(Iterables.getOnlyElement(currentOutflow))))

                // And, stages can only use MEMORY output mode if their downstream consumer is able to start
                // concurrently. So, once the stage group gets too big, we must stop using MEMORY output mode.
                && (currentOutflow.isEmpty()
                    || currentStageGroup.size() < maxStageGroupSizeAllowingForDownstreamConsumer)
            );

        currentStageGroup.add(currentStageId);

        if (currentOutflow.size() == 1
            && currentStageGroup.size() < maxStageGroupSize
            && currentOutputChannelMode == OutputChannelMode.MEMORY

            // Sorting causes a pipeline break: a sorting stage must read all its input before writing any output.
            // Continue the stage group only if this stage does not sort its output.
            && !queryDef.getStageDefinition(currentStageId).doesSortDuringShuffle()) {
          currentStageId = Iterables.getOnlyElement(currentOutflow);
        } else {
          currentStageId = null;
        }
      }

      stageGroups.add(new StageGroup(currentStageGroup, currentOutputChannelMode));

      // Simulate this stage group finishing.
      for (final StageId stageId : currentStageGroup) {
        stagesRun.add(stageId);
        removeStageFlow(stageId, inflow, outflow);
      }
    }

    return stageGroups;
  }

  /**
   * Returns a mapping of stage -> stages that flow *into* that stage. Uses TreeMaps and TreeSets so we have a
   * consistent order for analyzing and running stages.
   */
  public static Map<StageId, SortedSet<StageId>> computeStageInflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, SortedSet<StageId>> retVal = new TreeMap<>();

    for (final StageDefinition stageDef : queryDefinition.getStageDefinitions()) {
      final StageId stageId = stageDef.getId();
      retVal.computeIfAbsent(stageId, ignored -> new TreeSet<>());

      for (final int inputStageNumber : queryDefinition.getStageDefinition(stageId).getInputStageNumbers()) {
        final StageId inputStageId = new StageId(queryDefinition.getQueryId(), inputStageNumber);
        retVal.computeIfAbsent(stageId, ignored -> new TreeSet<>()).add(inputStageId);
      }
    }

    return retVal;
  }

  /**
   * Returns a mapping of stage -> stages that depend on that stage. Uses TreeMaps and TreeSets so we have a consistent
   * order for analyzing and running stages.
   */
  public static Map<StageId, SortedSet<StageId>> computeStageOutflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, SortedSet<StageId>> retVal = new TreeMap<>();

    for (final StageDefinition stageDef : queryDefinition.getStageDefinitions()) {
      final StageId stageId = stageDef.getId();
      retVal.computeIfAbsent(stageId, ignored -> new TreeSet<>());

      for (final int inputStageNumber : queryDefinition.getStageDefinition(stageId).getInputStageNumbers()) {
        final StageId inputStageId = new StageId(queryDefinition.getQueryId(), inputStageNumber);
        retVal.computeIfAbsent(inputStageId, ignored -> new TreeSet<>()).add(stageId);
      }
    }

    return retVal;
  }

  /**
   * Whether output of a stage can possibly use {@link OutputChannelMode#MEMORY}. Returning true does not guarantee
   * that the stage *will* use {@link OutputChannelMode#MEMORY}. Additional requirements are checked in
   * {@link #computeStageGroups(QueryDefinition, ControllerQueryKernelConfig)}.
   */
  public static boolean canUseMemoryOutput(
      final QueryDefinition queryDefinition,
      final int stageNumber,
      final ControllerQueryKernelConfig config,
      final Map<StageId, SortedSet<StageId>> outflowMap
  )
  {
    if (config.isFaultTolerant()) {
      // Cannot use MEMORY output mode if fault tolerance is enabled: durable storage is required.
      return false;
    }

    if (!config.isPipeline() || config.getMaxConcurrentStages() < 2) {
      // Cannot use MEMORY output mode if pipelining (& running multiple stages at once) is not enabled.
      return false;
    }

    final StageId stageId = queryDefinition.getStageDefinition(stageNumber).getId();
    final Set<StageId> outflowStageIds = outflowMap.get(stageId);

    if (outflowStageIds.isEmpty()) {
      return true;
    } else if (outflowStageIds.size() == 1) {
      final StageDefinition outflowStageDef =
          queryDefinition.getStageDefinition(Iterables.getOnlyElement(outflowStageIds));

      // Two things happening here:
      //   1) Stages cannot use output mode MEMORY when broadcasting. This is because when using output mode MEMORY, we
      //      can only support a single reader.
      //   2) Downstream stages can only have a single input stage with output mode MEMORY. This isn't strictly
      //      necessary, but it simplifies the logic around concurrently launching stages.
      return stageId.equals(getOnlyNonBroadcastInputAsStageId(outflowStageDef));
    } else {
      return false;
    }
  }

  /**
   * Return an {@link OutputChannelMode} to use for a given stage, based on query and context.
   */
  public static OutputChannelMode getOutputChannelMode(
      final QueryDefinition queryDef,
      final int stageNumber,
      @Nullable final MSQSelectDestination selectDestination,
      final boolean durableStorage,
      final boolean canStream
  )
  {
    final boolean isFinalStage = queryDef.getFinalStageDefinition().getStageNumber() == stageNumber;

    if (isFinalStage && selectDestination == MSQSelectDestination.DURABLESTORAGE) {
      return OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS;
    } else if (canStream) {
      return OutputChannelMode.MEMORY;
    } else if (durableStorage) {
      return OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE;
    } else {
      return OutputChannelMode.LOCAL_STORAGE;
    }
  }

  /**
   * If a stage has a single non-broadcast input stage, returns that input stage. Otherwise, returns null.
   * This is a helper used by {@link #canUseMemoryOutput}.
   */
  @Nullable
  public static StageId getOnlyNonBroadcastInputAsStageId(final StageDefinition downstreamStageDef)
  {
    final List<InputSpec> inputSpecs = downstreamStageDef.getInputSpecs();
    final IntSet broadcastInputNumbers = downstreamStageDef.getBroadcastInputNumbers();

    if (inputSpecs.size() - broadcastInputNumbers.size() != 1) {
      return null;
    }

    for (int i = 0; i < inputSpecs.size(); i++) {
      if (!broadcastInputNumbers.contains(i)) {
        final IntSet stageNumbers = InputSpecs.getStageNumbers(Collections.singletonList(inputSpecs.get(i)));
        if (stageNumbers.size() == 1) {
          return new StageId(downstreamStageDef.getId().getQueryId(), stageNumbers.iterator().nextInt());
        }
      }
    }

    return null;
  }

  /**
   * Remove all outflow from "stageId". At the conclusion of this method, "outflow" has an empty set for "stageId",
   * and no sets in "inflow" reference "stageId". Outflow and inflow sets may become empty as a result of this
   * operation. Sets that become empty are left empty, not removed from the inflow and outflow maps.
   */
  private static void removeStageFlow(
      final StageId stageId,
      final Map<StageId, SortedSet<StageId>> inflow,
      final Map<StageId, SortedSet<StageId>> outflow
  )
  {
    for (final StageId outStageId : outflow.get(stageId)) {
      inflow.get(outStageId).remove(stageId);
    }

    outflow.get(stageId).clear();
  }
}
