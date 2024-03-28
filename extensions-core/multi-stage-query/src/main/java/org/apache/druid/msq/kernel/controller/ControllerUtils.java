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
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Utilties for {@link ControllerQueryKernel}.
 */
public class ControllerUtils
{
  /**
   * Put stages from {@link QueryDefinition} into groups that must each be launched simultaneously.
   *
   * This method's goal is to maximize the usage of {@link OutputChannelMode#MEMORY} channels, subject to constraints
   * provided by {@link ControllerQueryKernelConfig#isPipeline()},
   * {@link ControllerQueryKernelConfig#getMaxConcurrentStages()}, and
   * {@link ControllerQueryKernelConfig#isFaultTolerant()}.
   */
  public static List<StageGroup> computeStageGroups(
      final QueryDefinition queryDef,
      final ControllerQueryKernelConfig config
  )
  {
    final MSQDestination destination = config.getDestination();
    final List<StageGroup> stageGroups = new ArrayList<>();
    final boolean useDurableStorage = config.isDurableStorage();
    final Map<StageId, Set<StageId>> inflow = computeStageInflowMap(queryDef);
    final Map<StageId, Set<StageId>> outflow = computeStageOutflowMap(queryDef);
    final Set<StageId> stagesRun = new HashSet<>();

    while (stagesRun.size() < queryDef.getStageDefinitions().size()) {
      // 1) Run all stages that cannot stream their output, as solo groups.
      boolean didRun;
      do {
        didRun = false;

        for (final StageId stageId : ImmutableList.copyOf(inflow.keySet())) {
          if (!stagesRun.contains(stageId)
              && inflow.get(stageId).isEmpty()
              && !canStreamOutput(queryDef, stageId.getStageNumber(), config, outflow)) {
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

            removeStageFlow(stageId, inflow, outflow);
            didRun = true;
          }
        }
      } while (didRun);

      // 2) Pick some stage that can stream its output, and run that as well as all ready-to-run dependents.
      StageId currentStageId = null;
      for (final StageId stageId : ImmutableList.copyOf(inflow.keySet())) {
        if (!stagesRun.contains(stageId)
            && inflow.get(stageId).isEmpty()
            && canStreamOutput(queryDef, stageId.getStageNumber(), config, outflow)) {
          currentStageId = stageId;
          break;
        }
      }

      if (currentStageId != null) {
        final List<StageId> currentStageGroup = new ArrayList<>();
        final int maxStageGroupSize;
        if (stageGroups.isEmpty()) {
          maxStageGroupSize = config.getMaxConcurrentStages();
        } else {
          final StageGroup priorGroup = stageGroups.get(stageGroups.size() - 1);
          if (priorGroup.lastStageOutputChannelMode() == OutputChannelMode.MEMORY) {
            // Prior group must run concurrently with this group.
            maxStageGroupSize = config.getMaxConcurrentStages() - priorGroup.size();
          } else {
            // Prior group can exit before this group starts.
            maxStageGroupSize = config.getMaxConcurrentStages();
          }
        }

        OutputChannelMode currentOutputChannelMode = null;
        while (currentStageId != null) {
          final boolean canStream = canStreamOutput(queryDef, currentStageId.getStageNumber(), config, outflow);
          final Set<StageId> currentOutflow = outflow.get(currentStageId);

          final int maxStageGroupSizeAllowingForDownstreamConsumer;
          if (queryDef.getStageDefinition(currentStageId).doesSortDuringShuffle()) {
            // When the current group sorts, there's a pipeline break, so we can "leapfrog": close the prior group
            // before starting the downstream group.
            maxStageGroupSizeAllowingForDownstreamConsumer = config.getMaxConcurrentStages() - 1;
          } else {
            // When the current group doesn't sort, we can't leapfrog.
            maxStageGroupSizeAllowingForDownstreamConsumer = maxStageGroupSize - 1;
          }

          currentOutputChannelMode =
              getOutputChannelMode(
                  queryDef,
                  currentStageId.getStageNumber(),
                  config.getDestination().toSelectDestination(),
                  config.isDurableStorage(),
                  canStream

                  // Stages can only stream if they have <= 1 downstream stage (checked by "canStreamOutput") and if
                  // that downstream stage has all of its other inputs available.
                  && (currentOutflow.isEmpty() ||
                      Collections.singleton(currentStageId)
                                 .equals(inflow.get(Iterables.getOnlyElement(currentOutflow))))

                  // Stages can only stream if that one downstream consumer is able to start concurrently. So, once the
                  // stage group gets too big, we must stop streaming.
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

        for (final StageId stageId : currentStageGroup) {
          stagesRun.add(stageId);
          removeStageFlow(stageId, inflow, outflow);
        }
      }
    }

    return stageGroups;
  }

  /**
   * Returns a mapping of stage -> stages that flow *into* that stage. Uses TreeMaps and TreeSets so we have a
   * consistent order for analyzing and running stages.
   */
  public static Map<StageId, Set<StageId>> computeStageInflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, Set<StageId>> retVal = new TreeMap<>();

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
  public static Map<StageId, Set<StageId>> computeStageOutflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, Set<StageId>> retVal = new TreeMap<>();

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
  public static boolean canStreamOutput(
      final QueryDefinition queryDefinition,
      final int stageNumber,
      final ControllerQueryKernelConfig config,
      final Map<StageId, Set<StageId>> outflowMap
  )
  {
    if (config.isFaultTolerant()) {
      // Cannot stream if fault tolerance is enabled: durable storage is required.
      return false;
    }

    if (!config.isPipeline() || config.getMaxConcurrentStages() < 2) {
      // Cannot stream if pipelining (& running multiple stages at once) is not enabled.
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
      //   1) Stages cannot both stream and broadcast their output. This is because when streaming, we can only
      //      support a single reader.
      //   2) Stages can only receive a single streamed input. This isn't strictly necessary, but it simplifies the
      //      logic around concurrently launching stages.
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
   * This is a helper used by {@link #canStreamOutput}.
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

  private static void removeStageFlow(
      final StageId stageId,
      final Map<StageId, Set<StageId>> inflow,
      final Map<StageId, Set<StageId>> outflow
  )
  {
    for (final StageId outStageId : outflow.get(stageId)) {
      inflow.get(outStageId).remove(stageId);
    }

    outflow.get(stageId).clear();
  }
}
