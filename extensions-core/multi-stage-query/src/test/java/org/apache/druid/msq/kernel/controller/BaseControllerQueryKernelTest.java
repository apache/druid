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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.MapInputSpecSlicer;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.stage.StageInputSpecSlicer;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.testing.InitializedNullHandlingTest;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BaseControllerQueryKernelTest extends InitializedNullHandlingTest
{

  public ControllerQueryKernelTester testControllerQueryKernel(int numWorkers)
  {
    return new ControllerQueryKernelTester(numWorkers);
  }

  /**
   * A tester is broken into 2 phases
   * 1. Before calling the init() - The unit tests can set up the controller DAG and the initial stages arbitrarily
   * 2. After calling the init() - The unit tests must use the public interface of {@link ControllerQueryKernel} to drive
   * the state machine forward and make assertions on the expected vs the actual state
   */
  public static class ControllerQueryKernelTester
  {
    private boolean initialized = false;
    private QueryDefinition queryDefinition = null;
    private ControllerQueryKernel controllerQueryKernel = null;
    private InputSpecSlicerFactory inputSlicerFactory =
        stagePartitionsMap ->
            new MapInputSpecSlicer(
                ImmutableMap.of(
                    StageInputSpec.class, new StageInputSpecSlicer(stagePartitionsMap),
                    ControllerTestInputSpec.class, new ControllerTestInputSpecSlicer()
                )
            );
    private final int numWorkers;
    Set<Integer> setupStages = new HashSet<>();

    private ControllerQueryKernelTester(int numWorkers)
    {
      this.numWorkers = numWorkers;
    }

    public ControllerQueryKernelTester queryDefinition(QueryDefinition queryDefinition)
    {
      this.queryDefinition = Preconditions.checkNotNull(queryDefinition);
      this.controllerQueryKernel = new ControllerQueryKernel(queryDefinition);
      return this;
    }


    public ControllerQueryKernelTester setupStage(
        int stageNumber,
        ControllerStagePhase controllerStagePhase
    )
    {
      return setupStage(stageNumber, controllerStagePhase, false);
    }

    public ControllerQueryKernelTester setupStage(
        int stageNumber,
        ControllerStagePhase controllerStagePhase,
        boolean recursiveCall
    )
    {
      Preconditions.checkNotNull(queryDefinition, "queryDefinition must be supplied before setting up stage");
      Preconditions.checkArgument(!initialized, "setupStage() can only be called pre init()");
      if (setupStages.contains(stageNumber)) {
        throw new ISE("A stage can only be setup once");
      }
      // Iniitalize the kernels that maybe necessary
      createAndGetNewStageNumbers(false);

      // Initial phase would always be new as we can call this method only once for each
      switch (controllerStagePhase) {
        case NEW:
          break;

        case READING_INPUT:
          controllerQueryKernel.startStage(new StageId(queryDefinition.getQueryId(), stageNumber));
          break;

        case POST_READING:
          setupStage(stageNumber, ControllerStagePhase.READING_INPUT, true);

          if (queryDefinition.getStageDefinition(stageNumber).mustGatherResultKeyStatistics()) {
            for (int i = 0; i < numWorkers; ++i) {
              controllerQueryKernel.addResultKeyStatisticsForStageAndWorker(
                  new StageId(queryDefinition.getQueryId(), stageNumber),
                  i,
                  ClusterByStatisticsSnapshot.empty()
              );
            }
          } else {
            throw new IAE("Stage %d doesn't gather key result statistics", stageNumber);
          }

          break;

        case RESULTS_READY:
          if (queryDefinition.getStageDefinition(stageNumber).mustGatherResultKeyStatistics()) {
            setupStage(stageNumber, ControllerStagePhase.POST_READING, true);
          } else {
            setupStage(stageNumber, ControllerStagePhase.READING_INPUT, true);
          }
          for (int i = 0; i < numWorkers; ++i) {
            controllerQueryKernel.setResultsCompleteForStageAndWorker(
                new StageId(queryDefinition.getQueryId(), stageNumber),
                i,
                new Object()
            );
          }
          break;

        case FINISHED:
          setupStage(stageNumber, ControllerStagePhase.RESULTS_READY, true);
          controllerQueryKernel.finishStage(new StageId(queryDefinition.getQueryId(), stageNumber), false);
          break;

        case FAILED:
          controllerQueryKernel.failStage(new StageId(queryDefinition.getQueryId(), stageNumber));
          break;
      }
      if (!recursiveCall) {
        setupStages.add(stageNumber);
      }
      return this;
    }

    public ControllerQueryKernelTester init()
    {

      Preconditions.checkNotNull(queryDefinition, "queryDefinition must be supplied");

      if (!isValidInitState()) {
        throw new ISE("The stages and their phases are not initialized correctly");
      }
      initialized = true;
      return this;
    }

    /**
     * For use by external callers. For internal purpose we can skip the "initialized" check
     */
    public Set<Integer> createAndGetNewStageNumbers()
    {
      return createAndGetNewStageNumbers(true);
    }

    private Set<Integer> createAndGetNewStageNumbers(boolean checkInitialized)
    {
      if (checkInitialized) {
        Preconditions.checkArgument(initialized);
      }
      return mapStageIdsToStageNumbers(
          controllerQueryKernel.createAndGetNewStageIds(
              inputSlicerFactory,
              WorkerAssignmentStrategy.MAX
          )
      );
    }

    public Set<Integer> getEffectivelyFinishedStageNumbers()
    {
      Preconditions.checkArgument(initialized);
      return mapStageIdsToStageNumbers(controllerQueryKernel.getEffectivelyFinishedStageIds());
    }

    public boolean isDone()
    {
      Preconditions.checkArgument(initialized);
      return controllerQueryKernel.isDone();
    }

    public void markSuccessfulTerminalStagesAsFinished()
    {
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.markSuccessfulTerminalStagesAsFinished();
    }

    public boolean isSuccess()
    {
      Preconditions.checkArgument(initialized);
      return controllerQueryKernel.isSuccess();
    }

    public ControllerStagePhase getStagePhase(int stageNumber)
    {
      Preconditions.checkArgument(initialized);
      return controllerQueryKernel.getStagePhase(new StageId(queryDefinition.getQueryId(), stageNumber));
    }

    public void startStage(int stageNumber)
    {
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.startStage(new StageId(queryDefinition.getQueryId(), stageNumber));
    }


    public void finishStage(int stageNumber)
    {
      finishStage(stageNumber, true);
    }

    public void finishStage(int stageNumber, boolean strict)
    {
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.finishStage(new StageId(queryDefinition.getQueryId(), stageNumber), strict);
    }

    public void addResultKeyStatisticsForStageAndWorker(int stageNumber, int workerNumber)
    {
      Preconditions.checkArgument(initialized);

      // Simulate 1000 keys being encountered in the data, so the kernel can generate some partitions.
      final ClusterByStatisticsCollector keyStatsCollector =
          queryDefinition.getStageDefinition(stageNumber).createResultKeyStatisticsCollector();
      for (int i = 0; i < 1000; i++) {
        final RowKey key = KeyTestUtils.createKey(
            MockQueryDefinitionBuilder.STAGE_SIGNATURE,
            String.valueOf(i)
        );

        keyStatsCollector.add(key, 1);
      }

      controllerQueryKernel.addResultKeyStatisticsForStageAndWorker(
          new StageId(queryDefinition.getQueryId(), stageNumber),
          workerNumber,
          keyStatsCollector.snapshot()
      );
    }

    public void setResultsCompleteForStageAndWorker(int stageNumber, int workerNumber)
    {
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.setResultsCompleteForStageAndWorker(
          new StageId(queryDefinition.getQueryId(), stageNumber),
          workerNumber,
          new Object()
      );
    }

    public void failStage(int stageNumber)
    {
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.failStage(new StageId(queryDefinition.getQueryId(), stageNumber));
    }

    public void assertStagePhase(int stageNumber, ControllerStagePhase expectedControllerStagePhase)
    {
      Preconditions.checkArgument(initialized);
      ControllerStageTracker controllerStageTracker = Preconditions.checkNotNull(
          controllerQueryKernel.getControllerStageKernel(stageNumber),
          StringUtils.format("Stage kernel for stage number %d is not initialized yet", stageNumber)
      );
      if (controllerStageTracker.getPhase() != expectedControllerStagePhase) {
        throw new ISE(
            StringUtils.format(
                "Stage kernel for stage number %d is in %s phase which is different from the expected phase",
                stageNumber,
                controllerStageTracker.getPhase()
            )
        );
      }
    }

    /**
     * Checks if the state of the BaseControllerQueryKernel is initialized properly. Currently this is just stubbed to
     * return true irrespective of the actual state
     */
    private boolean isValidInitState()
    {
      return true;
    }

    private Set<Integer> mapStageIdsToStageNumbers(List<StageId> stageIds)
    {
      return stageIds.stream()
                     .map(StageId::getStageNumber)
                     .collect(Collectors.toSet());
    }
  }
}
