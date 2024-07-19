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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.MapInputSpecSlicer;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.stage.StageInputSpecSlicer;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BaseControllerQueryKernelTest extends InitializedNullHandlingTest
{
  public static final UnknownFault RETRIABLE_FAULT = UnknownFault.forMessage("");

  public ControllerQueryKernelTester testControllerQueryKernel()
  {
    return testControllerQueryKernel(ControllerQueryKernelConfig.Builder::build);
  }

  public ControllerQueryKernelTester testControllerQueryKernel(
      final Function<ControllerQueryKernelConfig.Builder, ControllerQueryKernelConfig> configFn
  )
  {
    return new ControllerQueryKernelTester(
        configFn.apply(
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(100_000_000)
                .destination(TaskReportMSQDestination.instance())
        )
    );
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
    private final InputSpecSlicerFactory inputSlicerFactory =
        (stagePartitionsMap, stageOutputChannelModeMap) ->
            new MapInputSpecSlicer(
                ImmutableMap.of(
                    StageInputSpec.class, new StageInputSpecSlicer(stagePartitionsMap, stageOutputChannelModeMap),
                    ControllerTestInputSpec.class, new ControllerTestInputSpecSlicer()
                )
            );
    private final ControllerQueryKernelConfig config;
    Set<Integer> setupStages = new HashSet<>();

    private ControllerQueryKernelTester(ControllerQueryKernelConfig config)
    {
      this.config = config;
    }

    public ControllerQueryKernelTester queryDefinition(QueryDefinition queryDefinition)
    {
      this.queryDefinition = Preconditions.checkNotNull(queryDefinition);
      this.controllerQueryKernel = new ControllerQueryKernel(queryDefinition, config);
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
      StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      switch (controllerStagePhase) {
        case NEW:
          break;

        case READING_INPUT:
          controllerQueryKernel.createWorkOrders(stageId.getStageNumber(), null);
          controllerQueryKernel.startStage(stageId);
          for (int i = 0; i < queryDefinition.getStageDefinition(stageId).getMaxWorkerCount(); ++i) {
            controllerQueryKernel.workOrdersSentForWorker(stageId, i);
          }
          break;

        case MERGING_STATISTICS:
          setupStage(stageNumber, ControllerStagePhase.READING_INPUT, true);

          final ClusterByStatisticsCollector collector = getMockCollector(
              stageNumber);

          for (int i = 0; i < queryDefinition.getStageDefinition(stageId).getMaxWorkerCount(); ++i) {
            controllerQueryKernel.addPartialKeyStatisticsForStageAndWorker(
                stageId,
                i,
                collector.snapshot().partialKeyStatistics()
            );

            controllerQueryKernel.startFetchingStatsFromWorker(stageId, ImmutableSet.of(i));
          }

          for (int i = 0; i < queryDefinition.getStageDefinition(stageId).getMaxWorkerCount(); ++i) {
            controllerQueryKernel.mergeClusterByStatisticsCollectorForAllTimeChunks(
                stageId,
                i,
                collector.snapshot()
            );
          }

          break;


        case POST_READING:
          if (queryDefinition.getStageDefinition(stageNumber).mustGatherResultKeyStatistics()) {
            setupStage(stageNumber, ControllerStagePhase.MERGING_STATISTICS, true);

            for (int i = 0; i < queryDefinition.getStageDefinition(stageId).getMaxWorkerCount(); ++i) {
              controllerQueryKernel.partitionBoundariesSentForWorker(stageId, i);
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
          for (int i = 0; i < queryDefinition.getStageDefinition(stageId).getMaxWorkerCount(); ++i) {
            controllerQueryKernel.setResultsCompleteForStageAndWorker(
                stageId,
                i,
                new Object()
            );
          }
          break;

        case FINISHED:
          setupStage(stageNumber, ControllerStagePhase.RESULTS_READY, true);
          controllerQueryKernel.finishStage(stageId, false);
          break;

        case FAILED:
          controllerQueryKernel.failStage(stageId);
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
              WorkerAssignmentStrategy.MAX,
              Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
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

    public void startStage(int stageNumber)
    {
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.createWorkOrders(stageNumber, null);
      controllerQueryKernel.startStage(new StageId(queryDefinition.getQueryId(), stageNumber));

    }

    public void startWorkOrder(int stageNumber)
    {
      StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.getWorkerInputsForStage(stageId).workers()
                           .forEach(n -> controllerQueryKernel.workOrdersSentForWorker(stageId, n));
    }

    public void doneReadingInput(int stageNumber)
    {
      StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.getWorkerInputsForStage(stageId).workers()
                           .forEach(n -> controllerQueryKernel.setDoneReadingInputForStageAndWorker(stageId, n));
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


    public void addPartialKeyStatsInformation(int stageNumber, int... workers)
    {
      Preconditions.checkArgument(initialized);
      // Simulate 1000 keys being encountered in the data, so the kernel can generate some partitions.
      final ClusterByStatisticsCollector keyStatsCollector = getMockCollector(stageNumber);
      StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      for (int worker : workers) {
        controllerQueryKernel.addPartialKeyStatisticsForStageAndWorker(
            stageId,
            worker,
            keyStatsCollector.snapshot().partialKeyStatistics()
        );
      }
    }

    public void statsBeingFetchedForWorkers(int stageNumber, Integer... workers)
    {
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.startFetchingStatsFromWorker(
          new StageId(queryDefinition.getQueryId(), stageNumber),
          ImmutableSet.copyOf(workers)
      );
    }

    public void mergeClusterByStatsForAllTimeChunksForWorkers(int stageNumber, Integer... workers)
    {
      Preconditions.checkArgument(initialized);
      StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      final ClusterByStatisticsCollector keyStatsCollector = getMockCollector(stageNumber);
      for (int worker : workers) {
        controllerQueryKernel.mergeClusterByStatisticsCollectorForAllTimeChunks(
            stageId,
            worker,
            keyStatsCollector.snapshot()
        );
      }
    }

    public void setResultsCompleteForStageAndWorkers(int stageNumber, int... workers)
    {
      Preconditions.checkArgument(initialized);
      final StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      for (int worker : workers) {
        controllerQueryKernel.setResultsCompleteForStageAndWorker(
            stageId,
            worker,
            new Object()
        );
      }
    }

    public void failStage(int stageNumber)
    {
      Preconditions.checkArgument(initialized);
      controllerQueryKernel.failStage(new StageId(queryDefinition.getQueryId(), stageNumber));
    }

    public void assertStagePhase(int stageNumber, ControllerStagePhase expectedControllerStagePhase)
    {
      Preconditions.checkArgument(initialized);
      ControllerStageTracker controllerStageKernel = Preconditions.checkNotNull(
          controllerQueryKernel.getControllerStageTracker(stageNumber),
          StringUtils.format("Stage kernel for stage number %d is not initialized yet", stageNumber)
      );
      if (controllerStageKernel.getPhase() != expectedControllerStagePhase) {
        throw new ISE(
            StringUtils.format(
                "Stage kernel for stage number %d is in %s phase which is different from the expected phase %s",
                stageNumber,
                controllerStageKernel.getPhase(),
                expectedControllerStagePhase
            )
        );
      }
    }

    public ControllerQueryKernelConfig getConfig()
    {
      return config;
    }

    /**
     * Checks if the state of the BaseControllerQueryKernel is initialized properly. Currently, this is just stubbed to
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

    public void sendWorkOrdersForWorkers(int stageNumber, int... workers)
    {
      Preconditions.checkArgument(initialized);
      final StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      for (int worker : workers) {
        controllerQueryKernel.workOrdersSentForWorker(stageId, worker);
      }
    }

    public void sendPartitionBoundariesForStageAndWorkers(int stageNumber, int... workers)
    {
      Preconditions.checkArgument(initialized);
      final StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      for (int worker : workers) {
        controllerQueryKernel.partitionBoundariesSentForWorker(stageId, worker);
      }
    }

    public void sendPartitionBoundariesForStage(int stageNumber)
    {
      Preconditions.checkArgument(initialized);
      final StageId stageId = new StageId(queryDefinition.getQueryId(), stageNumber);
      for (int worker : controllerQueryKernel.getWorkersToSendPartitionBoundaries(stageId)) {
        controllerQueryKernel.partitionBoundariesSentForWorker(stageId, worker);
      }
    }

    public List<WorkOrder> getRetriableWorkOrdersAndChangeState(int workeNumber, MSQFault msqFault)
    {
      return controllerQueryKernel.getWorkInCaseWorkerEligibleForRetryElseThrow(workeNumber, msqFault);
    }

    public void failWorkerAndAssertWorkOrderes(int workeNumber, int retriedStage)
    {
      // fail one worker
      List<WorkOrder> workOrderList = getRetriableWorkOrdersAndChangeState(
          workeNumber,
          RETRIABLE_FAULT
      );

      // does not enable the current stage to enable running from start
      Assert.assertTrue(createAndGetNewStageNumbers().size() == 0);
      // only work order of failed worker should be there
      Assert.assertTrue(workOrderList.size() == 1);
      Assert.assertTrue(workOrderList.get(0).getWorkerNumber() == workeNumber);
      Assert.assertTrue(workOrderList.get(0).getStageNumber() == retriedStage);

    }

    @Nonnull
    private ClusterByStatisticsCollector getMockCollector(int stageNumber)
    {
      final ClusterByStatisticsCollector keyStatsCollector =
          queryDefinition.getStageDefinition(stageNumber).createResultKeyStatisticsCollector(10_000_000);
      for (int i = 0; i < 1000; i++) {
        final RowKey key = KeyTestUtils.createKey(
            MockQueryDefinitionBuilder.STAGE_SIGNATURE,
            String.valueOf(i)
        );
        keyStatsCollector.add(key, 1);
      }
      return keyStatsCollector;
    }
  }


}
