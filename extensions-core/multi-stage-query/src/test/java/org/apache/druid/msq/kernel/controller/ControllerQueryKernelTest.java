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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.ShuffleKind;
import org.apache.druid.msq.kernel.worker.WorkerStagePhase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class ControllerQueryKernelTest extends BaseControllerQueryKernelTest
{

  @Test
  public void testCompleteDAGExecutionForSingleWorker()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel();
    //      0       1
    //      |   /   |
    //      2 /     3
    //      |       |
    //      4       5
    //        \    /
    //          6
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(7)
            .addEdge(0, 2)
            .addEdge(1, 2)
            .addEdge(1, 3)
            .addEdge(2, 4)
            .addEdge(3, 5)
            .addEdge(4, 6)
            .addEdge(5, 6)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.init();

    Set<Integer> newStageNumbers;
    Set<Integer> effectivelyFinishedStageNumbers;

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);

    // Mark 0 as done. Next up will be 1.
    transitionNewToResultsComplete(controllerQueryKernelTester, 0);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(1), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);

    // Mark 1 as done and fetch the new kernels. Next up will be 2.
    transitionNewToResultsComplete(controllerQueryKernelTester, 1);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(2), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);

    // Mark 2 as done and fetch the new kernels. Next up will be 3.
    transitionNewToResultsComplete(controllerQueryKernelTester, 2);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0), effectivelyFinishedStageNumbers);

    // Mark 3 as done and fetch the new kernels. Next up will be 4.
    transitionNewToResultsComplete(controllerQueryKernelTester, 3);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(4), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 1), effectivelyFinishedStageNumbers);

    // Mark 4 as done and fetch new kernels. Next up will be 5.
    transitionNewToResultsComplete(controllerQueryKernelTester, 4);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(5), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 1, 2), effectivelyFinishedStageNumbers);

    // Mark 0, 1, 2 finished together.
    effectivelyFinishedStageNumbers.forEach(controllerQueryKernelTester::finishStage);

    // Mark 5 as done and fetch new kernels. Next up will be 6, and 3 will be ready to finish.
    transitionNewToResultsComplete(controllerQueryKernelTester, 5);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(6), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), effectivelyFinishedStageNumbers);

    // Mark 6 as done. No more kernels left, but we can clean up 4, 5, 6 along with 3.
    transitionNewToResultsComplete(controllerQueryKernelTester, 6);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3, 4, 5, 6), effectivelyFinishedStageNumbers);
    effectivelyFinishedStageNumbers.forEach(controllerQueryKernelTester::finishStage);
  }

  @Test
  public void testCompleteDAGExecutionForSingleWorkerWithPipelining()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(
        configBuilder ->
            configBuilder.maxConcurrentStages(2).pipeline(true).build()
    );
    //      0 [HLS]   1 [HLS]
    //      |       / |
    //      2 [none]  3 [HLS]
    //      |         |
    //      4 [mix]   5 [HLS]
    //       \       /
    //        \     /
    //           6 [none]

    final QueryDefinition queryDef = new MockQueryDefinitionBuilder(7)
        .addEdge(0, 2)
        .addEdge(1, 2)
        .addEdge(1, 3)
        .addEdge(2, 4)
        .addEdge(3, 5)
        .addEdge(4, 6)
        .addEdge(5, 6)
        .defineStage(0, ShuffleKind.HASH_LOCAL_SORT)
        .defineStage(1, ShuffleKind.HASH_LOCAL_SORT)
        .defineStage(3, ShuffleKind.HASH_LOCAL_SORT)
        .defineStage(4, ShuffleKind.MIX)
        .defineStage(5, ShuffleKind.HASH_LOCAL_SORT)
        .getQueryDefinitionBuilder()
        .build();

    controllerQueryKernelTester.queryDefinition(queryDef);
    controllerQueryKernelTester.init();

    Assert.assertEquals(
        ImmutableList.of(
            ControllerQueryKernelUtilsTest.makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0),
            ControllerQueryKernelUtilsTest.makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            ControllerQueryKernelUtilsTest.makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 2, 4),
            ControllerQueryKernelUtilsTest.makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 3),
            ControllerQueryKernelUtilsTest.makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 5),
            ControllerQueryKernelUtilsTest.makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 6)
        ),
        ControllerQueryKernelUtils.computeStageGroups(queryDef, controllerQueryKernelTester.getConfig())
    );

    Set<Integer> newStageNumbers;
    Set<Integer> effectivelyFinishedStageNumbers;

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 1), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);


    transitionNewToResultsComplete(controllerQueryKernelTester, 1);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);


    // Mark 0 as done and fetch the new kernels. 2 should be unblocked along with 4.
    transitionNewToResultsComplete(controllerQueryKernelTester, 0);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(2, 4), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);


    // Mark 2 as done and fetch the new kernels. 4 is still ready, 0 can now be cleaned, and 3 can be launched
    transitionNewToResultsComplete(controllerQueryKernelTester, 2);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3, 4), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0), effectivelyFinishedStageNumbers);

    // Mark 4 as done and fetch the new kernels. 3 is still ready, and 2 becomes cleanable
    transitionNewToResultsComplete(controllerQueryKernelTester, 4);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 2), effectivelyFinishedStageNumbers);

    // Mark 3 as post-reading and fetch new kernels. This makes 1 cleanable, and 5 ready to run
    transitionNewToDoneReadingInput(controllerQueryKernelTester, 3);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(5), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 1, 2), effectivelyFinishedStageNumbers);

    // Mark 0, 1, 2 finished together
    effectivelyFinishedStageNumbers.forEach(controllerQueryKernelTester::finishStage);

    // Mark 5 as post-reading and fetch new kernels. Nothing is ready, since 6 is waiting for 5 to finish
    // However, this does clear up 3 to become cleanable
    transitionNewToDoneReadingInput(controllerQueryKernelTester, 5);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), effectivelyFinishedStageNumbers);

    // Mark 5 as done. This makes 6 ready to go
    transitionDoneReadingInputToResultsComplete(controllerQueryKernelTester, 5);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(6), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), effectivelyFinishedStageNumbers);

    // Mark 6 as done. No more kernels left, but we can clean up 4 and 5 along with 2
    transitionNewToResultsComplete(controllerQueryKernelTester, 6);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3, 4, 5, 6), effectivelyFinishedStageNumbers);
    effectivelyFinishedStageNumbers.forEach(controllerQueryKernelTester::finishStage);
  }

  @Test
  public void testCompleteDAGExecutionForMultipleWorkers()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel();
    // 0 -> 1 -> 2 -> 3

    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(4)
            .addEdge(0, 1)
            .addEdge(1, 2)
            .addEdge(2, 3)
            .defineStage(0, ShuffleKind.GLOBAL_SORT, 1) // Ingestion only on one worker
            .defineStage(1, ShuffleKind.GLOBAL_SORT, 2)
            .defineStage(3, ShuffleKind.GLOBAL_SORT, 2)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.init();

    Set<Integer> newStageNumbers;
    Set<Integer> effectivelyFinishedStageNumbers;

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);
    controllerQueryKernelTester.startStage(0);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(1), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);
    controllerQueryKernelTester.startStage(1);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(1, 0);
    controllerQueryKernelTester.addPartialKeyStatsInformation(1, 0);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(1, 0);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(1, 0);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(1, 1);
    controllerQueryKernelTester.addPartialKeyStatsInformation(1, 1);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(1, 1);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(1, 1);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(1, 0);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(1, 0);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(1, 1);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(1, 1);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.RESULTS_READY);

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(2), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(0), effectivelyFinishedStageNumbers);
    controllerQueryKernelTester.startStage(2);
    controllerQueryKernelTester.assertStagePhase(2, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(2, 0);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(2, 0);
    controllerQueryKernelTester.assertStagePhase(2, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.finishStage(0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(1), effectivelyFinishedStageNumbers);
    controllerQueryKernelTester.startStage(3);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.startWorkOrder(3);
    controllerQueryKernelTester.addPartialKeyStatsInformation(3, 1);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(3, 0);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(3, 0, 1);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(3, 0, 1);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(3, 0);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(3, 0);

    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(3, 1);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(3, 1);

    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.finishStage(1);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.FINISHED);

    controllerQueryKernelTester.markSuccessfulTerminalStagesAsFinished();
    controllerQueryKernelTester.assertStagePhase(2, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.FINISHED);
  }

  @Test
  public void testTransitionsInShufflingStagesAndMultipleWorkers()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel();

    // Single stage query definition
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(1)
            .defineStage(0, ShuffleKind.GLOBAL_SORT, 2)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.init();


    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 1);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    controllerQueryKernelTester.finishStage(0, false);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);
  }

  @Test
  public void testPrematureResultsComplete()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel();

    // Single stage query definition
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(1)
            .defineStage(0, ShuffleKind.GLOBAL_SORT, 2)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);


    Assert.assertThrows(
        StringUtils.format(
            "Worker[%d] for stage[%d] expected to be in state[%s]. Found state[%s]",
            1,
            0,
            WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES,
            WorkerStagePhase.NEW
        ),
        ISE.class,
        () -> controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0)
    );

  }

  @Test
  public void testKernelFailed()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(
        configBuilder ->
            configBuilder.maxConcurrentStages(2).build()
    );

    // 0  1
    // \  /
    //   2
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(3)
            .addEdge(0, 2)
            .addEdge(1, 2)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setupStage(1, ControllerStagePhase.RESULTS_READY);

    controllerQueryKernelTester.init();

    controllerQueryKernelTester.failStage(0);

    Assert.assertTrue(controllerQueryKernelTester.isDone());
    Assert.assertFalse(controllerQueryKernelTester.isSuccess());
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FAILED);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.RESULTS_READY);
  }

  @Test(expected = IllegalStateException.class)
  public void testCycleInvalidQueryThrowsException()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel();

    // 0 - 1
    // \  /
    //   2
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(3)
            .addEdge(0, 1)
            .addEdge(1, 2)
            .addEdge(2, 0)
            .getQueryDefinitionBuilder()
            .build()
    );
  }

  @Test(expected = IllegalStateException.class)
  public void testSelfLoopInvalidQueryThrowsException()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel();

    // 0 _
    // |__|
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(1)
            .addEdge(0, 0)
            .getQueryDefinitionBuilder()
            .build()
    );
  }

  @Test(expected = IllegalStateException.class)
  public void testLoopInvalidQueryThrowsException()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel();

    // 0 - 1
    // |   |
    //  ---
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(2)
            .addEdge(0, 1)
            .addEdge(1, 0)
            .getQueryDefinitionBuilder()
            .build()
    );
  }

  @Test
  public void testMarkSuccessfulTerminalStagesAsFinished()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel();

    // 0  1
    // \  /
    //   2
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(3)
            .addEdge(0, 2)
            .addEdge(1, 2)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.setupStage(1, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.setupStage(2, ControllerStagePhase.RESULTS_READY);

    controllerQueryKernelTester.init();

    Assert.assertFalse(controllerQueryKernelTester.isDone());
    Assert.assertFalse(controllerQueryKernelTester.isSuccess());

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.assertStagePhase(2, ControllerStagePhase.RESULTS_READY);

    controllerQueryKernelTester.markSuccessfulTerminalStagesAsFinished();

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.assertStagePhase(2, ControllerStagePhase.FINISHED);
  }

  private static void transitionNewToResultsComplete(ControllerQueryKernelTester queryKernelTester, int stageNumber)
  {
    queryKernelTester.startStage(stageNumber);
    queryKernelTester.startWorkOrder(stageNumber);
    queryKernelTester.setResultsCompleteForStageAndWorkers(stageNumber, 0);
  }

  private static void transitionNewToDoneReadingInput(ControllerQueryKernelTester queryKernelTester, int stageNumber)
  {
    queryKernelTester.startStage(stageNumber);
    queryKernelTester.startWorkOrder(stageNumber);
    queryKernelTester.doneReadingInput(stageNumber);
  }

  private static void transitionDoneReadingInputToResultsComplete(
      ControllerQueryKernelTester queryKernelTester,
      int stageNumber
  )
  {
    queryKernelTester.setResultsCompleteForStageAndWorkers(stageNumber, 0);
  }
}
