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

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class ControllerQueryKernelTests extends BaseControllerQueryKernelTest
{

  @Test
  public void testCompleteDAGExecutionForSingleWorker()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(1);
    //      0       1
    //      |   /   |
    //      2 /     3
    //      |       |
    //      4       5
    //        \    /
    //          6
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(7)
            .addVertex(0, 2)
            .addVertex(1, 2)
            .addVertex(1, 3)
            .addVertex(2, 4)
            .addVertex(3, 5)
            .addVertex(4, 6)
            .addVertex(5, 6)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.init();

    Set<Integer> newStageNumbers;
    Set<Integer> effectivelyFinishedStageNumbers;

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 1), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);


    transitionNewToResultsComplete(controllerQueryKernelTester, 1);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 3), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);


    // Mark 3 as done and fetch the new kernels. 5 should be unblocked along with 0.
    transitionNewToResultsComplete(controllerQueryKernelTester, 3);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 5), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);


    // Mark 5 as done and fetch the new kernels. Only 0 is still unblocked, but 3 can now be cleaned
    transitionNewToResultsComplete(controllerQueryKernelTester, 5);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), effectivelyFinishedStageNumbers);

    // Mark 0 as done and fetch the new kernels. This should unblock 2
    transitionNewToResultsComplete(controllerQueryKernelTester, 0);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(2), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), effectivelyFinishedStageNumbers);

    // Mark 2 as done and fetch new kernels. This should clear up 0 and 1 alongside 3 (which is not marked as FINISHED yet)
    transitionNewToResultsComplete(controllerQueryKernelTester, 2);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(4), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(0, 1, 3), effectivelyFinishedStageNumbers);

    // Mark 0, 1, 3 finished together
    effectivelyFinishedStageNumbers.forEach(controllerQueryKernelTester::finishStage);

    // Mark 4 as done and fetch new kernels. This should unblock 6 and clear up 2
    transitionNewToResultsComplete(controllerQueryKernelTester, 4);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(6), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(2), effectivelyFinishedStageNumbers);

    // Mark 6 as done. No more kernels left, but we can clean up 4 and 5 alongwith 2
    transitionNewToResultsComplete(controllerQueryKernelTester, 6);
    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    Assert.assertEquals(ImmutableSet.of(), newStageNumbers);
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(2, 4, 5), effectivelyFinishedStageNumbers);
    effectivelyFinishedStageNumbers.forEach(controllerQueryKernelTester::finishStage);
  }

  @Test
  public void testCompleteDAGExecutionForMultipleWorkers()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(2);
    // 0 -> 1 -> 2 -> 3

    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(4)
            .addVertex(0, 1)
            .addVertex(1, 2)
            .addVertex(2, 3)
            .defineStage(0, true, 1) // Ingestion only on one worker
            .defineStage(1, true, 2)
            .defineStage(3, true, 2)
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
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(1), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(), effectivelyFinishedStageNumbers);
    controllerQueryKernelTester.startStage(1);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(1, 0);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(1, 1);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(1, 0);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(1, 1);
    controllerQueryKernelTester.assertStagePhase(1, ControllerStagePhase.RESULTS_READY);

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(2), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(0), effectivelyFinishedStageNumbers);
    controllerQueryKernelTester.startStage(2);
    controllerQueryKernelTester.assertStagePhase(2, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(2, 0);
    controllerQueryKernelTester.assertStagePhase(2, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.finishStage(0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);

    newStageNumbers = controllerQueryKernelTester.createAndGetNewStageNumbers();
    effectivelyFinishedStageNumbers = controllerQueryKernelTester.getEffectivelyFinishedStageNumbers();
    Assert.assertEquals(ImmutableSet.of(3), newStageNumbers);
    Assert.assertEquals(ImmutableSet.of(1), effectivelyFinishedStageNumbers);
    controllerQueryKernelTester.startStage(3);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(3, 0);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(3, 1);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(3, 0);
    controllerQueryKernelTester.assertStagePhase(3, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(3, 1);
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
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(2);

    // Single stage query definition
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(1)
            .defineStage(0, true, 2)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.init();


    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    controllerQueryKernelTester.finishStage(0, false);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);
  }

  @Test
  public void testPrematureResultsComplete()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(2);

    // Single stage query definition
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(1)
            .defineStage(0, true, 2)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);


    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
  }

  @Test
  public void testKernelFailed()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(1);

    // 0  1
    // \  /
    //   2
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(3)
            .addVertex(0, 2)
            .addVertex(1, 2)
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
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(1);

    // 0 - 1
    // \  /
    //   2
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(3)
            .addVertex(0, 1)
            .addVertex(1, 2)
            .addVertex(2, 0)
            .getQueryDefinitionBuilder()
            .build()
    );
  }

  @Test(expected = IllegalStateException.class)
  public void testSelfLoopInvalidQueryThrowsException()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(1);

    // 0 _
    // |__|
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(1)
            .addVertex(0, 0)
            .getQueryDefinitionBuilder()
            .build()
    );
  }

  @Test(expected = IllegalStateException.class)
  public void testLoopInvalidQueryThrowsException()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(1);

    // 0 - 1
    // |   |
    //  ---
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(2)
            .addVertex(0, 1)
            .addVertex(1, 0)
            .getQueryDefinitionBuilder()
            .build()
    );
  }

  @Test
  public void testMarkSuccessfulTerminalStagesAsFinished()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(1);

    // 0  1
    // \  /
    //   2
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(3)
            .addVertex(0, 2)
            .addVertex(1, 2)
            .getQueryDefinitionBuilder()
            .build()
    );

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.setupStage(1, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.setupStage(2, ControllerStagePhase.RESULTS_READY);

    controllerQueryKernelTester.init();

    Assert.assertTrue(controllerQueryKernelTester.isDone());
    Assert.assertTrue(controllerQueryKernelTester.isSuccess());

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
    queryKernelTester.setResultsCompleteForStageAndWorker(stageNumber, 0);
  }

}
