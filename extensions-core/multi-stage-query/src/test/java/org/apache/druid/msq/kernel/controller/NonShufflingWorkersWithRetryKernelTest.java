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


import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

public class NonShufflingWorkersWithRetryKernelTest extends BaseControllerQueryKernelTest
{


  @Test
  public void testWorkerFailedAfterInitialization()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1
                      && controllerQueryKernelTester.createAndGetNewStageNumbers().contains(0));
    Assert.assertTrue(controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT).size() == 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.NEW);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1
                      && controllerQueryKernelTester.createAndGetNewStageNumbers().contains(0));
  }

  @Test
  public void testWorkerFailedBeforeAnyWorkOrdersSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);


  }

  @Test
  public void testWorkerFailedBeforeAllWorkOrdersSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }


  @Test
  public void testWorkerFailedBeforeAnyResultsRecieved()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // workorders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    // fail one worker
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);


  }

  @Test
  public void testWorkerFailedBeforeAllResultsRecieved()
  {

    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // workorders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    // fail one worker
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }

  @Test
  public void testWorkerFailedBeforeFinished()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.init();

    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT).size());
    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT).size());

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
  }


  @Test
  public void testWorkerFailedAfterFinished()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.init();

    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT).size());
    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT).size());

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);

  }


  @Test
  public void testMultipleWorkersFailedAfterInitialization()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);
    controllerQueryKernelTester.init();

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1
                      && controllerQueryKernelTester.createAndGetNewStageNumbers().contains(0));
    Assert.assertTrue(controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT).size() == 0);
    Assert.assertTrue(controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT).size() == 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.NEW);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1
                      && controllerQueryKernelTester.createAndGetNewStageNumbers().contains(0));
  }

  @Test
  public void testMultipleWorkersFailedBeforeAnyWorkOrdersSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }

  @Test
  public void testMulttipleWorkerFailedBeforeAllWorkOrdersSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(2, RETRIABLE_FAULT);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }


  @Test
  public void testMultipleWorkersFailedBeforeAnyResultsRecieved()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);


    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);


    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);


  }

  @Test
  public void testMultipleWorkersFailedBeforeAllResultsRecieved()
  {

    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);

    // workorders sent for all stages
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(2, 0);
    // should be no op
    Assert.assertTrue(controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT).size() == 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }


  @Nonnull
  private ControllerQueryKernelTester getSimpleQueryDefinition(int numWorkers)
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(
        configBuilder ->
            configBuilder
                .destination(DurableStorageMSQDestination.instance())
                .durableStorage(true)
                .faultTolerance(true)
                .build()
    );
    // 0 -> 1
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(2)
            .addEdge(0, 1)
            .defineStage(0, null, numWorkers)
            .defineStage(1, null, numWorkers)
            .getQueryDefinitionBuilder()
            .build()
    );
    return controllerQueryKernelTester;
  }

}
