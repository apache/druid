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

import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.kernel.WorkOrder;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.List;

public class ShufflingWorkersWithRetryKernelTests extends BaseControllerQueryKernelTest
{

  private static final UnknownFault RETRIABLE_FAULT = UnknownFault.forMessage("");


  @Test
  public void workerFailedAfterInitialization()
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
  public void workerFailedBeforeAnyWorkOrdersSent()
  {

    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);

    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 1);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }


  @Test
  public void workerFailedBeforeAllWorkOrdersSent()
  {


    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 1);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);


  }


  @Test
  public void workerFailedBeforeAnyResultsStatsArrive()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // workorders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    // fail one worker
    List<WorkOrder> workOrderList = controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(
        0,
        RETRIABLE_FAULT
    );

    // does not enable the current stage to enable running from start
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);
    // only work order of failed worker should be there
    Assert.assertTrue(workOrderList.size() == 1);
    Assert.assertTrue(workOrderList.get(0).getWorkerNumber() == 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 1);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 1);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }

  @Test
  public void workerFailedBeforeAllResultsStatsArrive()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // workorders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    // fail one worker
    List<WorkOrder> workOrderList = controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(
        0,
        RETRIABLE_FAULT
    );

    // does not enable the current stage to enable running from start
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);
    // only work order of failed worker should be there
    Assert.assertTrue(workOrderList.size() == 1);
    Assert.assertTrue(workOrderList.get(0).getWorkerNumber() == 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 1);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);


    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }


  @Test
  public void workerFailedBeforeAnyPartitionBoundariesAreSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // workorders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    // fail one worker
    List<WorkOrder> workOrderList = controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(
        0,
        RETRIABLE_FAULT
    );

    // does not enable the current stage to enable running from start
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);
    // only work order of failed worker should be there
    Assert.assertTrue(workOrderList.size() == 1);
    Assert.assertTrue(workOrderList.get(0).getWorkerNumber() == 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);

    controllerQueryKernelTester.assertStagePhase(
        0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 1);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }


  @Test
  public void workerFailedBeforeAllPartitionBoundariesAreSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // workorders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 1);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    // fail one worker

    List<WorkOrder> workOrderList = controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(
        0,
        RETRIABLE_FAULT
    );

    // does not enable the current stage to enable running from start
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);
    // only work order of failed worker should be there
    Assert.assertTrue(workOrderList.size() == 1);
    Assert.assertTrue(workOrderList.get(0).getWorkerNumber() == 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }


  @Test
  public void workerFailedBeforeAnyResultsRecieved()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.init();

    List<WorkOrder> workOrderList = controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(
        0,
        RETRIABLE_FAULT
    );

    // does not enable the current stage to enable running from start
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);
    // only work order of failed worker should be there
    Assert.assertTrue(workOrderList.size() == 1);
    Assert.assertTrue(workOrderList.get(0).getWorkerNumber() == 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);

    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);


  }


  @Test
  public void workerFailedBeforeAllResultsRecieved()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);

    List<WorkOrder> workOrderList = controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(
        0,
        RETRIABLE_FAULT
    );

    // does not enable the current stage to enable running from start
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);
    // only work order of failed worker should be there
    Assert.assertTrue(workOrderList.size() == 1);
    Assert.assertTrue(workOrderList.get(0).getWorkerNumber() == 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);


    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);


  }


  @Test
  public void workerFailedBeforeFinished()
  {

    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.init();

    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT).size());
    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT).size());

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);


  }

  @Test
  public void workerFailedAfterFinished()
  {


    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.init();

    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT).size());
    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT).size());

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);

  }


  @Nonnull
  private ControllerQueryKernelTester getSimpleQueryDefinition(int numWorkers)
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(numWorkers);
    // 0 -> 1
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(2)
            .addVertex(0, 1)
            .defineStage(0, true, numWorkers) // Ingestion only on one worker
            .defineStage(1, true, numWorkers)
            .getQueryDefinitionBuilder()
            .build()
    );
    return controllerQueryKernelTester;
  }


  @Test
  public void multipleWorkerFailedAfterInitialization()
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
  public void multipleWorkerFailedBeforeAnyWorkOrdersSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);


    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
  }

  @Test
  public void multipleWorkerFailedBeforeAllWorkOrdersSent()
  {

    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);

    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);


    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 0);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addResultKeyStatisticsForStageAndWorker(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 0);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 1);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorker(0, 2);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
  }

  public void multipleWorkerFailedBeforeAnyResultsStatsArrive() {}

  public void multipleWorkerFailedBeforeAllResultsStatsArrive() {}

  public void multipleWorkerFailedBeforeAnyPartitionBoundariesAreSent() {}


  public void multipleWorkerFailedBeforeAllPartitionBoundariesAreSent() {}

  public void multipleWorkerFailedBeforeAnyResultsRecieved() {}

  public void multipleWorkerFailedBeforeAllResultsRecieved() {}


}
