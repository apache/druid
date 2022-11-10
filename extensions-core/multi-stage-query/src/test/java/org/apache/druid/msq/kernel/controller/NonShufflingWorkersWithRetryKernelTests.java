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

public class NonShufflingWorkersWithRetryKernelTests extends BaseControllerQueryKernelTest
{
  private static final UnknownFault RETRIABLE_FAULT = UnknownFault.forMessage("");


  @Test
  public void workerFailedAfterInitialization()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition();
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
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition();
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);

    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);


  }

  @Test
  public void workerFailedBeforeAllWorkOrdersSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition();
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }


  @Test
  public void workerFailedBeforeAnyResultsRecieved()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition();

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

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);


  }

  @Test
  public void workerFailedBeforeAllResultsRecieved()
  {

    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition();

    // workorders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 1);
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

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorker(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }

  @Test
  public void workerFailedBeforeFinished()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition();
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.RESULTS_READY);
    controllerQueryKernelTester.init();

    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT).size());
    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT).size());

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
  }


  @Test
  public void workerFailedAfterFinished()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition();
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.FINISHED);
    controllerQueryKernelTester.init();

    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT).size());
    Assert.assertEquals(0, controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT).size());

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.FINISHED);

  }

  @Nonnull
  private ControllerQueryKernelTester getSimpleQueryDefinition()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = testControllerQueryKernel(2);
    // 0 -> 1
    controllerQueryKernelTester.queryDefinition(
        new MockQueryDefinitionBuilder(2)
            .addVertex(0, 1)
            .defineStage(0, false, 2) // Ingestion only on one worker
            .defineStage(1, false, 2)
            .getQueryDefinitionBuilder()
            .build()
    );
    return controllerQueryKernelTester;
  }


  public void multipleWorkerFailedAfterInitialization()
  {

  }

  public void multipleWorkerFailedBeforeAnyWorkOrdersSent() {}

  public void multipleWorkerFailedAfterOneWorkOrdersSent() {}

  public void multipleWorkerFailedBeforeAnyResultsStatsArrive() {}

  public void multipleWorkerFailedBeforeAllResultsStatsArrive() {}

  public void multipleWorkerFailedBeforeAnyPartitionBoundariesAreSent() {}


  public void multipleWorkerFailedBeforeAllPartitionBoundariesAreSent() {}

  public void multipleWorkerFailedBeforeAnyResultsRecieved() {}

  public void multipleWorkerFailedBeforeAllResultsRecieved() {}


}
