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
import org.apache.druid.msq.kernel.ShuffleKind;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

public class ShufflingWorkersWithRetryKernelTest extends BaseControllerQueryKernelTest
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

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
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
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }

  @Test
  public void testWorkerFailedBeforeAnyPartialKeyInfoReceived()
  {


    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }


  @Test
  public void testWorkerFailedBeforeAllPartialKeyInfoReceived()
  {


    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);

    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }


  @Test
  public void testWorkerFailedBeforeAnyPartialKeyStatsFetchingFinishes()
  {


    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);

    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);

    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }


  @Test
  public void testWorkerFailedBeforeAllPartialKeyStatsFetchingFinishes()
  {


    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);

    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);

    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

  }


  @Test
  public void testWorkerFailedBeforeAnyStatsAreMerged()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // work orders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);


    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);


    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }


  @Test
  public void testWorkerFailedBeforeAllStatsAreMerged()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // work orders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);


    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);


    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }


  @Test
  public void testWorkerFailedBeforeAnyPartitionBoundariesAreSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // work orders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.init();


    // fail one worker
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStage(0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }


  @Test
  public void testWorkerFailedBeforeAllPartitionBoundariesAreSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);

    // work orders sent for both stage
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 1);

    // fail one worker

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }


  @Test
  public void testWorkerFailedBeforeAnyResultsReceived()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);


  }


  @Test
  public void testWorkerFailedBeforeAllResultsReceived()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(2);
    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);


    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

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
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);


    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
  }

  @Test
  public void testMultipleWorkersFailedBeforeAllWorkOrdersSent()
  {

    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.createAndGetNewStageNumbers();
    controllerQueryKernelTester.startStage(0);

    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(0, RETRIABLE_FAULT);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);

    controllerQueryKernelTester.getRetriableWorkOrdersAndChangeState(1, RETRIABLE_FAULT);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);


    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1, 2);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
  }

  @Test
  public void testMultipleWorkersFailedBeforeAnyPartialKeyInfoReceived()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);


    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);


    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1, 0, 2);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 1, 0, 2);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 1, 0, 2);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1, 2);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }

  @Test
  public void testMultipleWorkersFailedBeforeAllPartialKeyInfoReceived()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 2);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }


  @Test
  public void testMultipleWorkersFailedBeforeAnyPartialKeyStatsFetchingFinishes()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1, 2);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }

  @Test
  public void testMultipleWorkersFailedBeforeAllPartialKeyStatsFetchingFinishes()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1, 2);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 2);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 1);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 2);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }

  @Test
  public void testMultipleWorkersFailedBeforeAnyStatsAreMerged()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1, 2);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1, 2);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 2);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }

  @Test
  public void testMultipleWorkersFailedBeforeAllStatsAreMerged()
  {


    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1, 2);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 2);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.statsBeingFetchedForWorkers(0, 0, 1);
    controllerQueryKernelTester.mergeClusterByStatsForAllTimeChunksForWorkers(0, 0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);

  }


  @Test
  public void testMultipleWorkersFailedBeforeAnyPartitionBoundariesAreSent()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);


    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.init();


    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1, 0);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }


  @Test
  public void testMultipleWorkersFailedBeforeAllPartitionBoundariesAreSent()
  {

    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);


    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.MERGING_STATISTICS);
    controllerQueryKernelTester.init();
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 2);


    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);
    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }

  @Test
  public void testMultipleWorkersFailedBeforeAnyResultsReceived()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);
    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 1, 2);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RESULTS_READY);

    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 1);
  }

  @Test
  public void testMultipleWorkersFailedBeforeAllResultsReceived()
  {
    ControllerQueryKernelTester controllerQueryKernelTester = getSimpleQueryDefinition(3);

    controllerQueryKernelTester.setupStage(0, ControllerStagePhase.POST_READING);
    controllerQueryKernelTester.init();

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 2);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(0, 0);
    controllerQueryKernelTester.failWorkerAndAssertWorkOrderes(1, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.RETRYING);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 0);
    controllerQueryKernelTester.sendWorkOrdersForWorkers(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 0);

    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.READING_INPUT);

    controllerQueryKernelTester.addPartialKeyStatsInformation(0, 1);
    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);

    controllerQueryKernelTester.sendPartitionBoundariesForStageAndWorkers(0, 0, 1);


    controllerQueryKernelTester.assertStagePhase(0, ControllerStagePhase.POST_READING);


    Assert.assertTrue(controllerQueryKernelTester.createAndGetNewStageNumbers().size() == 0);

    controllerQueryKernelTester.setResultsCompleteForStageAndWorkers(0, 0, 1);
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
            .defineStage(0, ShuffleKind.GLOBAL_SORT, numWorkers)
            .defineStage(1, ShuffleKind.GLOBAL_SORT, numWorkers)
            .getQueryDefinitionBuilder()
            .build()
    );
    return controllerQueryKernelTester;
  }

}
