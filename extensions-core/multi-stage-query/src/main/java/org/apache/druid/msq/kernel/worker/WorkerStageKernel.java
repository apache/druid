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

package org.apache.druid.msq.kernel.worker;

import com.google.common.base.Preconditions;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

/**
 * Kernel for a worker in a multi-stage query.
 *
 * Instances of this class are state machines for worker execution. Kernels do not do any RPC or deal with any data.
 * This separation of decision-making from the "real world" allows the decision-making to live in one,
 * easy-to-follow place.
 *
 * @see org.apache.druid.msq.kernel.controller.ControllerQueryKernel state machine on the controller side
 */
public class WorkerStageKernel
{
  private final WorkOrder workOrder;

  private WorkerStagePhase phase = WorkerStagePhase.NEW;

  @Nullable
  private ClusterByStatisticsSnapshot resultKeyStatisticsSnapshot;

  @Nullable
  private ClusterByPartitions resultPartitionBoundaries;

  @Nullable
  private Object resultObject;

  @Nullable
  private Throwable exceptionFromFail;

  private final Set<Pair<StageId, Integer>> postedResultsComplete = new HashSet<>();

  private WorkerStageKernel(final WorkOrder workOrder)
  {
    this.workOrder = workOrder;

    if (workOrder.getStageDefinition().doesShuffle()
        && !workOrder.getStageDefinition().mustGatherResultKeyStatistics()) {
      // Use valueOrThrow instead of a nicer error collection mechanism, because we really don't expect the
      // MAX_PARTITIONS to be exceeded here. It would involve having a shuffleSpec that was statically configured
      // to use a huge number of partitions.
      resultPartitionBoundaries = workOrder.getStageDefinition().generatePartitionsForShuffle(null).valueOrThrow();
    }
  }

  public static WorkerStageKernel create(final WorkOrder workOrder)
  {
    return new WorkerStageKernel(workOrder);
  }

  public WorkerStagePhase getPhase()
  {
    return phase;
  }

  public WorkOrder getWorkOrder()
  {
    return workOrder;
  }

  public StageDefinition getStageDefinition()
  {
    return workOrder.getStageDefinition();
  }

  public void startReading()
  {
    transitionTo(WorkerStagePhase.READING_INPUT);
  }

  public void startPreshuffleWaitingForResultPartitionBoundaries()
  {
    assertPreshuffleStatisticsNeeded();
    transitionTo(WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES);
  }

  public void startPreshuffleWritingOutput()
  {
    assertPreshuffleStatisticsNeeded();
    transitionTo(WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT);
  }

  public void setResultKeyStatisticsSnapshot(final ClusterByStatisticsSnapshot resultKeyStatisticsSnapshot)
  {
    assertPreshuffleStatisticsNeeded();
    this.resultKeyStatisticsSnapshot = resultKeyStatisticsSnapshot;
  }

  public void setResultPartitionBoundaries(final ClusterByPartitions resultPartitionBoundaries)
  {
    assertPreshuffleStatisticsNeeded();
    this.resultPartitionBoundaries = resultPartitionBoundaries;
  }

  public boolean hasResultKeyStatisticsSnapshot()
  {
    return resultKeyStatisticsSnapshot != null;
  }

  public boolean hasResultPartitionBoundaries()
  {
    return resultPartitionBoundaries != null;
  }

  public ClusterByStatisticsSnapshot getResultKeyStatisticsSnapshot()
  {
    return Preconditions.checkNotNull(resultKeyStatisticsSnapshot, "resultKeyStatisticsSnapshot");
  }

  public ClusterByPartitions getResultPartitionBoundaries()
  {
    return Preconditions.checkNotNull(resultPartitionBoundaries, "resultPartitionBoundaries");
  }

  @Nullable
  public Object getResultObject()
  {
    if (phase == WorkerStagePhase.RESULTS_READY || phase == WorkerStagePhase.FINISHED) {
      return resultObject;
    } else {
      throw new ISE("Results are not ready yet");
    }
  }

  public Throwable getException()
  {
    if (phase == WorkerStagePhase.FAILED) {
      return exceptionFromFail;
    } else {
      throw new ISE("Stage has not failed");
    }
  }

  public void setResultsComplete(Object resultObject)
  {
    if (resultObject == null) {
      throw new NullPointerException("resultObject must not be null");
    }

    transitionTo(WorkerStagePhase.RESULTS_READY);
    this.resultObject = resultObject;
  }

  public void setStageFinished()
  {
    transitionTo(WorkerStagePhase.FINISHED);
  }

  public void fail(Throwable t)
  {
    Preconditions.checkNotNull(t, "t");

    transitionTo(WorkerStagePhase.FAILED);
    resultKeyStatisticsSnapshot = null;
    resultPartitionBoundaries = null;

    if (exceptionFromFail == null) {
      exceptionFromFail = t;
    }
  }

  public boolean addPostedResultsComplete(Pair<StageId, Integer> stageIdAndWorkerNumber)
  {
    return postedResultsComplete.add(stageIdAndWorkerNumber);
  }

  private void assertPreshuffleStatisticsNeeded()
  {
    if (!workOrder.getStageDefinition().mustGatherResultKeyStatistics()) {
      throw new ISE(
          "Result partitioning is not necessary for stage [%s]",
          workOrder.getStageDefinition().getId()
      );
    }
  }

  private void transitionTo(final WorkerStagePhase newPhase)
  {
    if (newPhase.canTransitionFrom(phase)) {
      phase = newPhase;
    } else {
      throw new IAE("Cannot transition from [%s] to [%s]", phase, newPhase);
    }
  }
}
