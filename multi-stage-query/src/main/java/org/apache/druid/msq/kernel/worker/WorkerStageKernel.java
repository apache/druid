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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.kernel.ShuffleKind;
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
 * Not thread-safe.
 *
 * @see org.apache.druid.msq.kernel.controller.ControllerQueryKernel state machine on the controller side
 */
public class WorkerStageKernel
{
  private static final Logger log = new Logger(WorkerStageKernel.class);
  private final WorkOrder workOrder;

  private WorkerStagePhase phase = WorkerStagePhase.NEW;

  @Nullable
  private ClusterByStatisticsSnapshot resultKeyStatisticsSnapshot;

  private boolean doneReadingInput;

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
        && workOrder.getStageDefinition().getShuffleSpec().kind() == ShuffleKind.GLOBAL_SORT
        && !workOrder.getStageDefinition().mustGatherResultKeyStatistics()) {
      // Use valueOrThrow instead of a nicer error collection mechanism, because we really don't expect the
      // MAX_PARTITIONS to be exceeded here. It would involve having a shuffleSpec that was statically configured
      // to use a huge number of partitions.
      resultPartitionBoundaries = workOrder.getStageDefinition().generatePartitionBoundariesForShuffle(null).valueOrThrow();
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
    assertPreshuffleStatisticsNeeded(true);
    transitionTo(WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES);
  }

  public void startPreshuffleWritingOutput()
  {
    transitionTo(WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT);
  }

  public void setResultKeyStatisticsSnapshot(@Nullable final ClusterByStatisticsSnapshot resultKeyStatisticsSnapshot)
  {
    assertPreshuffleStatisticsNeeded(resultKeyStatisticsSnapshot != null);
    this.resultKeyStatisticsSnapshot = resultKeyStatisticsSnapshot;
    this.doneReadingInput = true;
  }

  public void setResultPartitionBoundaries(final ClusterByPartitions resultPartitionBoundaries)
  {
    assertPreshuffleStatisticsNeeded(true);
    this.resultPartitionBoundaries = resultPartitionBoundaries;
  }

  public boolean hasResultKeyStatisticsSnapshot()
  {
    return resultKeyStatisticsSnapshot != null;
  }

  public boolean isDoneReadingInput()
  {
    return doneReadingInput;
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
    if (phase == WorkerStagePhase.RESULTS_COMPLETE) {
      return resultObject;
    } else {
      throw new ISE("Results are not ready in phase[%s]", phase);
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

    if (phase.isTerminal()) {
      // Ignore RESULTS_COMPLETE if work is already finished. This can happen if we transition to FINISHED early
      // due to a downstream stage including a limit.
      return;
    }

    transitionTo(WorkerStagePhase.RESULTS_COMPLETE);
    this.resultObject = resultObject;
  }

  public void setStageFinished()
  {
    transitionTo(WorkerStagePhase.FINISHED);
  }

  public void fail(Throwable t)
  {
    Preconditions.checkNotNull(t, "t");

    if (WorkerStagePhase.FAILED.canTransitionFrom(phase)) {
      transitionTo(WorkerStagePhase.FAILED);
      resultKeyStatisticsSnapshot = null;
      resultPartitionBoundaries = null;

      if (exceptionFromFail == null) {
        exceptionFromFail = t;
      }
    } else if (!MSQFaultUtils.isCanceledException(t)) {
      // Current phase is already terminal. Log and suppress this error. It likely happened during cleanup.
      // (Don't log CanceledFault though. Ignore those if they come after the kernel is in a terminal phase.)
      log.warn(t, "Stage[%s] failed while in phase[%s]", getStageDefinition().getId(), phase);
    }
  }

  public boolean addPostedResultsComplete(StageId stageId, int workerNumber)
  {
    return postedResultsComplete.add(Pair.of(stageId, workerNumber));
  }

  private void assertPreshuffleStatisticsNeeded(final boolean delivered)
  {
    if (delivered != workOrder.getStageDefinition().mustGatherResultKeyStatistics()) {
      throw new ISE(
          "Result key statistics %s, but %s, for stage[%s]",
          delivered ? "delivered" : "not delivered",
          workOrder.getStageDefinition().mustGatherResultKeyStatistics() ? "expected" : "not expected",
          workOrder.getStageDefinition().getId()
      );
    }
  }

  private void transitionTo(final WorkerStagePhase newPhase)
  {
    if (newPhase.canTransitionFrom(phase)) {
      log.info(
          "Stage [%d] transitioning from old phase [%s] to new phase [%s]",
          workOrder.getStageNumber(),
          phase,
          newPhase
      );
      phase = newPhase;
    } else {
      throw new IAE(
          "Cannot transition stage[%s] from[%s] to[%s]",
          workOrder.getStageDefinition().getId(),
          phase,
          newPhase
      );
    }
  }
}
