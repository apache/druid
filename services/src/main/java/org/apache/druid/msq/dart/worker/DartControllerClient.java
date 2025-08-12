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

package org.apache.druid.msq.dart.worker;

import org.apache.druid.common.guava.FutureBox;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.dart.controller.messages.DoneReadingInput;
import org.apache.druid.msq.dart.controller.messages.PartialKeyStatistics;
import org.apache.druid.msq.dart.controller.messages.ResultsComplete;
import org.apache.druid.msq.dart.controller.messages.WorkerError;
import org.apache.druid.msq.dart.controller.messages.WorkerWarning;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Implementation of {@link ControllerClient} that uses an {@link Outbox} to send {@link ControllerMessage}
 * to a controller.
 */
public class DartControllerClient implements ControllerClient
{
  private final Outbox<ControllerMessage> outbox;
  private final String queryId;
  private final String controllerHost;

  /**
   * Currently-outstanding futures. These are tracked so they can be canceled in {@link #close()}.
   */
  private final FutureBox futureBox = new FutureBox();

  public DartControllerClient(
      final Outbox<ControllerMessage> outbox,
      final String queryId,
      final String controllerHost
  )
  {
    this.outbox = outbox;
    this.queryId = queryId;
    this.controllerHost = controllerHost;
  }

  @Override
  public void postPartialKeyStatistics(
      final StageId stageId,
      final int workerNumber,
      final PartialKeyStatisticsInformation partialKeyStatisticsInformation
  )
  {
    validateStage(stageId);
    sendMessage(new PartialKeyStatistics(stageId, workerNumber, partialKeyStatisticsInformation));
  }

  @Override
  public void postDoneReadingInput(StageId stageId, int workerNumber)
  {
    validateStage(stageId);
    sendMessage(new DoneReadingInput(stageId, workerNumber));
  }

  @Override
  public void postResultsComplete(StageId stageId, int workerNumber, @Nullable Object resultObject)
  {
    validateStage(stageId);
    sendMessage(new ResultsComplete(stageId, workerNumber, resultObject));
  }

  @Override
  public void postWorkerError(MSQErrorReport errorWrapper)
  {
    sendMessage(new WorkerError(queryId, errorWrapper));
  }

  @Override
  public void postWorkerWarning(List<MSQErrorReport> errorWrappers)
  {
    sendMessage(new WorkerWarning(queryId, errorWrappers));
  }

  @Override
  public void postCounters(String workerId, CounterSnapshotsTree snapshotsTree)
  {
    // Do nothing. Live counters are not sent to the controller in this mode.
  }

  @Override
  public List<String> getWorkerIds()
  {
    // Workers are set in advance through the WorkOrder, so this method isn't used.
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {
    // Cancel any pending futures.
    futureBox.close();
  }

  private void sendMessage(final ControllerMessage message)
  {
    FutureUtils.getUnchecked(futureBox.register(outbox.sendMessage(controllerHost, message)), true);
  }

  /**
   * Validate that a {@link StageId} has the expected query ID.
   */
  private void validateStage(final StageId stageId)
  {
    if (!stageId.getQueryId().equals(queryId)) {
      throw DruidException.defensive(
          "Expected queryId[%s] but got queryId[%s], stageNumber[%s]",
          queryId,
          stageId.getQueryId(),
          stageId.getStageNumber()
      );
    }
  }
}
