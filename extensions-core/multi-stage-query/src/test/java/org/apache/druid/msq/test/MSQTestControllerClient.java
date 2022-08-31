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

package org.apache.druid.msq.test;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;

import javax.annotation.Nullable;
import java.util.List;

public class MSQTestControllerClient implements ControllerClient
{
  private final Controller controller;

  public MSQTestControllerClient(Controller controller)
  {
    this.controller = controller;
  }

  @Override
  public void postKeyStatistics(
      StageId stageId,
      int workerNumber,
      ClusterByStatisticsSnapshot keyStatistics
  )
  {
    try {
      controller.updateStatus(stageId.getStageNumber(), workerNumber, keyStatistics);
    }
    catch (Exception e) {
      throw new ISE(e, "unable to post key statistics");
    }
  }

  @Override
  public void postCounters(CounterSnapshotsTree snapshotsTree)
  {
    if (snapshotsTree != null) {
      controller.updateCounters(snapshotsTree);
    }
  }

  @Override
  public void postResultsComplete(StageId stageId, int workerNumber, @Nullable Object resultObject)
  {
    controller.resultsComplete(stageId.getQueryId(), stageId.getStageNumber(), workerNumber, resultObject);
  }

  @Override
  public void postWorkerError(String workerId, MSQErrorReport errorWrapper)
  {
    controller.workerError(errorWrapper);
  }

  @Override
  public void postWorkerWarning(String workerId, List<MSQErrorReport> MSQErrorReports)
  {
    controller.workerWarning(MSQErrorReports);
  }

  @Override
  public List<String> getTaskList()
  {
    return controller.getTaskIds();
  }

  @Override
  public void close()
  {
    // Nothing to do.
  }
}
