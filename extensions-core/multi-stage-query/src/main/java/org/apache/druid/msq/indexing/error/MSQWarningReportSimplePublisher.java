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

package org.apache.druid.msq.indexing.error;

import com.google.common.collect.ImmutableList;
import org.apache.druid.msq.exec.ControllerClient;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Publishes the warning report to the {@link ControllerClient#postWorkerWarning} as is without any buffering/batching.
 */
public class MSQWarningReportSimplePublisher implements MSQWarningReportPublisher
{

  final String workerId;
  final ControllerClient controllerClient;
  final String taskId;
  @Nullable
  final String host;

  public MSQWarningReportSimplePublisher(
      final String workerId,
      final ControllerClient controllerClient,
      final String taskId,
      @Nullable final String host
  )
  {
    this.workerId = workerId;
    this.controllerClient = controllerClient;
    this.taskId = taskId;
    this.host = host;
  }


  @Override
  public void publishException(int stageNumber, Throwable e)
  {
    final MSQErrorReport warningReport = MSQErrorReport.fromException(taskId, host, stageNumber, e);

    try {
      controllerClient.postWorkerWarning(workerId, ImmutableList.of(warningReport));
    }
    catch (IOException e2) {
      throw new RuntimeException(e2);
    }
  }

  @Override
  public void close()
  {

  }
}
