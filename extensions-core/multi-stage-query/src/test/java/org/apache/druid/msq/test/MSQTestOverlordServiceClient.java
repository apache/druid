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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerImpl;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class MSQTestOverlordServiceClient extends NoopOverlordClient
{
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final TaskActionClient taskActionClient;
  private final WorkerMemoryParameters workerMemoryParameters;
  private Map<String, Controller> inMemoryControllers = new HashMap<>();
  private Map<String, Map<String, TaskReport>> reports = new HashMap<>();
  private Map<String, MSQSpec> msqSpec = new HashMap<>();

  public MSQTestOverlordServiceClient(
      ObjectMapper objectMapper,
      Injector injector,
      TaskActionClient taskActionClient,
      WorkerMemoryParameters workerMemoryParameters
  )
  {
    this.objectMapper = objectMapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
    this.workerMemoryParameters = workerMemoryParameters;
  }

  @Override
  public ListenableFuture<Void> runTask(String taskId, Object taskObject)
  {
    ControllerImpl controller = null;
    MSQTestControllerContext msqTestControllerContext = null;
    try {
      msqTestControllerContext = new MSQTestControllerContext(
          objectMapper,
          injector,
          taskActionClient,
          workerMemoryParameters
      );

      MSQControllerTask cTask = objectMapper.convertValue(taskObject, MSQControllerTask.class);
      msqSpec.put(cTask.getId(), cTask.getQuerySpec());

      controller = new ControllerImpl(
          cTask,
          msqTestControllerContext
      );

      inMemoryControllers.put(cTask.getId(), controller);

      controller.run();
      return Futures.immediateFuture(null);
    }
    catch (Exception e) {
      throw new ISE(e, "Unable to run");
    }
    finally {
      if (controller != null && msqTestControllerContext != null) {
        reports.put(controller.id(), msqTestControllerContext.getAllReports());
      }
    }
  }

  @Override
  public ListenableFuture<Void> cancelTask(String taskId)
  {
    inMemoryControllers.get(taskId).stopGracefully();
    return Futures.immediateFuture(null);
  }

  // hooks to pull stuff out for testing
  @Nullable
  Map<String, TaskReport> getReportForTask(String id)
  {
    return reports.get(id);
  }

  @Nullable
  MSQSpec getQuerySpecForTask(String id)
  {
    return msqSpec.get(id);
  }

}
