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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Injector;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerImpl;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MSQTestOverlordServiceClient extends NoopOverlordClient
{
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final TaskActionClient taskActionClient;
  private final WorkerMemoryParameters workerMemoryParameters;
  private final List<ImmutableSegmentLoadInfo> loadedSegmentMetadata;
  private final Map<String, Controller> inMemoryControllers = new HashMap<>();
  private final Map<String, Map<String, TaskReport>> reports = new HashMap<>();
  private final Map<String, MSQControllerTask> inMemoryControllerTask = new HashMap<>();
  private final Map<String, TaskStatus> inMemoryTaskStatus = new HashMap<>();

  public static final DateTime CREATED_TIME = DateTimes.of("2023-05-31T12:00Z");
  public static final DateTime QUEUE_INSERTION_TIME = DateTimes.of("2023-05-31T12:01Z");

  public static final long DURATION = 100L;

  public MSQTestOverlordServiceClient(
      ObjectMapper objectMapper,
      Injector injector,
      TaskActionClient taskActionClient,
      WorkerMemoryParameters workerMemoryParameters,
      List<ImmutableSegmentLoadInfo> loadedSegmentMetadata
  )
  {
    this.objectMapper = objectMapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
    this.workerMemoryParameters = workerMemoryParameters;
    this.loadedSegmentMetadata = loadedSegmentMetadata;
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
          workerMemoryParameters,
          loadedSegmentMetadata
      );

      MSQControllerTask cTask = objectMapper.convertValue(taskObject, MSQControllerTask.class);
      inMemoryControllerTask.put(cTask.getId(), cTask);

      controller = new ControllerImpl(cTask, msqTestControllerContext);

      inMemoryControllers.put(controller.id(), controller);

      inMemoryTaskStatus.put(taskId, controller.run());
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

  @Override
  public ListenableFuture<Map<String, Object>> taskReportAsMap(String taskId)
  {
    SettableFuture<Map<String, Object>> future = SettableFuture.create();
    try {
      future.set(
          objectMapper.readValue(
              objectMapper.writeValueAsBytes(getReportForTask(taskId)),
              new TypeReference<Map<String, Object>>()
              {
              }
          ));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return future;
  }

  @Override
  public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
  {
    SettableFuture<TaskPayloadResponse> future = SettableFuture.create();
    future.set(new TaskPayloadResponse(taskId, getMSQControllerTask(taskId)));
    return future;
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
  {
    SettableFuture<TaskStatusResponse> future = SettableFuture.create();
    TaskStatus taskStatus = inMemoryTaskStatus.get(taskId);
    future.set(new TaskStatusResponse(taskId, new TaskStatusPlus(
        taskId,
        null,
        MSQControllerTask.TYPE,
        CREATED_TIME,
        QUEUE_INSERTION_TIME,
        taskStatus.getStatusCode(),
        null,
        DURATION,
        taskStatus.getLocation(),
        null,
        taskStatus.getErrorMsg()
    )));

    return future;
  }

  // hooks to pull stuff out for testing
  @Nullable
  public Map<String, TaskReport> getReportForTask(String id)
  {
    return reports.get(id);
  }

  @Nullable
  MSQControllerTask getMSQControllerTask(String id)
  {
    return inMemoryControllerTask.get(id);
  }
}
