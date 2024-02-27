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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerImpl;
import org.apache.druid.msq.exec.WorkerManagerClient;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MSQTestControllerContext implements ControllerContext
{
  private static final Logger log = new Logger(MSQTestControllerContext.class);
  private static final int NUM_WORKERS = 4;
  private final TaskActionClient taskActionClient;
  private final Map<String, Worker> inMemoryWorkers = new HashMap<>();
  private final ConcurrentMap<String, TaskStatus> statusMap = new ConcurrentHashMap<>();
  private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Execs.multiThreaded(
      NUM_WORKERS,
      "MultiStageQuery-test-controller-client"));
  private final CoordinatorClient coordinatorClient;
  private final DruidNode node = new DruidNode(
      "controller",
      "localhost",
      true,
      8080,
      8081,
      true,
      false
  );
  private final Injector injector;
  private final ObjectMapper mapper;
  private final ServiceEmitter emitter = new NoopServiceEmitter();

  private Controller controller;
  private Map<String, TaskReport> report = null;
  private final WorkerMemoryParameters workerMemoryParameters;

  public MSQTestControllerContext(
      ObjectMapper mapper,
      Injector injector,
      TaskActionClient taskActionClient,
      WorkerMemoryParameters workerMemoryParameters,
      List<ImmutableSegmentLoadInfo> loadedSegments
  )
  {
    this.mapper = mapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
    coordinatorClient = Mockito.mock(CoordinatorClient.class);

    Mockito.when(coordinatorClient.fetchServerViewSegments(
                    ArgumentMatchers.anyString(),
                    ArgumentMatchers.any()
                 )
    ).thenAnswer(invocation -> loadedSegments.stream()
                                             .filter(immutableSegmentLoadInfo ->
                                                         immutableSegmentLoadInfo.getSegment()
                                                                                 .getDataSource()
                                                                                 .equals(invocation.getArguments()[0]))
                                             .collect(Collectors.toList())
    );
    this.workerMemoryParameters = workerMemoryParameters;
  }

  WorkerManagerClient workerManagerClient = new WorkerManagerClient()
  {
    @Override
    public String run(String taskId, MSQWorkerTask task)
    {
      if (controller == null) {
        throw new ISE("Controller needs to be set using the register method");
      }

      WorkerStorageParameters workerStorageParameters;
      // If we are testing durable storage, set a low limit on storage so that the durable storage will be used.
      if (MultiStageQueryContext.isDurableStorageEnabled(QueryContext.of(task.getContext()))) {
        workerStorageParameters = WorkerStorageParameters.createInstanceForTests(100);
      } else {
        workerStorageParameters = WorkerStorageParameters.createInstanceForTests(Long.MAX_VALUE);
      }

      Worker worker = new WorkerImpl(
          task,
          new MSQTestWorkerContext(inMemoryWorkers, controller, mapper, injector, workerMemoryParameters),
          workerStorageParameters
      );
      inMemoryWorkers.put(task.getId(), worker);
      statusMap.put(task.getId(), TaskStatus.running(task.getId()));

      ListenableFuture<TaskStatus> future = executor.submit(worker::run);

      Futures.addCallback(future, new FutureCallback<TaskStatus>()
      {
        @Override
        public void onSuccess(@Nullable TaskStatus result)
        {
          statusMap.put(task.getId(), result);
        }

        @Override
        public void onFailure(Throwable t)
        {
          log.error(t, "error running worker task %s", task.getId());
          statusMap.put(task.getId(), TaskStatus.failure(task.getId(), t.getMessage()));
        }
      }, MoreExecutors.directExecutor());

      return task.getId();
    }

    @Override
    public Map<String, TaskStatus> statuses(Set<String> taskIds)
    {
      Map<String, TaskStatus> result = new HashMap<>();
      for (String taskId : taskIds) {
        TaskStatus taskStatus = statusMap.get(taskId);
        if (taskStatus != null) {

          if (taskStatus.getStatusCode().equals(TaskState.RUNNING) && !inMemoryWorkers.containsKey(taskId)) {
            result.put(taskId, new TaskStatus(taskId, TaskState.FAILED, 0, null, null));
          } else {
            result.put(
                taskId,
                new TaskStatus(
                    taskStatus.getId(),
                    taskStatus.getStatusCode(),
                    taskStatus.getDuration(),
                    taskStatus.getErrorMsg(),
                    taskStatus.getLocation()
                )
            );
          }
        }
      }
      return result;
    }

    @Override
    public TaskLocation location(String workerId)
    {
      final TaskStatus status = statusMap.get(workerId);
      if (status != null && status.getStatusCode().equals(TaskState.RUNNING) && inMemoryWorkers.containsKey(workerId)) {
        return TaskLocation.create("host-" + workerId, 1, -1);
      } else {
        return TaskLocation.unknown();
      }
    }

    @Override
    public void cancel(String workerId)
    {
      final Worker worker = inMemoryWorkers.remove(workerId);
      if (worker != null) {
        worker.stopGracefully();
      }
    }

    @Override
    public void close()
    {
      //do nothing
    }
  };

  @Override
  public ServiceEmitter emitter()
  {
    return emitter;
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return mapper;
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public DruidNode selfNode()
  {
    return node;
  }

  @Override
  public CoordinatorClient coordinatorClient()
  {
    return coordinatorClient;
  }

  @Override
  public TaskActionClient taskActionClient()
  {
    return taskActionClient;
  }

  @Override
  public WorkerManagerClient workerManager()
  {
    return workerManagerClient;
  }

  @Override
  public void registerController(Controller controller, Closer closer)
  {
    this.controller = controller;
  }

  @Override
  public WorkerClient taskClientFor(Controller controller)
  {
    return new MSQTestWorkerClient(inMemoryWorkers);
  }

  @Override
  public void writeReports(String controllerTaskId, Map<String, TaskReport> taskReport)
  {
    if (controller != null && controller.id().equals(controllerTaskId)) {
      report = taskReport;
    }
  }

  public Map<String, TaskReport> getAllReports()
  {
    return report;
  }
}
