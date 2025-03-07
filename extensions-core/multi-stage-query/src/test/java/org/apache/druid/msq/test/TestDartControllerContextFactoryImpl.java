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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.DartControllerContext;
import org.apache.druid.msq.dart.controller.DartControllerContextFactoryImpl;
import org.apache.druid.msq.dart.worker.DartWorkerClient;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerImpl;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.DruidNode;

import javax.management.RuntimeErrorException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class TestDartControllerContextFactoryImpl extends DartControllerContextFactoryImpl
{
  private Map<String, Worker> workerMap;
  private DartTestControllerContext controllerContext;

  @Inject
  public TestDartControllerContextFactoryImpl(
      final Injector injector,
      @Json final ObjectMapper jsonMapper,
      @Smile final ObjectMapper smileMapper,
      @Self final DruidNode selfNode,
      @EscalatedGlobal final ServiceClientFactory serviceClientFactory,
      final MemoryIntrospector memoryIntrospector,
      final TimelineServerView serverView,
      final ServiceEmitter emitter,
      @Dart Map<String, Worker> workerMap)
  {
    super(injector, jsonMapper, smileMapper, selfNode, serviceClientFactory, memoryIntrospector, serverView, emitter);
    this.workerMap = workerMap;
  }

  @Override
  public ControllerContext newContext(String queryId)
  {
    if (controllerContext != null) {
      throw new RuntimeException();
    }
    controllerContext = new DartTestControllerContext(queryId);
    return controllerContext;
  }

  public DartTestWorkerClient dartTestWorkerClient = new DartTestWorkerClient();

  public class DartTestControllerContext extends DartControllerContext
  {

    public ExecutorService executor = Executors.newCachedThreadPool();
    public Map<String, Worker> inMemoryWorkers = new HashMap<>();

    public DartTestControllerContext(String queryId)
    {
      super(injector, jsonMapper, selfNode, dartTestWorkerClient, memoryIntrospector, serverView, emitter);
    }

    @Override
    public void registerController(Controller controller, Closer closer)
    {
      super.registerController(controller, closer);
      dartTestWorkerClient.controller = controller;
    }

    protected Worker newWorker(String workerId)
    {
      Worker worker = new WorkerImpl(
          null,
          new MSQTestWorkerContext(
              workerId,
              inMemoryWorkers,
              controller,
              jsonMapper,
              injector,
              MSQTestBase.makeTestWorkerMemoryParameters(),
              WorkerStorageParameters.createInstanceForTests(Long.MAX_VALUE)
          )
      );

      executor.submit(() -> {
        try {
          worker.run();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      return worker;
    }
  }

  public class DartTestWorkerClient extends MSQTestWorkerClient implements DartWorkerClient
  {
    public DartControllerContext controllerCtx;

    public DartTestWorkerClient()
    {
      super(new HashMap<>());
    }

    @Override
    protected Worker newWorker(String workerId)
    {
      return makeWorker(workerId);
    }

    private Worker makeWorker(String workerId)
    {
      if (true) {
        throw new RuntimeException("FIXME: Unimplemented!");
      }
      return null;

    }

    @Override
    public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
    {
      return super.postWorkOrder(workerTaskId, workOrder);
    }

    @Override
    public ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId)
    {
      return super.postCleanupStage(workerTaskId, stageId);

    }

    @Override
    public void closeClient(String hostAndPort)
    {
    }

    @Override
    public ListenableFuture<?> stopWorker(String workerId)
    {
      return null;

    }
  }
}
