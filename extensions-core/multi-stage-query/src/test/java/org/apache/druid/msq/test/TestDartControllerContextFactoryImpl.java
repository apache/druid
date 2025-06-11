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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.apache.druid.query.QueryContext;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.DruidNode;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestDartControllerContextFactoryImpl extends DartControllerContextFactoryImpl
{
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setNameFormat("dart-worker-%d").build()
  );

  private Map<String, Worker> workerMap;
  public Controller controller;

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
  public ControllerContext newContext(QueryContext context)
  {
    return new DartControllerContext(
        injector,
        jsonMapper,
        selfNode,
        new DartTestWorkerClient(),
        memoryIntrospector,
        serverView,
        emitter,
        context
    )
    {
      @Override
      public void registerController(Controller currentController, Closer closer)
      {
        super.registerController(currentController, closer);
        controller = currentController;
      }
    };
  }

  public class DartTestWorkerClient extends MSQTestWorkerClient implements DartWorkerClient
  {

    public DartTestWorkerClient()
    {
      super(workerMap);
    }

    @Override
    protected Worker newWorker(String workerId)
    {
      String queryId = workerId;
      Worker worker = new WorkerImpl(
          null,
          new MSQTestWorkerContext(
              queryId,
              inMemoryWorkers,
              controller,
              jsonMapper,
              injector,
              MSQTestBase.makeTestWorkerMemoryParameters(),
              WorkerStorageParameters.createInstanceForTests(Long.MAX_VALUE)
          )
      );

      EXECUTOR.submit(() -> {
        try {
          worker.run();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
        finally {
          inMemoryWorkers.remove(workerId);
        }
      });

      return worker;
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
