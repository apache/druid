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
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.DartControllerContext;
import org.apache.druid.msq.dart.controller.DartControllerContextFactoryImpl;
import org.apache.druid.msq.dart.worker.DartWorkerClient;
import org.apache.druid.msq.dart.worker.DartWorkerService;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.MSQMetricEventBuilder;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerImpl;
import org.apache.druid.msq.exec.WorkerRunRef;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.msq.input.InputSpecSlicerProvider;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.query.QueryContext;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TestDartControllerContextFactoryImpl extends DartControllerContextFactoryImpl
{
  private static final ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(Execs.makeThreadFactory("dart-worker-%d"))
  );

  private final Map<String, WorkerRunRef> workerMap;
  public Controller controller;
  private final ServiceEmitter serviceEmitter = new StubServiceEmitter();

  @Inject
  public TestDartControllerContextFactoryImpl(
      final Injector injector,
      @Json final ObjectMapper jsonMapper,
      @Smile final ObjectMapper smileMapper,
      @Self final DruidNode selfNode,
      @EscalatedGlobal final ServiceClientFactory serviceClientFactory,
      final MemoryIntrospector memoryIntrospector,
      final TimelineServerView serverView,
      @Dart final Set<InputSpecSlicerProvider> inputSpecSlicerProviders,
      final ServiceEmitter emitter,
      final DruidNodeDiscoveryProvider discoveryProvider,
      @Dart Map<String, WorkerRunRef> workerMap
  )
  {
    super(
        injector,
        jsonMapper,
        smileMapper,
        selfNode,
        serviceClientFactory,
        memoryIntrospector,
        serverView,
        inputSpecSlicerProviders,
        emitter,
        discoveryProvider
    );
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
        inputSpecSlicerProviders,
        emitter,
        context,
        advertiseAllHistoricals(serverView)
    )
    {
      @Override
      public void registerController(Controller currentController, Closer closer)
      {
        super.registerController(currentController, closer);
        controller = currentController;
      }

      @Override
      public void emitMetric(MSQMetricEventBuilder metricBuilder)
      {
        serviceEmitter.emit(metricBuilder.build("controller", queryId()));
      }

      @Override
      public boolean isDebug()
      {
        return true;
      }
    };
  }

  /**
   * Builds a {@link DruidNodeDiscovery} that reports every {@link ServerType#HISTORICAL} in the server view as a
   * Dart worker (advertising {@link DartWorkerService}). {@link DartControllerContext#queryKernelConfig} reads
   * only {@link DruidNodeDiscovery#getAllNodes()}, so a no-op listener registration is sufficient.
   */
  private static DruidNodeDiscovery advertiseAllHistoricals(final TimelineServerView serverView)
  {
    final List<DiscoveryDruidNode> nodes =
        serverView.getDruidServerMetadatas()
                  .stream()
                  .filter(server -> server.getType() == ServerType.HISTORICAL)
                  .map(TestDartControllerContextFactoryImpl::historicalDartWorkerNode)
                  .collect(Collectors.toList());

    return new DruidNodeDiscovery()
    {
      @Override
      public Collection<DiscoveryDruidNode> getAllNodes()
      {
        return nodes;
      }

      @Override
      public void registerListener(Listener listener)
      {
        // Not used by queryKernelConfig.
      }
    };
  }

  /**
   * A Historical {@link DiscoveryDruidNode} advertising {@link DartWorkerService}, whose
   * {@link DruidNode#getHostAndPortToUse()} matches {@link DruidServerMetadata#getHost()}.
   */
  public static DiscoveryDruidNode historicalDartWorkerNode(final DruidServerMetadata server)
  {
    // Build a DruidNode whose getHostAndPortToUse() equals server.getHost() (the key queryKernelConfig matches on).
    final boolean tls = server.getHostAndTlsPort() != null;
    final HostAndPort hostAndPort = HostAndPort.fromString(server.getHost());
    final DruidNode druidNode = new DruidNode(
        "no",
        hostAndPort.getHost(),
        false,
        tls ? -1 : hostAndPort.getPort(),
        tls ? hostAndPort.getPort() : -1,
        !tls,
        tls
    );
    return new DiscoveryDruidNode(
        druidNode,
        NodeRole.HISTORICAL,
        ImmutableMap.of(DartWorkerService.NAME, new DartWorkerService())
    );
  }

  public class DartTestWorkerClient extends MSQTestWorkerClient implements DartWorkerClient
  {

    public DartTestWorkerClient()
    {
      super(workerMap, jsonMapper, true);
    }

    @Override
    protected WorkerRunRef newWorker(String workerId)
    {
      final Worker worker = new WorkerImpl(
          null,
          new MSQTestWorkerContext(
              workerId,
              inMemoryWorkers,
              controller,
              jsonMapper,
              injector,
              MSQTestBase.makeTestWorkerMemoryParameters(),
              WorkerStorageParameters.createInstanceForTests(Long.MAX_VALUE),
              serviceEmitter,
              null // No CoordinatorClient needed for Dart
          )
      );
      final WorkerRunRef workerRunRef = new WorkerRunRef();
      workerRunRef.run(worker, EXECUTOR)
                  .addListener(() -> inMemoryWorkers.remove(workerId), MoreExecutors.directExecutor());
      return workerRunRef;
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
