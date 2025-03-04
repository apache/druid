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
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.DartControllerContextFactoryImpl;
import org.apache.druid.msq.dart.worker.DartWorkerClient;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerImpl;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.DruidNode;

import java.util.Map;

public class TestDartControllerContextFactoryImpl extends DartControllerContextFactoryImpl
{
  private Map<String, Worker> workerMap;

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
      @Dart Map<String, Worker> workerMap
      )
  {
    super(injector, jsonMapper, smileMapper, selfNode, serviceClientFactory, memoryIntrospector, serverView, emitter);
    this.workerMap = workerMap;
  }

  @Override
  protected DartWorkerClient makeWorkerClient(String queryId)
  {
    Controller controller = null;
    Worker worker = new WorkerImpl(
        null,
        new MSQTestWorkerContext(
            queryId,
            workerMap,
            controller,
            jsonMapper,
            injector,
            MSQTestBase.makeTestWorkerMemoryParameters(),
            WorkerStorageParameters.createInstanceForTests(Long.MAX_VALUE)
        )
    );

    return new DartTestWorkerClient(
        queryId, serviceClientFactory, smileMapper, selfNode.getHostAndPortToUse(), workerMap
    );
  }

  static class DartTestWorkerClient extends MSQTestWorkerClient implements DartWorkerClient
  {
    private Map<String, Worker> workerMap;

    public DartTestWorkerClient(
        String queryId,
        ServiceClientFactory clientFactory,
        ObjectMapper smileMapper,
        String controllerHost, Map<String, Worker> workerMap
      )
    {
      super(workerMap);
    }

    @Override
    public void closeClient(String hostAndPort)
    {
      if(true)
      {
        throw new RuntimeException("FIXME: Unimplemented!");
      }

    }

    @Override
    public ListenableFuture<?> stopWorker(String workerId)
    {
      if(true)
      {
        throw new RuntimeException("FIXME: Unimplemented!");
      }
      return null;

    }
  }

  // @Override
  // protected ServiceClient getClient(String workerIdString)
  // {
  // return super.getClient(workerIdString);
  // }
  //
  // @Override
  // public void closeClient(String workerHost)
  // {
  // super.closeClient(workerHost);
  // }
  // @Override
  // protected Object clone() throws CloneNotSupportedException
  // {
  // return super.clone();
  // }
  // @Override
  // public ListenableFuture<?> stopWorker(String workerId)
  // {
  // return super.stopWorker(workerId);
  // }
  // @Override
  // public ListenableFuture<Boolean> fetchChannelData(String workerId, StageId
  // stageId, int partitionNumber,
  // long offset, ReadableByteChunksFrameChannel channel)
  // {
  // return super.fetchChannelData(workerId, stageId, partitionNumber, offset,
  // channel);
  // }
  //
  // @Override
  // public ListenableFuture<ClusterByStatisticsSnapshot>
  // fetchClusterByStatisticsSnapshot(String workerId,
  // StageId stageId, SketchEncoding sketchEncoding)
  // {
  // return super.fetchClusterByStatisticsSnapshot(workerId, stageId,
  // sketchEncoding);
  //
  // }
  // @Override
  // public ListenableFuture<ClusterByStatisticsSnapshot>
  // fetchClusterByStatisticsSnapshotForTimeChunk(String workerId,
  // StageId stageId, long timeChunk, SketchEncoding sketchEncoding)
  // {
  // return super.fetchClusterByStatisticsSnapshotForTimeChunk(workerId,
  // stageId, timeChunk, sketchEncoding);
  //
  // }
  // @Override
  // public ListenableFuture<CounterSnapshotsTree> getCounters(String workerId)
  // {
  // return super.getCounters(workerId);
  //
  // }
  // @Override
  // public ListenableFuture<Void> postCleanupStage(String workerId, StageId
  // stageId)
  // {
  // return super.postCleanupStage(workerId, stageId);
  //
  // }
  // @Override
  // public ListenableFuture<Void> postFinish(String workerId)
  // {
  // return super.postFinish(workerId);
  //
  // }
  //
  // @Override
  // public ListenableFuture<Void> postResultPartitionBoundaries(String
  // workerId, StageId stageId,
  // ClusterByPartitions partitionBoundaries)
  // {
  // return super.postResultPartitionBoundaries(workerId, stageId,
  // partitionBoundaries);
  //
  // }
  // @Override
  // public ListenableFuture<Void> postWorkOrder(String workerId, WorkOrder
  // workOrder)
  // {
  // return super.postWorkOrder(workerId, workOrder);
  //
  // }
  //
  // @Override
  // protected Pair<ServiceClient, Closeable> makeNewClient(WorkerId workerId)
  // {
  // ServiceClient client =new TestDartServiceClient(workerId);
  // Closeable c=null;
  // Pair<ServiceClient, Closeable> p = Pair.of(client, c);
  // return p;
  // }
  //
  //
  // }
  //
  // static class TestDartServiceClient implements ServiceClient {
  //
  //
  // private MSQTestWorkerClient a;
  //
  // public TestDartServiceClient(WorkerId workerId)
  // {
  // a = new MSQTestWorkerClient(null);
  // }
  //
  // @Override
  // public <IntermediateType, FinalType> ListenableFuture<FinalType>
  // asyncRequest(RequestBuilder requestBuilder,
  // HttpResponseHandler<IntermediateType, FinalType> handler)
  // {
  // if(true)
  // {
  // throw new RuntimeException("FIXME: Unimplemented!");
  // }
  // return null;
  //
  // }
  //
  // @Override
  // public ServiceClient withRetryPolicy(ServiceRetryPolicy retryPolicy)
  // {
  // if(true)
  // {
  // throw new RuntimeException("FIXME: Unimplemented!");
  // }
  // return null;
  //
  // }
  //
  // }
}
