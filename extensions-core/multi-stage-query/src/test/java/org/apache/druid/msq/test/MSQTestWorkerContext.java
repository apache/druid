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
import com.google.inject.Injector;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.ProcessingBuffers;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.DruidNode;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;

public class MSQTestWorkerContext implements WorkerContext
{
  private static final StupidPool<ByteBuffer> BUFFER_POOL = new StupidPool<>("testProcessing", () -> ByteBuffer.allocate(1_000_000));

  private final String workerId;
  private final Controller controller;
  private final ObjectMapper mapper;
  private final Injector injector;
  private final Map<String, Worker> inMemoryWorkers;
  private final File file = FileUtils.createTempDir();
  private final WorkerMemoryParameters workerMemoryParameters;
  private final WorkerStorageParameters workerStorageParameters;

  public MSQTestWorkerContext(
      String workerId,
      Map<String, Worker> inMemoryWorkers,
      Controller controller,
      ObjectMapper mapper,
      Injector injector,
      WorkerMemoryParameters workerMemoryParameters,
      WorkerStorageParameters workerStorageParameters
  )
  {
    this.workerId = workerId;
    this.inMemoryWorkers = inMemoryWorkers;
    this.controller = controller;
    this.mapper = mapper;
    this.injector = injector;
    this.workerMemoryParameters = workerMemoryParameters;
    this.workerStorageParameters = workerStorageParameters;
  }

  @Override
  public String queryId()
  {
    return controller.queryId();
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return mapper;
  }

  @Override
  public PolicyEnforcer policyEnforcer()
  {
    return injector.getInstance(PolicyEnforcer.class);
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public void registerWorker(Worker worker, Closer closer)
  {

  }

  @Override
  public String workerId()
  {
    return workerId;
  }

  @Override
  public ControllerClient makeControllerClient()
  {
    return new MSQTestControllerClient(controller);
  }

  @Override
  public WorkerClient makeWorkerClient()
  {
    return new MSQTestWorkerClient(inMemoryWorkers);
  }

  @Override
  public File tempDir()
  {
    return file;
  }

  @Override
  public FrameContext frameContext(WorkOrder workOrder)
  {
    return new FrameContextImpl(new File(tempDir(), workOrder.getStageDefinition().getId().toString()));
  }

  @Override
  public int threadCount()
  {
    return 1;
  }

  @Override
  public DruidNode selfNode()
  {
    return new DruidNode("test", "123", true, 8080, 8081, true, false);
  }

  @Override
  public int maxConcurrentStages()
  {
    return 2;
  }

  @Override
  public DataServerQueryHandlerFactory dataServerQueryHandlerFactory()
  {
    return injector.getInstance(DataServerQueryHandlerFactory.class);
  }

  @Override
  public boolean includeAllCounters()
  {
    return true;
  }

  class FrameContextImpl implements FrameContext
  {
    private final File tempDir;

    public FrameContextImpl(File tempDir)
    {
      this.tempDir = tempDir;
    }

    @Override
    public PolicyEnforcer policyEnforcer()
    {
      return MSQTestWorkerContext.this.policyEnforcer();
    }

    @Override
    public SegmentWrangler segmentWrangler()
    {
      return injector.getInstance(SegmentWrangler.class);
    }

    @Override
    public GroupingEngine groupingEngine()
    {
      return injector.getInstance(GroupingEngine.class);
    }

    @Override
    public RowIngestionMeters rowIngestionMeters()
    {
      return new NoopRowIngestionMeters();
    }

    @Override
    public DataSegmentProvider dataSegmentProvider()
    {
      return injector.getInstance(DataSegmentProvider.class);
    }

    @Override
    public DataServerQueryHandlerFactory dataServerQueryHandlerFactory()
    {
      return injector.getInstance(DataServerQueryHandlerFactory.class);
    }

    @Override
    public File tempDir()
    {
      return new File(tempDir, "tmp");
    }

    @Override
    public ObjectMapper jsonMapper()
    {
      return mapper;
    }

    @Override
    public IndexIO indexIO()
    {
      return new IndexIO(mapper, ColumnConfig.DEFAULT);
    }

    @Override
    public File persistDir()
    {
      return new File(tempDir, "persist");
    }

    @Override
    public DataSegmentPusher segmentPusher()
    {
      return injector.getInstance(DataSegmentPusher.class);
    }

    @Override
    public IndexMergerV9 indexMerger()
    {
      return new IndexMergerV9(
          mapper,
          indexIO(),
          OffHeapMemorySegmentWriteOutMediumFactory.instance(),
          true
      );
    }

    @Override
    public ProcessingBuffers processingBuffers()
    {
      return new ProcessingBuffers(
          BUFFER_POOL,
          new Bouncer(1)
      );
    }

    @Override
    public WorkerMemoryParameters memoryParameters()
    {
      return workerMemoryParameters;
    }

    @Override
    public WorkerStorageParameters storageParameters()
    {
      return workerStorageParameters;
    }

    @Override
    public void close()
    {

    }
  }
}
