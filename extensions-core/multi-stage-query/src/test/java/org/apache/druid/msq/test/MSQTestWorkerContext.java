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
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.IndexerFrameContext;
import org.apache.druid.msq.indexing.IndexerWorkerContext;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.security.AuthTestUtils;

import java.io.File;
import java.util.Map;

public class MSQTestWorkerContext implements WorkerContext
{
  private final Controller controller;
  private final ObjectMapper mapper;
  private final Injector injector;
  private final Map<String, Worker> inMemoryWorkers;
  private final File file = FileUtils.createTempDir();
  private final WorkerMemoryParameters workerMemoryParameters;

  public MSQTestWorkerContext(
      Map<String, Worker> inMemoryWorkers,
      Controller controller,
      ObjectMapper mapper,
      Injector injector,
      WorkerMemoryParameters workerMemoryParameters
  )
  {
    this.inMemoryWorkers = inMemoryWorkers;
    this.controller = controller;
    this.mapper = mapper;
    this.injector = injector;
    this.workerMemoryParameters = workerMemoryParameters;
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
  public void registerWorker(Worker worker, Closer closer)
  {

  }

  @Override
  public ControllerClient makeControllerClient(String controllerId)
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
  public FrameContext frameContext(QueryDefinition queryDef, int stageNumber)
  {
    IndexIO indexIO = new IndexIO(
        mapper,
        () -> 0
    );
    IndexMergerV9 indexMerger = new IndexMergerV9(
        mapper,
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
    final TaskReportFileWriter reportFileWriter = new TaskReportFileWriter()
    {
      @Override
      public void write(String taskId, Map<String, TaskReport> reports)
      {

      }

      @Override
      public void setObjectMapper(ObjectMapper objectMapper)
      {

      }
    };

    return new IndexerFrameContext(
        new IndexerWorkerContext(
            new TaskToolbox.Builder()
                .segmentPusher(injector.getInstance(DataSegmentPusher.class))
                .segmentAnnouncer(injector.getInstance(DataSegmentAnnouncer.class))
                .jsonMapper(mapper)
                .taskWorkDir(tempDir())
                .indexIO(indexIO)
                .indexMergerV9(indexMerger)
                .taskReportFileWriter(reportFileWriter)
                .authorizerMapper(AuthTestUtils.TEST_AUTHORIZER_MAPPER)
                .chatHandlerProvider(new NoopChatHandlerProvider())
                .rowIngestionMetersFactory(NoopRowIngestionMeters::new)
                .build(),
            injector,
            indexIO,
            null,
            null
        ),
        indexIO,
        injector.getInstance(DataSegmentProvider.class),
        workerMemoryParameters
    );
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
  public Bouncer processorBouncer()
  {
    return injector.getInstance(Bouncer.class);
  }
}
