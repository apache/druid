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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.query.rowsandcols.serde.WireTransferableContext;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.FrameWriterSpec;
import org.apache.druid.msq.exec.ProcessingBuffers;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.server.SegmentManager;

import java.io.File;

/**
 * Dart implementation of {@link FrameContext}.
 */
public class DartFrameContext implements FrameContext
{
  private final StageId stageId;
  private final FrameWriterSpec frameWriterSpec;
  private final SegmentWrangler segmentWrangler;
  private final GroupingEngine groupingEngine;
  private final SegmentManager segmentManager;
  private final CoordinatorClient coordinatorClient;
  private final WorkerContext workerContext;
  private final ResourceHolder<ProcessingBuffers> processingBuffers;
  private final WorkerMemoryParameters memoryParameters;
  private final WorkerStorageParameters storageParameters;
  private final DataServerQueryHandlerFactory dataServerQueryHandlerFactory;

  public DartFrameContext(
      final StageId stageId,
      final WorkerContext workerContext,
      final FrameWriterSpec frameWriterSpec,
      final SegmentWrangler segmentWrangler,
      final GroupingEngine groupingEngine,
      final SegmentManager segmentManager,
      final CoordinatorClient coordinatorClient,
      final ResourceHolder<ProcessingBuffers> processingBuffers,
      final WorkerMemoryParameters memoryParameters,
      final WorkerStorageParameters storageParameters,
      final DataServerQueryHandlerFactory dataServerQueryHandlerFactory
  )
  {
    this.stageId = stageId;
    this.segmentWrangler = segmentWrangler;
    this.frameWriterSpec = frameWriterSpec;
    this.groupingEngine = groupingEngine;
    this.segmentManager = segmentManager;
    this.coordinatorClient = coordinatorClient;
    this.workerContext = workerContext;
    this.processingBuffers = processingBuffers;
    this.memoryParameters = memoryParameters;
    this.storageParameters = storageParameters;
    this.dataServerQueryHandlerFactory = dataServerQueryHandlerFactory;
  }

  @Override
  public PolicyEnforcer policyEnforcer()
  {
    return workerContext.policyEnforcer();
  }

  @Override
  public SegmentWrangler segmentWrangler()
  {
    return segmentWrangler;
  }

  @Override
  public GroupingEngine groupingEngine()
  {
    return groupingEngine;
  }

  @Override
  public RowIngestionMeters rowIngestionMeters()
  {
    return new NoopRowIngestionMeters();
  }

  @Override
  public SegmentManager segmentManager()
  {
    return segmentManager;
  }

  @Override
  public CoordinatorClient coordinatorClient()
  {
    return coordinatorClient;
  }

  @Override
  public File tempDir()
  {
    return new File(workerContext.tempDir(), stageId.toString());
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return workerContext.jsonMapper();
  }

  @Override
  public WireTransferableContext wireTransferableContext()
  {
    return workerContext.injector().getInstance(WireTransferableContext.class);
  }

  @Override
  public IndexIO indexIO()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public File persistDir()
  {
    return new File(tempDir(), "persist");
  }

  @Override
  public DataSegmentPusher segmentPusher()
  {
    throw DruidException.defensive("Ingestion not implemented");
  }

  @Override
  public IndexMerger indexMerger()
  {
    throw DruidException.defensive("Ingestion not implemented");
  }

  @Override
  public ProcessingBuffers processingBuffers()
  {
    return processingBuffers.get();
  }

  @Override
  public WorkerMemoryParameters memoryParameters()
  {
    return memoryParameters;
  }

  @Override
  public WorkerStorageParameters storageParameters()
  {
    return storageParameters;
  }

  @Override
  public DataServerQueryHandlerFactory dataServerQueryHandlerFactory()
  {
    return dataServerQueryHandlerFactory;
  }

  @Override
  public FrameWriterSpec frameWriterSpec()
  {
    return frameWriterSpec;
  }

  @Override
  public void close()
  {
    processingBuffers.close();
  }
}
