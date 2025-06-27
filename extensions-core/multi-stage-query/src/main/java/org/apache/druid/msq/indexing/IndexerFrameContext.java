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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.ProcessingBuffers;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.loading.DataSegmentPusher;

import java.io.File;

public class IndexerFrameContext implements FrameContext
{
  private final StageId stageId;
  private final IndexerWorkerContext context;
  private final IndexIO indexIO;
  private final DataSegmentProvider dataSegmentProvider;
  private final ResourceHolder<ProcessingBuffers> processingBuffers;
  private final WorkerMemoryParameters memoryParameters;
  private final WorkerStorageParameters storageParameters;
  private final DataServerQueryHandlerFactory dataServerQueryHandlerFactory;

  public IndexerFrameContext(
      StageId stageId,
      IndexerWorkerContext context,
      IndexIO indexIO,
      DataSegmentProvider dataSegmentProvider,
      ResourceHolder<ProcessingBuffers> processingBuffers,
      DataServerQueryHandlerFactory dataServerQueryHandlerFactory,
      WorkerMemoryParameters memoryParameters,
      WorkerStorageParameters storageParameters
  )
  {
    this.stageId = stageId;
    this.context = context;
    this.indexIO = indexIO;
    this.dataSegmentProvider = dataSegmentProvider;
    this.processingBuffers = processingBuffers;
    this.memoryParameters = memoryParameters;
    this.storageParameters = storageParameters;
    this.dataServerQueryHandlerFactory = dataServerQueryHandlerFactory;
  }

  @Override
  public PolicyEnforcer policyEnforcer()
  {
    return context.policyEnforcer();
  }

  @Override
  public SegmentWrangler segmentWrangler()
  {
    return context.injector().getInstance(SegmentWrangler.class);
  }

  @Override
  public GroupingEngine groupingEngine()
  {
    return context.injector().getInstance(GroupingEngine.class);
  }

  @Override
  public RowIngestionMeters rowIngestionMeters()
  {
    return context.toolbox().getRowIngestionMetersFactory().createRowIngestionMeters();
  }

  @Override
  public DataSegmentProvider dataSegmentProvider()
  {
    return dataSegmentProvider;
  }

  @Override
  public DataServerQueryHandlerFactory dataServerQueryHandlerFactory()
  {
    return dataServerQueryHandlerFactory;
  }


  @Override
  public File tempDir()
  {
    // No need to include query ID; each task handles a single query, so there is no ambiguity.
    return new File(context.tempDir(), StringUtils.format("stage_%06d", stageId.getStageNumber()));
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return context.jsonMapper();
  }

  @Override
  public IndexIO indexIO()
  {
    return indexIO;
  }

  @Override
  public File persistDir()
  {
    return context.toolbox().getPersistDir();
  }

  @Override
  public DataSegmentPusher segmentPusher()
  {
    return context.toolbox().getSegmentPusher();
  }

  @Override
  public IndexMergerV9 indexMerger()
  {
    return context.toolbox().getIndexMergerV9();
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
  public void close()
  {
    processingBuffers.close();
  }
}
