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
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;

import java.io.File;

public class IndexerFrameContext implements FrameContext
{
  private final IndexerWorkerContext context;
  private final IndexIO indexIO;
  private final DataSegmentProvider dataSegmentProvider;
  private final WorkerMemoryParameters memoryParameters;

  public IndexerFrameContext(
      IndexerWorkerContext context,
      IndexIO indexIO,
      DataSegmentProvider dataSegmentProvider,
      WorkerMemoryParameters memoryParameters
  )
  {
    this.context = context;
    this.indexIO = indexIO;
    this.dataSegmentProvider = dataSegmentProvider;
    this.memoryParameters = memoryParameters;
  }

  @Override
  public JoinableFactory joinableFactory()
  {
    return context.injector().getInstance(JoinableFactory.class);
  }

  @Override
  public GroupByStrategySelector groupByStrategySelector()
  {
    return context.injector().getInstance(GroupByStrategySelector.class);
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
  public File tempDir()
  {
    return context.tempDir();
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
  public WorkerMemoryParameters memoryParameters()
  {
    return memoryParameters;
  }
}
