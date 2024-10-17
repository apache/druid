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
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.exec.ProcessingBuffers;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.loading.DataSegmentPusher;

import javax.annotation.Nullable;
import java.io.File;

/**
 * Dart implementation of {@link FrameContext}.
 */
public class DartFrameContext implements FrameContext
{
  private final StageId stageId;
  private final SegmentWrangler segmentWrangler;
  private final GroupingEngine groupingEngine;
  private final DataSegmentProvider dataSegmentProvider;
  private final WorkerContext workerContext;
  @Nullable
  private final ResourceHolder<ProcessingBuffers> processingBuffers;
  private final WorkerMemoryParameters memoryParameters;
  private final WorkerStorageParameters storageParameters;

  public DartFrameContext(
      final StageId stageId,
      final WorkerContext workerContext,
      final SegmentWrangler segmentWrangler,
      final GroupingEngine groupingEngine,
      final DataSegmentProvider dataSegmentProvider,
      @Nullable ResourceHolder<ProcessingBuffers> processingBuffers,
      final WorkerMemoryParameters memoryParameters,
      final WorkerStorageParameters storageParameters
  )
  {
    this.stageId = stageId;
    this.segmentWrangler = segmentWrangler;
    this.groupingEngine = groupingEngine;
    this.dataSegmentProvider = dataSegmentProvider;
    this.workerContext = workerContext;
    this.processingBuffers = processingBuffers;
    this.memoryParameters = memoryParameters;
    this.storageParameters = storageParameters;
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
  public DataSegmentProvider dataSegmentProvider()
  {
    return dataSegmentProvider;
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
  public IndexMergerV9 indexMerger()
  {
    throw DruidException.defensive("Ingestion not implemented");
  }

  @Override
  public ProcessingBuffers processingBuffers()
  {
    if (processingBuffers != null) {
      return processingBuffers.get();
    } else {
      throw new ISE("No processing buffers");
    }
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
    // We don't query data servers. This factory won't actually be used, because Dart doesn't allow segmentSource to be
    // overridden; it always uses SegmentSource.NONE. (If it is called, some wires got crossed somewhere.)
    return null;
  }

  @Override
  public void close()
  {
    if (processingBuffers != null) {
      processingBuffers.close();
    }
  }
}
