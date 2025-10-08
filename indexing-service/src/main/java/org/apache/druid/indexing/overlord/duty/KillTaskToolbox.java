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

package org.apache.druid.indexing.overlord.duty;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.OutputStream;

/**
 * Wrapper over {@link TaskToolbox} used for embedded kill tasks launched by
 * {@link UnusedSegmentsKiller}.
 */
public class KillTaskToolbox
{
  /**
   * Creates a {@link TaskToolbox} with just enough dependencies to make the
   * embedded kill tasks work in {@link UnusedSegmentsKiller}.
   */
  static TaskToolbox create(
      TaskActionClient taskActionClient,
      DataSegmentKiller dataSegmentKiller,
      ServiceEmitter emitter
  )
  {
    final ObjectMapper mapper = DefaultObjectMapper.INSTANCE;
    final IndexIO indexIO = new IndexIO(mapper, ColumnConfig.DEFAULT);

    return new TaskToolbox.Builder()
        .taskActionClient(taskActionClient)
        .dataSegmentKiller(dataSegmentKiller)
        .taskReportFileWriter(NoopReportWriter.INSTANCE)
        .indexIO(indexIO)
        .indexMerger(new IndexMergerV9(mapper, indexIO, TmpFileSegmentWriteOutMediumFactory.instance(), false))
        .emitter(emitter)
        .build();
  }

  /**
   * Noop report writer.
   */
  private static class NoopReportWriter extends SingleFileTaskReportFileWriter
  {
    private static final NoopReportWriter INSTANCE = new NoopReportWriter();

    private NoopReportWriter()
    {
      super(null);
    }

    @Override
    public void setObjectMapper(ObjectMapper objectMapper)
    {
      // Do nothing
    }

    @Override
    @Nullable
    public File getReportsFile(String taskId)
    {
      return null;
    }

    @Override
    public void write(String taskId, TaskReport.ReportMap reports)
    {
      // Do nothing, metrics are emitted by the KillUnusedSegmentsTask itself
    }

    @Override
    public OutputStream openReportOutputStream(String taskId)
    {
      throw DruidException.defensive("Cannot write reports using this reporter");
    }
  }
}
