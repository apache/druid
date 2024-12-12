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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.report.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class SingleFileTaskReportFileWriterTest
{
  private static final String TASK_ID = "mytask";

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testReport() throws IOException
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final File file = tempFolder.newFile();
    final SingleFileTaskReportFileWriter writer = new SingleFileTaskReportFileWriter(file);
    writer.setObjectMapper(mapper);
    final TaskReport.ReportMap reportsMap = TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(TASK_ID, null)
    );
    writer.write(TASK_ID, reportsMap);
    Assert.assertEquals(
        reportsMap,
        mapper.readValue(Files.readAllBytes(file.toPath()), new TypeReference<Map<String, TaskReport>>() {})
    );
  }
}
