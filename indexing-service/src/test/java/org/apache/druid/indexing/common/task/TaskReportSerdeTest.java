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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TaskReportSerdeTest
{
  private final ObjectMapper jsonMapper;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  public TaskReportSerdeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    jsonMapper.registerSubtypes(ExceptionalTaskReport.class);
  }

  @Test
  public void testSerde() throws Exception
  {
    IngestionStatsAndErrorsTaskReport report1 = new IngestionStatsAndErrorsTaskReport(
        "testID",
        new IngestionStatsAndErrorsTaskReportData(
            IngestionState.BUILD_SEGMENTS,
            ImmutableMap.of(
                "hello", "world"
            ),
            ImmutableMap.of(
                "number", 1234
            ),
            "an error message",
            true,
            1000L,
            ImmutableMap.of("PartitionA", 5000L)
        )
    );
    String report1serialized = jsonMapper.writeValueAsString(report1);
    IngestionStatsAndErrorsTaskReport report2 = (IngestionStatsAndErrorsTaskReport) jsonMapper.readValue(
        report1serialized,
        TaskReport.class
    );
    Assert.assertEquals(report1, report2);
    Assert.assertEquals(report1.hashCode(), report2.hashCode());

    final File reportFile = temporaryFolder.newFile();
    final SingleFileTaskReportFileWriter writer = new SingleFileTaskReportFileWriter(reportFile);
    writer.setObjectMapper(jsonMapper);
    Map<String, TaskReport> reportMap1 = TaskReport.buildTaskReports(report1);
    writer.write("testID", reportMap1);

    Map<String, TaskReport> reportMap2 = jsonMapper.readValue(
        reportFile,
        new TypeReference<Map<String, TaskReport>>() {}
    );
    Assert.assertEquals(reportMap1, reportMap2);
  }

  @Test
  public void testExceptionWhileWritingReport() throws Exception
  {
    final File reportFile = temporaryFolder.newFile();
    final SingleFileTaskReportFileWriter writer = new SingleFileTaskReportFileWriter(reportFile);
    writer.setObjectMapper(jsonMapper);
    writer.write("theTask", ImmutableMap.of("report", new ExceptionalTaskReport()));

    // Read the file, ensure it's incomplete and not valid JSON. This allows callers to determine the report was
    // not complete when written.
    Assert.assertEquals(
        "{\"report\":{\"type\":\"exceptional\"",
        Files.asCharSource(reportFile, StandardCharsets.UTF_8).read()
    );
  }

  /**
   * Task report that throws an exception while being serialized.
   */
  @JsonTypeName("exceptional")
  private static class ExceptionalTaskReport implements TaskReport
  {
    @Override
    @JsonProperty
    public String getTaskId()
    {
      throw new UnsupportedOperationException("cannot serialize task ID");
    }

    @Override
    public String getReportKey()
    {
      return "report";
    }

    @Override
    @JsonProperty
    public Object getPayload()
    {
      throw new UnsupportedOperationException("cannot serialize payload");
    }
  }
}
