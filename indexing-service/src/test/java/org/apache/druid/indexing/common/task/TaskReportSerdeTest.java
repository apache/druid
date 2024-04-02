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
import org.apache.druid.indexer.report.IngestionStatsAndErrors;
import org.apache.druid.indexer.report.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexer.report.KillTaskReport;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

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
  public void testSerdeOfIngestionReport() throws Exception
  {
    IngestionStatsAndErrorsTaskReport originalReport = buildTestIngestionReport();
    String reportJson = jsonMapper.writeValueAsString(originalReport);
    TaskReport deserialized = jsonMapper.readValue(reportJson, TaskReport.class);

    Assert.assertTrue(deserialized instanceof IngestionStatsAndErrorsTaskReport);

    IngestionStatsAndErrorsTaskReport deserializedReport = (IngestionStatsAndErrorsTaskReport) deserialized;
    Assert.assertEquals(originalReport, deserializedReport);
  }

  @Test
  public void testSerdeOfKillTaskReport() throws Exception
  {
    KillTaskReport originalReport = new KillTaskReport("taskId", new KillTaskReport.Stats(1, 2, 3));
    String reportJson = jsonMapper.writeValueAsString(originalReport);
    TaskReport deserialized = jsonMapper.readValue(reportJson, TaskReport.class);

    Assert.assertTrue(deserialized instanceof KillTaskReport);

    KillTaskReport deserializedReport = (KillTaskReport) deserialized;
    Assert.assertEquals(originalReport, deserializedReport);
  }

  @Test
  public void testWriteReportMapToFileAndRead() throws Exception
  {
    IngestionStatsAndErrorsTaskReport report1 = buildTestIngestionReport();
    final File reportFile = temporaryFolder.newFile();
    final SingleFileTaskReportFileWriter writer = new SingleFileTaskReportFileWriter(reportFile);
    writer.setObjectMapper(jsonMapper);
    TaskReport.ReportMap reportMap1 = TaskReport.buildTaskReports(report1);
    writer.write("testID", reportMap1);

    TaskReport.ReportMap reportMap2 = jsonMapper.readValue(reportFile, TaskReport.ReportMap.class);
    Assert.assertEquals(reportMap1, reportMap2);
  }

  @Test
  public void testWriteReportMapToStringAndRead() throws Exception
  {
    IngestionStatsAndErrorsTaskReport ingestionReport = buildTestIngestionReport();
    TaskReport.ReportMap reportMap = TaskReport.buildTaskReports(ingestionReport);
    String json = jsonMapper.writeValueAsString(reportMap);

    TaskReport.ReportMap deserializedReportMap = jsonMapper.readValue(json, TaskReport.ReportMap.class);
    Assert.assertEquals(reportMap, deserializedReportMap);
  }

  @Test
  public void testSerializationOnMissingPartitionStats() throws Exception
  {
    String json = "{\n"
                  + "  \"type\": \"ingestionStatsAndErrors\",\n"
                  + "  \"taskId\": \"ingestionStatsAndErrors\",\n"
                  + "  \"payload\": {\n"
                  + "    \"ingestionState\": \"COMPLETED\",\n"
                  + "    \"unparseableEvents\": {\n"
                  + "      \"hello\": \"world\"\n"
                  + "    },\n"
                  + "    \"rowStats\": {\n"
                  + "      \"number\": 1234\n"
                  + "    },\n"
                  + "    \"errorMsg\": \"an error message\",\n"
                  + "    \"segmentAvailabilityConfirmed\": true,\n"
                  + "    \"segmentAvailabilityWaitTimeMs\": 1000\n"
                  + "  }\n"
                  + "}";

    IngestionStatsAndErrorsTaskReport expected = new IngestionStatsAndErrorsTaskReport(
        IngestionStatsAndErrorsTaskReport.REPORT_KEY,
        new IngestionStatsAndErrors(
            IngestionState.COMPLETED,
            ImmutableMap.of(
                "hello", "world"
            ),
            ImmutableMap.of(
                "number", 1234
            ),
            "an error message",
            true,
            1000L,
            null,
            null,
            null
        )
    );


    Assert.assertEquals(expected, jsonMapper.readValue(
        json,
        new TypeReference<TaskReport>()
        {
        }
    ));
  }

  @Test
  public void testExceptionWhileWritingReport() throws Exception
  {
    final File reportFile = temporaryFolder.newFile();
    final SingleFileTaskReportFileWriter writer = new SingleFileTaskReportFileWriter(reportFile);
    writer.setObjectMapper(jsonMapper);
    writer.write("theTask", TaskReport.buildTaskReports(new ExceptionalTaskReport()));

    // Read the file, ensure it's incomplete and not valid JSON. This allows callers to determine the report was
    // not complete when written.
    Assert.assertEquals(
        "{\"report\":{\"type\":\"exceptional\"",
        Files.asCharSource(reportFile, StandardCharsets.UTF_8).read()
    );
  }

  private IngestionStatsAndErrorsTaskReport buildTestIngestionReport()
  {
    return new IngestionStatsAndErrorsTaskReport(
        "testID",
        new IngestionStatsAndErrors(
            IngestionState.BUILD_SEGMENTS,
            Collections.singletonMap("hello", "world"),
            Collections.singletonMap("number", 1234),
            "an error message",
            true,
            1000L,
            Collections.singletonMap("PartitionA", 5000L),
            5L,
            10L
        )
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
