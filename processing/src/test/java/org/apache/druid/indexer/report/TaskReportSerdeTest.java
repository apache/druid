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

package org.apache.druid.indexer.report;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.segment.incremental.RowMeters;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TaskReportSerdeTest
{
  private final ObjectMapper jsonMapper;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  public TaskReportSerdeTest()
  {
    jsonMapper = new DefaultObjectMapper();
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
    KillTaskReport originalReport = new KillTaskReport("taskId", new KillTaskReport.Stats(1, 2));
    String reportJson = jsonMapper.writeValueAsString(originalReport);
    TaskReport deserialized = jsonMapper.readValue(reportJson, TaskReport.class);

    Assert.assertTrue(deserialized instanceof KillTaskReport);

    KillTaskReport deserializedReport = (KillTaskReport) deserialized;
    Assert.assertEquals(originalReport, deserializedReport);
    Assert.assertEquals(originalReport.hashCode(), deserializedReport.hashCode());
  }

  @Test
  public void testSerdeOfTaskContextReport() throws Exception
  {
    TaskContextReport originalReport = new TaskContextReport(
        "taskId",
        ImmutableMap.of("key1", "value1", "key2", "value2")
    );
    String reportJson = jsonMapper.writeValueAsString(originalReport);
    TaskReport deserialized = jsonMapper.readValue(reportJson, TaskReport.class);

    Assert.assertTrue(deserialized instanceof TaskContextReport);

    TaskContextReport deserializedReport = (TaskContextReport) deserialized;
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
  @SuppressWarnings("unchecked")
  public void testWritePlainMapAndReadAsReportMap() throws Exception
  {
    final long now = System.currentTimeMillis();
    final List<ParseExceptionReport> buildUnparseableEvents = Arrays.asList(
        new ParseExceptionReport("abc,def", "invalid timestamp", Arrays.asList("row: 1", "col: 1"), now),
        new ParseExceptionReport("xyz,pqr", "invalid timestamp", Arrays.asList("row: 1", "col: 1"), now)
    );
    final Map<String, Object> unparseableEvents
        = ImmutableMap.of("determinePartitions", Collections.emptyList(), "buildSegments", buildUnparseableEvents);

    final Map<String, Object> emptyAverageMinuteMap = ImmutableMap.of(
        "processed", 0,
        "processedBytes", 0,
        "unparseable", 0,
        "thrownAway", 0,
        "processedWithError", 0
    );

    final Map<String, Object> emptyAverages = ImmutableMap.of(
        "1m", emptyAverageMinuteMap,
        "5m", emptyAverageMinuteMap,
        "15m", emptyAverageMinuteMap
    );

    final Map<String, Object> expectedAverages
        = ImmutableMap.of("determinePartitions", emptyAverages, "buildSegments", emptyAverages);

    final RowIngestionMetersTotals determinePartitionTotalStats
        = RowMeters.with().errors(10).unparseable(1).thrownAway(1).bytes(2000).totalProcessed(100);
    final RowIngestionMetersTotals buildSegmentTotalStats
        = RowMeters.with().errors(5).unparseable(2).thrownAway(1).bytes(2500).totalProcessed(150);
    final Map<String, Object> expectedTotals
        = ImmutableMap.of("determinePartitions", determinePartitionTotalStats, "buildSegments", buildSegmentTotalStats);

    final Map<String, Object> expectedRowStats = ImmutableMap.of(
        "movingAverages", expectedAverages,
        "totals", expectedTotals
    );

    final Map<String, Object> expectedPayload = new HashMap<>();
    expectedPayload.put("ingestionState", IngestionState.COMPLETED);
    expectedPayload.put("unparseableEvents", unparseableEvents);
    expectedPayload.put("rowStats", expectedRowStats);

    final Map<String, Object> ingestionStatsAndErrors = new HashMap<>();
    ingestionStatsAndErrors.put("taskId", "abc");
    ingestionStatsAndErrors.put("payload", expectedPayload);
    ingestionStatsAndErrors.put("type", "ingestionStatsAndErrors");

    final Map<String, Object> expectedReportMap = new HashMap<>();
    expectedReportMap.put("ingestionStatsAndErrors", ingestionStatsAndErrors);

    final String plainMapJson = jsonMapper.writeValueAsString(expectedReportMap);

    // Verify the top-level structure of the report
    final TaskReport.ReportMap deserializedReportMap = jsonMapper.readValue(plainMapJson, TaskReport.ReportMap.class);
    Optional<IngestionStatsAndErrorsTaskReport> ingestStatsReport = deserializedReportMap.findReport(
        "ingestionStatsAndErrors");
    Assert.assertTrue(ingestStatsReport.isPresent());

    Assert.assertEquals("ingestionStatsAndErrors", ingestStatsReport.get().getReportKey());
    Assert.assertEquals("abc", ingestStatsReport.get().getTaskId());

    // Verify basic fields in the payload
    final IngestionStatsAndErrors observedPayload = ingestStatsReport.get().getPayload();
    Assert.assertEquals(expectedPayload.get("ingestionState"), observedPayload.getIngestionState());
    Assert.assertNull(observedPayload.getSegmentsRead());
    Assert.assertNull(observedPayload.getSegmentsPublished());
    Assert.assertNull(observedPayload.getErrorMsg());
    Assert.assertNull(observedPayload.getRecordsProcessed());

    // Verify stats and unparseable events
    final Map<String, Object> observedRowStats = observedPayload.getRowStats();
    Assert.assertEquals(expectedAverages, observedRowStats.get("movingAverages"));

    final Map<String, Object> observedTotals = (Map<String, Object>) observedRowStats.get("totals");
    verifyTotalRowStats(observedTotals, determinePartitionTotalStats, buildSegmentTotalStats);
    verifyUnparseableEvents(observedPayload.getUnparseableEvents(), buildUnparseableEvents);

    // Re-serialize report map and deserialize as plain map
    final String reportMapJson = jsonMapper.writeValueAsString(deserializedReportMap);

    final Map<String, Object> deserializedPlainMap = jsonMapper.readValue(
        reportMapJson,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    final Map<String, Object> ingestStatsReport2
        = (Map<String, Object>) deserializedPlainMap.get("ingestionStatsAndErrors");

    // Verify basic fields in the payload
    final Map<String, Object> observedPayload2 = (Map<String, Object>) ingestStatsReport2.get("payload");
    Assert.assertEquals(expectedPayload.get("ingestionState").toString(), observedPayload2.get("ingestionState"));
    Assert.assertNull(observedPayload2.get("segmentsRead"));
    Assert.assertNull(observedPayload2.get("segmentsPublished"));
    Assert.assertNull(observedPayload2.get("errorMsg"));
    Assert.assertNull(observedPayload2.get("recordsProcessed"));

    // Verify stats and unparseable events
    final Map<String, Object> observedRowStats2 = (Map<String, Object>) observedPayload2.get("rowStats");
    Assert.assertEquals(expectedAverages, observedRowStats2.get("movingAverages"));

    final Map<String, Object> observedTotals2 = (Map<String, Object>) observedRowStats2.get("totals");
    verifyTotalRowStats(observedTotals2, determinePartitionTotalStats, buildSegmentTotalStats);
    verifyUnparseableEvents(
        (Map<String, Object>) observedPayload2.get("unparseableEvents"),
        buildUnparseableEvents
    );
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

    TaskReport deserialized = jsonMapper.readValue(json, TaskReport.class);
    Assert.assertEquals(expected.getTaskId(), deserialized.getTaskId());
    Assert.assertEquals(expected, deserialized);
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

  private void verifyUnparseableEvents(
      Map<String, Object> observed,
      List<ParseExceptionReport> buildSegmentUnparseableEvents
  )
  {
    Assert.assertEquals(Collections.emptyList(), observed.get("determinePartitions"));

    final List<Object> observedBuildSegmentUnparseableEvents
        = (List<Object>) observed.get("buildSegments");
    Assert.assertEquals(2, observedBuildSegmentUnparseableEvents.size());

    for (int i = 0; i < buildSegmentUnparseableEvents.size(); ++i) {
      final ParseExceptionReport expectedEvent = buildSegmentUnparseableEvents.get(i);
      final Object observedEvent = observedBuildSegmentUnparseableEvents.get(i);
      Assert.assertEquals(
          ImmutableMap.of(
              "input", expectedEvent.getInput(),
              "errorType", expectedEvent.getErrorType(),
              "details", expectedEvent.getDetails(),
              "timeOfExceptionMillis", expectedEvent.getTimeOfExceptionMillis()
          ),
          observedEvent
      );
    }
  }

  private void verifyTotalRowStats(
      Map<String, Object> observedTotals,
      RowIngestionMetersTotals determinePartitionTotalStats,
      RowIngestionMetersTotals buildSegmentTotalStats
  )
  {
    Assert.assertEquals(
        ImmutableMap.of(
            "processed", (int) determinePartitionTotalStats.getProcessed(),
            "processedBytes", (int) determinePartitionTotalStats.getProcessedBytes(),
            "processedWithError", (int) determinePartitionTotalStats.getProcessedWithError(),
            "thrownAway", (int) determinePartitionTotalStats.getThrownAway(),
            "unparseable", (int) determinePartitionTotalStats.getUnparseable()
        ),
        observedTotals.get("determinePartitions")
    );
    Assert.assertEquals(
        ImmutableMap.of(
            "processed", (int) buildSegmentTotalStats.getProcessed(),
            "processedBytes", (int) buildSegmentTotalStats.getProcessedBytes(),
            "processedWithError", (int) buildSegmentTotalStats.getProcessedWithError(),
            "thrownAway", (int) buildSegmentTotalStats.getThrownAway(),
            "unparseable", (int) buildSegmentTotalStats.getUnparseable()
        ),
        observedTotals.get("buildSegments")
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
