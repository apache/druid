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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.indexer.report.TaskContextReport;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.exec.QueryListener;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Query listener that writes {@link MSQTaskReport} to an {@link OutputStream}.
 *
 * This is used so the report can be written one row at a time, as results are being read, as part of the main
 * query loop. This allows reports to scale to row counts that cannot be materialized in memory, and allows
 * report-writing to be interleaved with query execution when using {@link OutputChannelMode#MEMORY}.
 */
public class TaskReportQueryListener implements QueryListener
{
  private static final String FIELD_TYPE = "type";
  private static final String FIELD_TASK_ID = "taskId";
  private static final String FIELD_PAYLOAD = "payload";
  private static final String FIELD_STATUS = "status";
  private static final String FIELD_STAGES = "stages";
  private static final String FIELD_COUNTERS = "counters";
  private static final String FIELD_RESULTS = "results";
  private static final String FIELD_RESULTS_SIGNATURE = "signature";
  private static final String FIELD_RESULTS_SQL_TYPE_NAMES = "sqlTypeNames";
  private static final String FIELD_RESULTS_RESULTS = "results";
  private static final String FIELD_RESULTS_TRUNCATED = "resultsTruncated";

  private final long rowsInTaskReport;
  private final OutputStreamSupplier reportSink;
  private final ObjectMapper jsonMapper;
  private final SerializerProvider serializers;
  private final String taskId;
  private final Map<String, Object> taskContext;

  private JsonGenerator jg;
  private long numResults;
  private MSQStatusReport statusReport;

  public TaskReportQueryListener(
      final MSQDestination destination,
      final OutputStreamSupplier reportSink,
      final ObjectMapper jsonMapper,
      final String taskId,
      final Map<String, Object> taskContext
  )
  {
    this.rowsInTaskReport = destination.getRowsInTaskReport();
    this.reportSink = reportSink;
    this.jsonMapper = jsonMapper;
    this.serializers = jsonMapper.getSerializerProviderInstance();
    this.taskId = taskId;
    this.taskContext = taskContext;
  }

  @Override
  public boolean readResults()
  {
    return rowsInTaskReport == MSQDestination.UNLIMITED || rowsInTaskReport > 0;
  }

  @Override
  public void onResultsStart(List<MSQResultsReport.ColumnAndType> signature, @Nullable List<SqlTypeName> sqlTypeNames)
  {
    try {
      openGenerator();

      jg.writeObjectFieldStart(FIELD_RESULTS);
      writeObjectField(FIELD_RESULTS_SIGNATURE, signature);
      if (sqlTypeNames != null) {
        writeObjectField(FIELD_RESULTS_SQL_TYPE_NAMES, sqlTypeNames);
      }
      jg.writeArrayFieldStart(FIELD_RESULTS_RESULTS);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean onResultRow(Object[] row)
  {
    try {
      JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, row);
      numResults++;

      if (rowsInTaskReport == MSQDestination.UNLIMITED || numResults < rowsInTaskReport) {
        return true;
      } else {
        jg.writeEndArray();
        jg.writeBooleanField(FIELD_RESULTS_TRUNCATED, true);
        jg.writeEndObject();
        return false;
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onResultsComplete()
  {
    try {
      jg.writeEndArray();
      jg.writeBooleanField(FIELD_RESULTS_TRUNCATED, false);
      jg.writeEndObject();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onQueryComplete(MSQTaskReportPayload report)
  {
    try {
      openGenerator();
      statusReport = report.getStatus();
      writeObjectField(FIELD_STATUS, report.getStatus());

      if (report.getStages() != null) {
        writeObjectField(FIELD_STAGES, report.getStages());
      }

      if (report.getCounters() != null) {
        writeObjectField(FIELD_COUNTERS, report.getCounters());
      }

      jg.writeEndObject(); // End MSQTaskReportPayload
      jg.writeEndObject(); // End MSQTaskReport
      jg.writeObjectField(TaskContextReport.REPORT_KEY, new TaskContextReport(taskId, taskContext));
      jg.writeEndObject(); // End report
      jg.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public MSQStatusReport getStatusReport()
  {
    if (statusReport == null) {
      throw new ISE("Status report not available");
    }

    return statusReport;
  }

  /**
   * Initialize {@link #jg}, if it wasn't already set up. Writes the object start marker, too.
   */
  private void openGenerator() throws IOException
  {
    if (jg == null) {
      jg = jsonMapper.createGenerator(reportSink.get());
      jg.writeStartObject(); // Start report
      jg.writeObjectFieldStart(MSQTaskReport.REPORT_KEY); // Start MSQTaskReport
      jg.writeStringField(FIELD_TYPE, MSQTaskReport.REPORT_KEY);
      jg.writeStringField(FIELD_TASK_ID, taskId);
      jg.writeObjectFieldStart(FIELD_PAYLOAD); // Start MSQTaskReportPayload
    }
  }

  /**
   * Write a field name followed by an object. Unlike {@link JsonGenerator#writeObjectField(String, Object)},
   * this approach avoids the re-creation of a {@link SerializerProvider} for each call.
   */
  private void writeObjectField(final String fieldName, final Object value) throws IOException
  {
    jg.writeFieldName(fieldName);
    JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, value);
  }

  public interface OutputStreamSupplier
  {
    OutputStream get() throws IOException;
  }
}
