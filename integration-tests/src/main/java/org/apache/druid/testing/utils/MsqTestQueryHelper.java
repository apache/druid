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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.sql.SqlTaskStatus;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.MsqOverlordResourceTestClient;
import org.apache.druid.testing.clients.MsqTestClient;
import org.apache.druid.testing.guice.models.MSQResultsReportDeserializable;
import org.apache.druid.testing.guice.models.MSQTaskReportDeserializable;
import org.apache.druid.testing.guice.models.MSQTaskReportPayloadDeserializable;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MsqTestQueryHelper extends AbstractTestQueryHelper<MsqQueryWithResults>
{
  final ObjectMapper jsonMapper;
  final IntegrationTestingConfig config;
  final MsqOverlordResourceTestClient overlordClient;
  final MsqTestClient msqClient;


  @Inject
  MsqTestQueryHelper(
      ObjectMapper jsonMapper,
      MsqTestClient queryClient,
      IntegrationTestingConfig config,
      MsqOverlordResourceTestClient overlordClient,
      MsqTestClient msqClient
  )
  {
    super(jsonMapper, queryClient, config);
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.overlordClient = overlordClient;
    this.msqClient = msqClient;
  }

  @Override
  public String getQueryURL(String schemeAndHost)
  {
    return StringUtils.format("%s/druid/v2/sql/task", schemeAndHost);
  }

  public String submitMsqTask(String sqlQueryString) throws ExecutionException, InterruptedException
  {
    return submitMsqTask(new SqlQuery(sqlQueryString, null, false, false, false, ImmutableMap.of(), null));
  }

  // Run the task, wait for it to complete, fetch the reports, verify the results,
  public String submitMsqTask(SqlQuery sqlQuery) throws ExecutionException, InterruptedException
  {
    String queryUrl = getQueryURL(config.getBrokerUrl());
    Future<StatusResponseHolder> responseHolderFuture = msqClient.queryAsync(queryUrl, sqlQuery);
    // It is okay to block here for the result because MSQ tasks return the task Id associated with it, which shouldn't
    // consume a lot of time
    StatusResponseHolder statusResponseHolder = responseHolderFuture.get();
    HttpResponseStatus httpResponseStatus = statusResponseHolder.getStatus();
    if (!httpResponseStatus.equals(HttpResponseStatus.ACCEPTED)) {
      throw new ISE("Unable to submit the task successfully");
    }
    String content = statusResponseHolder.getContent();
    SqlTaskStatus sqlTaskStatus;
    try {
      sqlTaskStatus = jsonMapper.readValue(content, SqlTaskStatus.class);
    }
    catch (JsonProcessingException e) {
      throw new ISE("Unable to parse the response");
    }
    if (sqlTaskStatus.getState().isFailure()) {
      throw new ISE("Unable to start the task successfully.\nPossible exception: %s", sqlTaskStatus.getError());
    }
    return sqlTaskStatus.getTaskId();
  }

  public TaskState pollTaskIdForCompletion(String taskId, long maxTimeoutSeconds)
  {
    if (maxTimeoutSeconds < 0) {
      throw new IAE("Timeout cannot be negative");
    } else if (maxTimeoutSeconds == 0) {
      maxTimeoutSeconds = Long.MAX_VALUE;
    }
    long time = 0;
    do {
      TaskStatusPlus taskStatusPlus = overlordClient.getTaskStatus(taskId);
      TaskState statusCode = taskStatusPlus.getStatusCode();
      if (statusCode != null && statusCode.isComplete()) {
        return taskStatusPlus.getStatusCode();
      }
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        throw new ISE(e, "Interrupted while polling for task [%s] completion", taskId);
      }
    } while (time++ < maxTimeoutSeconds);
    return overlordClient.getTaskStatus(taskId).getStatusCode();
  }

  public Map<String, MSQTaskReportDeserializable> fetchStatusReports(String taskId)
  {
    return overlordClient.getTaskReportForMsqTask(taskId);
  }

  public void compareResults(String taskId, MsqQueryWithResults expectedQueryWithResults)
  {
    Map<String, MSQTaskReportDeserializable> statusReport = fetchStatusReports(taskId);
    MSQTaskReportDeserializable taskReport = statusReport.get(MSQTaskReport.REPORT_KEY);
    if (taskReport == null) {
      throw new ISE("Unable to fetch the status report for the task [%]", taskId);
    }
    MSQTaskReportPayloadDeserializable taskReportPayload = Preconditions.checkNotNull(
        (MSQTaskReportPayloadDeserializable) taskReport.getPayload(),
        "payload"
    );
    MSQResultsReportDeserializable resultsReport = Preconditions.checkNotNull(
        taskReportPayload.getResults(),
        "Results report for the task id is empty"
    );

    List<Map<String, Object>> actualResults = new ArrayList<>();

    List<Object[]> results = resultsReport.getResults();
    RowSignature rowSignature = resultsReport.getSignature();

    for (Object[] row : results) {
      Map<String, Object> rowWithFieldNames = new HashMap<>();
      for (int i = 0; i < row.length; ++i) {
        rowWithFieldNames.put(rowSignature.getColumnName(i), row[i]);
      }
      actualResults.add(rowWithFieldNames);
    }

    QueryResultVerifier.compareResults(
        actualResults,
        expectedQueryWithResults.getExpectedResults(),
        Collections.emptyList()
    );
  }
}
