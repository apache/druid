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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.sql.SqlTaskStatus;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.SqlResourceTestClient;
import org.apache.druid.testing.clients.msq.MsqOverlordResourceTestClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Helper class to aid out ITs for MSQ.
 * This takes all the clients required to make the necessary calls to the client APIs for MSQ and performs the boilerplate
 * functions for the tests
 */
public class MsqTestQueryHelper extends AbstractTestQueryHelper<MsqQueryWithResults>
{

  private final ObjectMapper jsonMapper;
  private final IntegrationTestingConfig config;
  private final MsqOverlordResourceTestClient overlordClient;
  private final SqlResourceTestClient msqClient;


  @Inject
  MsqTestQueryHelper(
      final ObjectMapper jsonMapper,
      final SqlResourceTestClient queryClient,
      final IntegrationTestingConfig config,
      final MsqOverlordResourceTestClient overlordClient,
      final SqlResourceTestClient msqClient
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

  /**
   * Submits a task to the MSQ API with the given query string, and default headers and parameters
   */
  public SqlTaskStatus submitMsqTaskSuccesfully(String sqlQueryString) throws ExecutionException, InterruptedException
  {
    return submitMsqTaskSuccesfully(sqlQueryString, ImmutableMap.of());
  }

  /**
   * Submits a task to the MSQ API with the given query string, and default headers and custom context parameters
   */
  public SqlTaskStatus submitMsqTaskSuccesfully(String sqlQueryString, Map<String, Object> context) throws ExecutionException, InterruptedException
  {
    return submitMsqTaskSuccesfully(new SqlQuery(sqlQueryString, null, false, false, false, context, null), null, null);
  }

  public SqlTaskStatus submitMsqTaskSuccesfully(SqlQuery sqlQuery) throws ExecutionException, InterruptedException
  {
    return submitMsqTaskSuccesfully(sqlQuery, null, null);
  }

  // Run the task, wait for it to complete, fetch the reports, verify the results,

  /**
   * Submits a {@link SqlQuery} to the MSQ API for execution. This method waits for the task to be accepted by the cluster
   * and returns the status associated with the submitted task
   */
  public SqlTaskStatus submitMsqTaskSuccesfully(SqlQuery sqlQuery, String username, String password) throws ExecutionException, InterruptedException
  {
    StatusResponseHolder statusResponseHolder = submitMsqTask(sqlQuery, username, password);
    // Check if the task has been accepted successfully
    HttpResponseStatus httpResponseStatus = statusResponseHolder.getStatus();
    if (!httpResponseStatus.equals(HttpResponseStatus.ACCEPTED)) {
      throw new ISE(
          StringUtils.format(
              "Unable to submit the task successfully. Received response status code [%d], and response content:\n[%s]",
              httpResponseStatus.getCode(),
              statusResponseHolder.getContent()
          )
      );
    }
    String content = statusResponseHolder.getContent();
    SqlTaskStatus sqlTaskStatus;
    try {
      sqlTaskStatus = jsonMapper.readValue(content, SqlTaskStatus.class);
    }
    catch (JsonProcessingException e) {
      throw new ISE("Unable to parse the response");
    }
    return sqlTaskStatus;
  }

  public StatusResponseHolder submitMsqTask(SqlQuery sqlQuery, String username, String password) throws ExecutionException, InterruptedException
  {
    String queryUrl = getQueryURL(config.getBrokerUrl());
    Future<StatusResponseHolder> responseHolderFuture = msqClient.queryAsync(queryUrl, sqlQuery, username, password);
    // It is okay to block here for the result because MSQ tasks return the task id associated with it, which shouldn't
    // consume a lot of time
    try {
      return responseHolderFuture.get(5, TimeUnit.MINUTES);
    }
    catch (TimeoutException e) {
      throw new ISE(e, "Unable to fetch the task id for the submitted task in time.");
    }
  }

  /**
   * The method retries till the task with taskId gets completed i.e. {@link TaskState#isComplete()}} returns true and
   * returns the last fetched state {@link TaskState} of the task
   */
  public TaskState pollTaskIdForCompletion(String taskId) throws Exception
  {
    return RetryUtils.retry(
        () -> {
          TaskStatusPlus taskStatusPlus = overlordClient.getTaskStatus(taskId);
          TaskState statusCode = taskStatusPlus.getStatusCode();
          if (statusCode != null && statusCode.isComplete()) {
            return taskStatusPlus.getStatusCode();
          }
          throw new TaskStillRunningException();
        },
        (Throwable t) -> t instanceof TaskStillRunningException,
        99,
        100
    );
  }

  public void pollTaskIdForSuccess(String taskId) throws Exception
  {
    Assert.assertEquals(pollTaskIdForCompletion(taskId), TaskState.SUCCESS);
  }

  /**
   * Fetches status reports for a given task
   */
  public Map<String, MSQTaskReport> fetchStatusReports(String taskId)
  {
    return overlordClient.getMsqTaskReport(taskId);
  }

  /**
   * Compares the results for a given taskId. It is required that the task has produced some results that can be verified
   */
  private void compareResults(String taskId, MsqQueryWithResults expectedQueryWithResults)
  {
    Map<String, MSQTaskReport> statusReport = fetchStatusReports(taskId);
    MSQTaskReport taskReport = statusReport.get(MSQTaskReport.REPORT_KEY);
    if (taskReport == null) {
      throw new ISE("Unable to fetch the status report for the task [%]", taskId);
    }
    MSQTaskReportPayload taskReportPayload = Preconditions.checkNotNull(
        taskReport.getPayload(),
        "payload"
    );
    MSQResultsReport resultsReport = Preconditions.checkNotNull(
        taskReportPayload.getResults(),
        "Results report for the task id is empty"
    );

    List<Map<String, Object>> actualResults = new ArrayList<>();

    Yielder<Object[]> yielder = resultsReport.getResultYielder();
    List<MSQResultsReport.ColumnAndType> rowSignature = resultsReport.getSignature();

    while (!yielder.isDone()) {
      Object[] row = yielder.get();
      Map<String, Object> rowWithFieldNames = new LinkedHashMap<>();
      for (int i = 0; i < row.length; ++i) {
        rowWithFieldNames.put(rowSignature.get(i).getName(), row[i]);
      }
      actualResults.add(rowWithFieldNames);
      yielder = yielder.next(null);
    }

    QueryResultVerifier.ResultVerificationObject resultsComparison = QueryResultVerifier.compareResults(
        actualResults,
        expectedQueryWithResults.getExpectedResults(),
        Collections.emptyList()
    );
    if (!resultsComparison.isSuccess()) {
      throw new IAE(
          "Expected query result is different from the actual result.\n"
          + "Query: %s\n"
          + "Actual Result: %s\n"
          + "Expected Result: %s\n"
          + "Mismatch Error: %s\n",
          expectedQueryWithResults.getQuery(),
          actualResults,
          expectedQueryWithResults.getExpectedResults(),
          resultsComparison.getErrorMessage()
      );
    }
  }

  /**
   * Runs queries from files using MSQ and compares the results with the ones provided
   */
  @Override
  public void testQueriesFromFile(String filePath, String fullDatasourcePath) throws Exception
  {
    LOG.info("Starting query tests for [%s]", filePath);
    List<MsqQueryWithResults> queries =
        jsonMapper.readValue(
            TestQueryHelper.class.getResourceAsStream(filePath),
            new TypeReference<List<MsqQueryWithResults>>()
            {
            }
        );
    for (MsqQueryWithResults queryWithResults : queries) {
      String queryString = queryWithResults.getQuery();
      String queryWithDatasource = StringUtils.replace(queryString, "%%DATASOURCE%%", fullDatasourcePath);
      SqlTaskStatus sqlTaskStatus = submitMsqTaskSuccesfully(queryWithDatasource);
      if (sqlTaskStatus.getState().isFailure()) {
        throw new ISE(
            "Unable to start the task successfully.\nPossible exception: %s",
            sqlTaskStatus.getError()
        );
      }
      String taskId = sqlTaskStatus.getTaskId();
      pollTaskIdForSuccess(taskId);
      compareResults(taskId, queryWithResults);
    }
  }

  /**
   * Submits a {@link SqlQuery} to the MSQ API for execution. This method waits for the created task to be completed.
   */
  public void submitMsqTaskAndWaitForCompletion(String sqlQueryString, Map<String, Object> context)
      throws Exception
  {
    SqlTaskStatus sqlTaskStatus = submitMsqTaskSuccesfully(sqlQueryString, context);

    LOG.info("Sql Task submitted with task Id - %s", sqlTaskStatus.getTaskId());

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }
    pollTaskIdForCompletion(sqlTaskStatus.getTaskId());
  }

  private static class TaskStillRunningException extends Exception
  {

  }
}
