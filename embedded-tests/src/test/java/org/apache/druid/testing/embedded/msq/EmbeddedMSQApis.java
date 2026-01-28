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

package org.apache.druid.testing.embedded.msq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.sql.http.GetQueryReportResponse;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Convenience APIs for issuing MSQ SQL queries.
 */
public class EmbeddedMSQApis
{
  private final EmbeddedDruidCluster cluster;
  private final EmbeddedOverlord overlord;

  public EmbeddedMSQApis(EmbeddedDruidCluster cluster, EmbeddedOverlord overlord)
  {
    this.cluster = cluster;
    this.overlord = overlord;
  }

  /**
   * Submits the given SQL query to any of the brokers (using {@code BrokerClient})
   * of the cluster.
   *
   * @return The result of the SQL as a single CSV string.
   *
   * @see EmbeddedClusterApis#runSql(String, Object...) similar command for the default interactive SQL engine
   */
  public String runDartSql(String sql, Object... args)
  {
    return cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(
            new ClientSqlQuery(
                StringUtils.format(sql, args),
                ResultFormat.CSV.name(),
                false,
                false,
                false,
                Map.of(QueryContexts.ENGINE, DartSqlEngine.NAME),
                null
            )
        )
    ).trim();
  }

  /**
   * Submits the given SQL query to any of the brokers (using {@code BrokerClient})
   * of the cluster, checks that the task has started and returns the {@link SqlTaskStatus}.
   *
   * @return The result of the SQL as a single CSV string.
   */
  public SqlTaskStatus submitTaskSql(String sql, Object... args)
  {
    return submitTaskSql(null, sql, args);
  }

  /**
   * Submits the given SQL query to any of the brokers (using {@code BrokerClient})
   * of the cluster, checks that the task has started and returns the {@link SqlTaskStatus}.
   *
   * @return The result of the SQL as a single CSV string.
   */
  public SqlTaskStatus submitTaskSql(Map<String, Object> queryContext, String sql, Object... args)
  {
    final SqlTaskStatus taskStatus =
        cluster.callApi().onAnyBroker(
            b -> b.submitSqlTask(
                new ClientSqlQuery(
                    StringUtils.format(sql, args),
                    ResultFormat.CSV.name(),
                    false,
                    false,
                    false,
                    queryContext,
                    null
                )
            )
        );

    if (taskStatus.getState() != TaskState.RUNNING) {
      throw DruidException.defensive(
          "Task[%s] had unexpected state[%s]",
          taskStatus.getTaskId(),
          taskStatus.getState()
      );
    }

    return taskStatus;
  }

  /**
   * Submits the given SQL query to any of the brokers (using {@code BrokerClient})
   * of the cluster. Waits for it to complete, then returns the query report.
   *
   * @return The result of the SQL as a single CSV string.
   */
  public MSQTaskReportPayload runTaskSqlAndGetReport(String sql, Object... args)
  {
    SqlTaskStatus taskStatus = submitTaskSql(sql, args);

    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);

    final TaskReport.ReportMap taskReport = cluster.callApi().onLeaderOverlord(
        o -> o.taskReportAsMap(taskStatus.getTaskId())
    );

    final Optional<MSQTaskReport> report = taskReport.findReport(MSQTaskReport.REPORT_KEY);
    final MSQTaskReportPayload taskReportPayload = report.map(MSQTaskReport::getPayload).orElse(null);

    if (taskReportPayload == null) {
      throw DruidException.defensive(
          "No report of type[%s] found for task[%s]",
          MSQTaskReport.REPORT_KEY,
          taskStatus.getTaskId()
      );
    }

    return taskReportPayload;
  }

  /**
   * Gets the query report for a Dart query by its SQL query ID, fetching from a specific broker.
   * Returns null if the query is not found.
   *
   * @param sqlQueryId   the SQL query ID
   * @param targetBroker the broker to fetch the report from
   */
  @Nullable
  public GetQueryReportResponse getDartQueryReport(String sqlQueryId, EmbeddedBroker targetBroker)
  {
    try {
      final String responseJson = cluster.callApi().onTargetBroker(
          targetBroker,
          b -> b.getQueryReport(sqlQueryId, false /* allow cross-broker forwarding */)
      );
      return parseReportResponse(responseJson, targetBroker.bindings().jsonMapper());
    }
    catch (RuntimeException e) {
      // Check if this is a 404 Not Found response
      final Throwable cause = e.getCause();
      if (cause instanceof HttpResponseException) {
        if (((HttpResponseException) cause).getResponse().getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
          return null;
        }
      }
      throw e;
    }
  }

  /**
   * Submits a Dart SQL query asynchronously to a specific broker.
   *
   * @param sql          the SQL query
   * @param context      additional context parameters
   * @param targetBroker the broker to submit the query to
   *
   * @return a future that resolves when the query completes
   */
  public ListenableFuture<String> submitDartSqlAsync(
      String sql,
      Map<String, Object> context,
      EmbeddedBroker targetBroker
  )
  {
    final Map<String, Object> fullContext = new HashMap<>(context);
    fullContext.put(QueryContexts.ENGINE, DartSqlEngine.NAME);

    return cluster.callApi().onTargetBrokerAsync(
        targetBroker,
        b -> b.submitSqlQuery(
            new ClientSqlQuery(
                sql,
                ResultFormat.CSV.name(),
                false,
                false,
                false,
                fullContext,
                null
            )
        )
    );
  }

  /**
   * Cancels a Dart SQL query by its SQL query ID.
   *
   * @param sqlQueryId   the SQL query ID to cancel
   * @param targetBroker the broker where the query is running
   *
   * @return true if the cancellation was accepted
   */
  public boolean cancelDartQuery(String sqlQueryId, EmbeddedBroker targetBroker)
  {
    return cluster.callApi().onTargetBroker(targetBroker, b -> b.cancelSqlQuery(sqlQueryId));
  }

  private static GetQueryReportResponse parseReportResponse(String responseJson, ObjectMapper jsonMapper)
  {
    try {
      return jsonMapper.readValue(responseJson, GetQueryReportResponse.class);
    }
    catch (JsonProcessingException e) {
      throw DruidException.defensive(e, "Failed to parse query report response[%s]", responseJson);
    }
  }
}
