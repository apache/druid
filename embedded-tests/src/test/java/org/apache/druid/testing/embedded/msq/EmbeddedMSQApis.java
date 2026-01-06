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
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.broker.BrokerClientImpl;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.sql.http.GetReportResponse;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
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
   * Creates a {@link BrokerClient} that targets the specific broker using a {@link FixedServiceLocator}.
   * Returns null if the query is not found.
   *
   * @param sqlQueryId   the SQL query ID
   * @param targetBroker the broker to fetch the report from
   */
  @Nullable
  public GetReportResponse getDartQueryReport(String sqlQueryId, EmbeddedBroker targetBroker)
  {
    final BrokerClient brokerClient = createBrokerClientForBroker(targetBroker);
    return fetchReportFromBrokerClient(sqlQueryId, brokerClient, targetBroker.bindings().jsonMapper());
  }

  /**
   * Creates a {@link BrokerClient} that targets a specific broker using a {@link FixedServiceLocator}.
   */
  private static BrokerClient createBrokerClientForBroker(EmbeddedBroker targetBroker)
  {
    final ServiceLocation brokerLocation = ServiceLocation.fromDruidNode(targetBroker.bindings().selfNode());
    final ServiceClientFactory clientFactory =
        targetBroker.bindings().getInstance(ServiceClientFactory.class, EscalatedGlobal.class);
    return new BrokerClientImpl(
        clientFactory.makeClient(
            NodeRole.BROKER.getJsonName(),
            new FixedServiceLocator(brokerLocation),
            StandardRetryPolicy.noRetries()
        ),
        targetBroker.bindings().jsonMapper()
    );
  }

  /**
   * Fetches a query report using a {@link BrokerClient}, handling 404 responses.
   *
   * @param sqlQueryId   the SQL query ID to look up
   * @param brokerClient the broker client to use
   * @param jsonMapper   the JSON mapper for parsing the response
   */
  @Nullable
  private static GetReportResponse fetchReportFromBrokerClient(
      final String sqlQueryId,
      final BrokerClient brokerClient,
      final ObjectMapper jsonMapper
  )
  {
    final String responseJson;
    try {
      responseJson = brokerClient.getQueryReport(sqlQueryId, false /* allow cross-broker forwarding */).get();
    }
    catch (Exception e) {
      // Check if this is a 404 Not Found response
      final Throwable cause = e.getCause();
      if (cause instanceof HttpResponseException) {
        if (((HttpResponseException) cause).getResponse().getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
          return null;
        }
      }
      throw new RuntimeException(e);
    }
    return parseReportResponse(responseJson, jsonMapper);
  }

  private static GetReportResponse parseReportResponse(String responseJson, ObjectMapper jsonMapper)
  {
    try {
      return jsonMapper.readValue(responseJson, GetReportResponse.class);
    }
    catch (JsonProcessingException e) {
      throw DruidException.defensive(e, "Failed to parse query report response[%s]", responseJson);
    }
  }
}
