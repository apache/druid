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
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;

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
}
