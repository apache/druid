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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.QueryCounterSnapshot;
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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

  /**
   * Returns the sums of all input channel counters across all workers.
   */
  public ChannelSums getInputChannelSums(final MSQTaskReportPayload payload, final int stageNumber)
  {
    long rows = 0;
    long bytes = 0;
    long files = 0;
    long totalFiles = 0;
    long queries = 0;
    long totalQueries = 0;
    long loadBytes = 0;
    long loadFiles = 0;
    long loadTime = 0;
    long loadWait = 0;

    for (final ChannelCounters.Snapshot snapshot : getAllInputChannelCounters(payload, stageNumber)) {
      rows += sum(snapshot.getRows());
      bytes += sum(snapshot.getBytes());
      files += sum(snapshot.getFiles());
      totalFiles += sum(snapshot.getTotalFiles());
      queries += sum(snapshot.getQueries());
      totalQueries += sum(snapshot.getTotalQueries());
      loadBytes += sum(snapshot.getLoadBytes());
      loadFiles += sum(snapshot.getLoadFiles());
      loadTime += sum(snapshot.getLoadTime());
      loadWait += sum(snapshot.getLoadWait());
    }

    return new ChannelSums(rows, bytes, files, totalFiles, queries, totalQueries, loadBytes, loadFiles, loadTime, loadWait);
  }

  /**
   * Sums the values of a nullable channel counter array, treating {@code null} as empty.
   */
  private static long sum(@Nullable final long[] values)
  {
    return values == null ? 0 : Arrays.stream(values).sum();
  }

  /**
   * Returns all {@link ChannelCounters.Snapshot} from input channels across all workers for a stage.
   */
  private List<ChannelCounters.Snapshot> getAllInputChannelCounters(
      final MSQTaskReportPayload payload,
      final int stageNumber
  )
  {
    final List<ChannelCounters.Snapshot> snapshots = new ArrayList<>();
    final Map<Integer, CounterSnapshots> stageMap = payload.getCounters().snapshotForStage(stageNumber);

    for (final Map.Entry<Integer, CounterSnapshots> workerEntry : stageMap.entrySet()) {
      for (final Map.Entry<String, QueryCounterSnapshot> counterEntry : workerEntry.getValue().getMap().entrySet()) {
        if (counterEntry.getKey().startsWith("input")
            && counterEntry.getValue() instanceof ChannelCounters.Snapshot counterSnapshot) {
          snapshots.add(counterSnapshot);
        }
      }
    }

    return snapshots;
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

  /**
   * Sums of input channel counters computed by {@link #getInputChannelSums(MSQTaskReportPayload, int)}.
   *
   * @param rows         total rows read
   * @param bytes        total bytes read
   * @param files        total files read
   * @param totalFiles   total number of files to read
   * @param queries      total queries completed
   * @param totalQueries total number of queries to run
   * @param loadBytes    total bytes loaded into the virtual storage file cache (VSF)
   * @param loadFiles    total files loaded into the VSF
   * @param loadTime     total time (in milliseconds) spent loading files into the VSF
   * @param loadWait     total time (in milliseconds) spent waiting to load files into the VSF
   */
  public record ChannelSums(
      long rows,
      long bytes,
      long files,
      long totalFiles,
      long queries,
      long totalQueries,
      long loadBytes,
      long loadFiles,
      long loadTime,
      long loadWait
  )
  {
  }
}
