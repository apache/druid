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

package org.apache.druid.testing.embedded;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.TaskMetrics;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Contains various utility methods to interact with an {@link EmbeddedDruidCluster}.
 *
 * @see #onLeaderCoordinator(Function)
 * @see #onLeaderOverlord(Function)
 * @see #runSql(String, Object...)
 */
public class EmbeddedClusterApis
{
  private final EmbeddedDruidCluster cluster;

  EmbeddedClusterApis(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }

  public <T> T onLeaderCoordinator(Function<CoordinatorClient, ListenableFuture<T>> coordinatorApi)
  {
    return getResult(coordinatorApi.apply(cluster.leaderCoordinator()));
  }

  public <T> T onLeaderOverlord(Function<OverlordClient, ListenableFuture<T>> overlordApi)
  {
    return getResult(overlordApi.apply(cluster.leaderOverlord()));
  }

  /**
   * Submits the given SQL query to any of the brokers (using {@code BrokerClient})
   * of the cluster.
   *
   * @return The result of the SQL as a single CSV string.
   */
  public String runSql(String sql, Object... args)
  {
    try {
      return getResult(
          cluster.anyBroker().submitSqlQuery(
              new ClientSqlQuery(
                  StringUtils.format(sql, args),
                  ResultFormat.CSV.name(),
                  false,
                  false,
                  false,
                  null,
                  null
              )
          )
      ).trim();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits for the given task to finish successfully. If the given
   * {@link EmbeddedOverlord} is not the leader, this method can only return by
   * throwing an exception upon timeout.
   */
  public void waitForTaskToSucceed(String taskId, EmbeddedOverlord overlord)
  {
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(TaskMetrics.RUN_DURATION)
                      .hasDimension(DruidMetrics.TASK_ID, taskId)
    );
    verifyTaskHasStatus(taskId, TaskStatus.success(taskId));
  }

  /**
   * Waits for the given task to fail, with the given errorMsq. If the given
   * {@link EmbeddedOverlord} is not the leader, this method can only return by
   * throwing an exception upon timeout.
   */
  public void waitForTaskToFail(String taskId, String errorMsg, EmbeddedOverlord overlord)
  {
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(TaskMetrics.RUN_DURATION)
                      .hasDimension(DruidMetrics.TASK_ID, taskId)
    );
    verifyTaskHasStatus(taskId, TaskStatus.failure(taskId, errorMsg));
  }

  /**
   * Fetches the status of the given task from the cluster and verifies that it
   * matches the expected status.
   */
  public void verifyTaskHasStatus(String taskId, TaskStatus expectedStatus)
  {
    final TaskStatusResponse currentStatus = onLeaderOverlord(
        o -> o.taskStatus(taskId)
    );
    Assertions.assertNotNull(currentStatus.getStatus());
    Assertions.assertEquals(
        expectedStatus.getStatusCode(),
        currentStatus.getStatus().getStatusCode(),
        StringUtils.format("Task[%s] has unexpected status", taskId)
    );
    Assertions.assertEquals(
        expectedStatus.getErrorMsg(),
        currentStatus.getStatus().getErrorMsg(),
        StringUtils.format("Task[%s] has unexpected error message", taskId)
    );
  }

  /**
   * Fetches the current status of the given supervisor ID.
   */
  public SupervisorStatus getSupervisorStatus(String supervisorId)
  {
    final List<SupervisorStatus> supervisors = ImmutableList.copyOf(
        onLeaderOverlord(OverlordClient::supervisorStatuses)
    );
    for (SupervisorStatus supervisor : supervisors) {
      if (supervisor.getId().equals(supervisorId)) {
        return supervisor;
      }
    }

    throw new ISE("Could not find supervisor[%s]", supervisorId);
  }

  /**
   * Creates a random datasource name prefixed with {@link TestDataSource#WIKI}.
   */
  public static String createTestDatasourceName()
  {
    return TestDataSource.WIKI + "_" + IdUtils.getRandomId();
  }

  /**
   * Deserializes the given String payload into a Map-based object that may be
   * submitted directly to the Overlord using
   * {@code cluster.callApi().onLeaderOverlord(o -> o.runTask(...))}.
   */
  public static Object createTaskFromPayload(String taskId, String payload)
  {
    final Map<String, Object> task = deserializeJsonToMap(payload);
    task.put("id", taskId);
    return task;
  }

  /**
   * Deserializes the given JSON string into a generic map that can be used to
   * post JSON payloads to a Druid API. Using a generic map allows the client
   * to make requests even if required types are not loaded.
   */
  public static Map<String, Object> deserializeJsonToMap(String payload)
  {
    try {
      return TestHelper.JSON_MAPPER.readValue(
          payload,
          new TypeReference<>() {}
      );
    }
    catch (Exception e) {
      throw new ISE(e, "Could not deserialize payload[%s]", payload);
    }
  }

  private static <T> T getResult(ListenableFuture<T> future)
  {
    return FutureUtils.getUnchecked(future, true);
  }
}
