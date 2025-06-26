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

package org.apache.druid.testing.simulate.junit5;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;

/**
 * Base class for simulation tests related to ingestion and indexing.
 * This class should not contain any hidden configs or setup, only shorthand
 * utility methods.
 * <p>
 * Steps:
 * <ul>
 * <li>Write a {@code *Test} class that extends this class.</li>
 * <li>Create an {@link EmbeddedDruidCluster} containing all servers, resources,
 * extensions and properties in {@link #createCluster()}.</li>
 * <li>Write one or more {@code @Test} (JUnit5) methods.</li>
 * </ul>
 *
 * @see DruidSimulationTestBase for usage instructions
 */
public abstract class IndexingSimulationTestBase extends DruidSimulationTestBase
{
  /**
   * Random test datasource name that is freshly generated for each test method.
   */
  protected String dataSource;

  @BeforeEach
  protected void beforeEachTest()
  {
    dataSource = createTestDatasourceName();
  }

  /**
   * Gets the result from a future obtained by invoking a Druid API. Use
   * {@code cluster.leaderOverlord()}, {@code cluster.leaderCoordinator()} and
   * {@code cluster.anyBroker()} to see the list of available APIs.
   *
   * @see EmbeddedDruidCluster#leaderOverlord()
   * @see EmbeddedDruidCluster#leaderCoordinator()
   * @see EmbeddedDruidCluster#anyBroker()
   */
  protected static <T> T callApi(ListenableFuture<T> future)
  {
    return FutureUtils.getUnchecked(future, true);
  }

  /**
   * Runs the given SQL query using the broker client used by the {@link #cluster}.
   * This is a shorthand for {@code runSql(callApi(cluster.anyBroker().submitSqlQuery(...)))}.
   *
   * @return The result of the SQL as a single CSV string.
   */
  protected String runSql(String sql, Object... args)
  {
    try {
      return callApi(
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
   * Waits for the given task to finish successfully.
   */
  protected void waitForTaskToSucceed(String taskId, EmbeddedOverlord overlord)
  {
    overlord.waitUntilTaskFinishes(taskId);
    verifyTaskHasStatus(taskId, TaskStatus.success(taskId), overlord);
  }

  protected void verifyTaskHasStatus(String taskId, TaskStatus expectedStatus, EmbeddedOverlord overlord)
  {
    final TaskStatusResponse currentStatus = callApi(
        overlord.client().taskStatus(taskId)
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

  protected SupervisorStatus getSupervisorStatus(String supervisorId)
  {
    final List<SupervisorStatus> supervisors = ImmutableList.copyOf(
        callApi(cluster.leaderOverlord().supervisorStatuses())
    );
    for (SupervisorStatus supervisor : supervisors) {
      if (supervisor.getId().equals(supervisorId)) {
        return supervisor;
      }
    }

    throw new ISE("Could not find supervisor[%s]", supervisorId);
  }

  protected static String createTestDatasourceName()
  {
    return TestDataSource.WIKI + "_" + IdUtils.getRandomId();
  }

  /**
   * Deserializes the given String payload into a Map-based object that may be
   * submitted directly to the Overlord using {@code cluster.leaderOverlord().runTask()}.
   */
  protected static Object createTaskFromPayload(String taskId, String payload)
  {
    try {
      final Map<String, Object> task = TestHelper.JSON_MAPPER.readValue(
          payload,
          new TypeReference<>() {}
      );
      task.put("id", taskId);

      return task;
    }
    catch (Exception e) {
      throw new ISE(e, "Could not deserialize task payload[%s]", payload);
    }
  }
}
