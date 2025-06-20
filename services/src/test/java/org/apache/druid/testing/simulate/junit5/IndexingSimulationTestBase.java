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
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;

/**
 * Base class for simulation tests related to ingestion and indexing.
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
    dataSource = createTestDataourceName();
  }

  /**
   * @return Row count in the {@link #dataSource}.
   * @throws RuntimeException if the datasource could not be found.
   */
  protected int getRowCountInDatasource()
  {
    try {
      final String queryResult = getResult(
          cluster.anyBroker().submitSqlQuery(
              new ClientSqlQuery(
                  "SELECT COUNT(*) AS c FROM " + dataSource,
                  ResultFormat.OBJECTLINES.name(),
                  false,
                  false,
                  false,
                  null,
                  null
              )
          )
      );
      Map<String, Object> queryResultMap
          = TestHelper.JSON_MAPPER.readValue(queryResult, new TypeReference<>() {});
      Object rowCount = queryResultMap.get("c");
      Assertions.assertInstanceOf(Integer.class, rowCount);
      return (int) rowCount;
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
    final TaskStatusResponse currentStatus = getResult(
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

  protected static String createTestDataourceName()
  {
    return TestDataSource.WIKI + "_" + IdUtils.getRandomId();
  }
}
