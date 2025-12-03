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

package org.apache.druid.testing.embedded.query;


import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test for system table queries.
 * In this test we're using deterministic table names to avoid flaky behavior of the test.
 */
public class SystemTableQueryTest extends QueryTestBase
{
  private final String tableName1 = "table_a";
  private final String tableName2 = "table_b";

  @Override
  public void beforeAll()
  {
    final String taskId1 = IdUtils.getRandomId();
    final String taskId2 = IdUtils.getRandomId();
    final IndexTask task1 = MoreResources.Task.BASIC_INDEX.get().dataSource(tableName1).withId(taskId1);
    final IndexTask task2 = MoreResources.Task.BASIC_INDEX.get().dataSource(tableName2).withId(taskId2);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId1, task1));
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId2, task2));

    cluster.callApi().waitForTaskToSucceed(taskId1, overlord);
    cluster.callApi().waitForTaskToSucceed(taskId2, overlord);

    cluster.callApi().waitForAllSegmentsToBeAvailable(tableName1, coordinator, broker);
    cluster.callApi().waitForAllSegmentsToBeAvailable(tableName2, coordinator, broker);
  }

  @Test
  public void testSystemTableQueries_segmentsCount()
  {
    String query = StringUtils.format(
        "SELECT datasource, count(*) \n"
        + "FROM sys.segments \n"
        + "WHERE datasource='%s' \n"
        + "OR    datasource='%s' \n"
        + "GROUP BY 1\n"
        + "ORDER BY 1", tableName1, tableName2
    );

    String[] result = cluster.callApi().runSql(query).split("\n");
    Assertions.assertEquals(2, result.length);
    Assertions.assertEquals(StringUtils.format("%s,10", tableName1), result[0]);
    Assertions.assertEquals(StringUtils.format("%s,10", tableName2), result[1]);
  }

  @Test
  public void testSystemTableQueries_serverTypes()
  {
    String query = "SELECT server_type FROM sys.servers WHERE tier IS NOT NULL AND server_type <> 'indexer'";
    Assertions.assertEquals("historical", cluster.callApi().runSql(query));
  }
}
