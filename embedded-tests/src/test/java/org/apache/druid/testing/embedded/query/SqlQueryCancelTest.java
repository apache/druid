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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SqlQueryCancelTest extends QueryTestBase
{
  private static final String QUERY = " SELECT sleep(4) FROM %s LIMIT 4";

  private ObjectMapper jsonMapper;
  private String tableName;

  @Override
  public void beforeAll()
  {
    jsonMapper = overlord.bindings().jsonMapper();
    tableName = EmbeddedClusterApis.createTestDatasourceName();
    final String taskId = IdUtils.getRandomId();
    final IndexTask task = MoreResources.Task.BASIC_INDEX.get().dataSource(tableName).withId(taskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(tableName, coordinator, broker);
  }

  @Test
  public void testCancelValidQuery() throws Exception
  {
    final String sqlQuery = StringUtils.format(QUERY, tableName);
    final String queryId = "sql-cancel-test";
    final ClientSqlQuery query = new ClientSqlQuery(
        sqlQuery,
        null,
        false,
        false,
        false,
        ImmutableMap.of(BaseQuery.SQL_QUERY_ID, queryId),
        List.of()
    );

    ListenableFuture<StatusResponseHolder> f = executeQueryAsync(routerEndpoint, jsonMapper.writeValueAsString(query));

    // Wait until the sqlLifecycle is authorized and registered
    Thread.sleep(500L);
    cancelQuery(
        routerEndpoint,
        queryId,
        (r) -> Assertions.assertEquals(HttpResponseStatus.ACCEPTED, r.getStatus())
    );

    StatusResponseHolder srh = f.get();
    Assertions.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), srh.getStatus().getCode());
  }

  @Test
  public void testCancelInvalidQuery() throws Exception
  {
    final String sqlQuery = StringUtils.format(QUERY, tableName);
    final String validQueryId = "sql-cancel-test";
    final String invalidQueryId = "sql-continue-test";
    final ClientSqlQuery query = new ClientSqlQuery(
        sqlQuery,
        null,
        false,
        false,
        false,
        ImmutableMap.of(BaseQuery.SQL_QUERY_ID, validQueryId),
        List.of()
    );

    ListenableFuture<StatusResponseHolder> f = executeQueryAsync(routerEndpoint, jsonMapper.writeValueAsString(query));

    // Wait until the sqlLifecycle is authorized and registered
    Thread.sleep(500L);
    cancelQuery(
        routerEndpoint,
        invalidQueryId,
        (r) -> Assertions.assertEquals(HttpResponseStatus.NOT_FOUND, r.getStatus())
    );

    StatusResponseHolder srh = f.get();
    Assertions.assertEquals(HttpResponseStatus.OK.getCode(), srh.getStatus().getCode());
  }
}
