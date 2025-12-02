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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryException;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SqlQueryCancelTest extends QueryTestBase
{

  /**
   * This query will run exactly for 15 seconds.
   */
  private static final String QUERY
      = "SELECT sleep(CASE WHEN added > 0 THEN 1 ELSE 0 END) FROM wikipedia_editstream WHERE added > 0 LIMIT 15";

  private static final int NUM_QUERIES = 3;
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
    final String queryId = "sql-cancel-test";
    final List<Future<StatusResponseHolder>> queryResponseFutures = new ArrayList<>();
    for (int i = 0; i < NUM_QUERIES; i++) {
      queryResponseFutures.add(
          sqlClient.queryAsync(
              sqlHelper.getQueryURL(config.getRouterUrl()),
              new SqlQuery(QUERY, null, false, false, false, ImmutableMap.of(BaseQuery.SQL_QUERY_ID, queryId), null)
          )
      );
    }

    // Wait until the sqlLifecycle is authorized and registered
    Thread.sleep(1000);
    final HttpResponseStatus responseStatus = sqlClient.cancelQuery(
        sqlHelper.getCancelUrl(config.getRouterUrl(), queryId),
        1000
    );
    if (!responseStatus.equals(HttpResponseStatus.ACCEPTED)) {
      throw new RE("Failed to cancel query [%s]. Response code was [%s]", queryId, responseStatus);
    }

    for (Future<StatusResponseHolder> queryResponseFuture : queryResponseFutures) {
      final StatusResponseHolder queryResponse = queryResponseFuture.get(1, TimeUnit.SECONDS);
      if (!queryResponse.getStatus().equals(HttpResponseStatus.INTERNAL_SERVER_ERROR)) {
        throw new ISE("Query is not canceled after cancel request");
      }
      QueryException queryException = jsonMapper.readValue(queryResponse.getContent(), QueryException.class);
      if (!"Query cancelled".equals(queryException.getErrorCode())) {
        throw new ISE(
            "Expected error code [%s], actual [%s]",
            "Query cancelled",
            queryException.getErrorCode()
        );
      }
    }
  }

  @Test
  public void testCancelInvalidQuery() throws Exception
  {
    final Future<StatusResponseHolder> queryResponseFuture = sqlClient
        .queryAsync(
            sqlHelper.getQueryURL(config.getRouterUrl()),
            new SqlQuery(QUERY, null, false, false, false, ImmutableMap.of(BaseQuery.SQL_QUERY_ID, "validId"), null)
        );

    // Wait until the sqlLifecycle is authorized and registered
    Thread.sleep(1000);
    final HttpResponseStatus responseStatus = sqlClient.cancelQuery(
        sqlHelper.getCancelUrl(config.getRouterUrl(), "invalidId"),
        1000
    );
    if (!responseStatus.equals(HttpResponseStatus.NOT_FOUND)) {
      throw new RE("Expected http response [%s], actual response [%s]", HttpResponseStatus.NOT_FOUND, responseStatus);
    }

    final StatusResponseHolder queryResponse = queryResponseFuture.get(30, TimeUnit.SECONDS);
    if (!queryResponse.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Cancel request failed with status[%s] and content[%s]",
          queryResponse.getStatus(),
          queryResponse.getContent()
      );
    }
  }
}
