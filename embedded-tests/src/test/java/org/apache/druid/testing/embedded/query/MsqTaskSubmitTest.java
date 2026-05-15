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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Verifies that the Router correctly counts 202 Accepted responses from MSQ task
 * submissions ({@code POST /druid/v2/sql/task}) as successful queries in the
 * {@code query/time} metric, rather than counting them as failures because the
 * status code is not exactly 200.
 */
public class MsqTaskSubmitTest extends QueryTestBase
{
  private String dataSourceName;

  @Override
  public void beforeAll()
  {
    jsonMapper = overlord.bindings().jsonMapper();
    dataSourceName = ingestBasicData();
  }

  @Test
  public void test_msqTaskSubmissionThroughRouter_reportedAsSuccess() throws Exception
  {
    final String routerTaskEndpoint = StringUtils.format("%s/druid/v2/sql/task", getServerUrl(router));
    final String destDatasource = EmbeddedClusterApis.createTestDatasourceName();

    final String insertQuery = StringUtils.format(
        "INSERT INTO \"%s\" SELECT * FROM \"%s\" PARTITIONED BY ALL",
        destDatasource,
        dataSourceName
    );

    final ClientSqlQuery query = new ClientSqlQuery(
        insertQuery,
        null,
        false,
        false,
        false,
        Collections.emptyMap(),
        List.of()
    );

    ListenableFuture<StatusResponseHolder> future = executeQueryAsync(routerTaskEndpoint, query);
    StatusResponseHolder response = future.get();

    Assertions.assertEquals(
        HttpResponseStatus.ACCEPTED.getCode(),
        response.getStatus().getCode(),
        "MSQ task submission should return 202 Accepted"
    );

    ServiceMetricEvent event = router.latchableEmitter().waitForEvent(
        matcher -> matcher.hasMetricName("query/time")
                          .hasDimension("success", "true")
    );

    Assertions.assertEquals(
        202,
        event.getUserDims().get("statusCode"),
        "statusCode dimension should be 202 for MSQ task submissions"
    );
  }
}
