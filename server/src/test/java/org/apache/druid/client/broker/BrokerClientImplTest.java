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

package org.apache.druid.client.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.explain.ExplainAttributes;
import org.apache.druid.query.explain.ExplainPlan;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BrokerClientImplTest
{
  private ObjectMapper jsonMapper;
  private MockServiceClient serviceClient;
  private BrokerClient brokerClient;

  @Before
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();
    serviceClient = new MockServiceClient();
    brokerClient = new BrokerClientImpl(serviceClient, jsonMapper);
  }

  @After
  public void tearDown()
  {
    serviceClient.verify();
  }

  @Test
  public void testSubmitSqlTask() throws Exception
  {
    final ClientSqlQuery query = new ClientSqlQuery(
        "REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY ALL",
        null,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        null
    );
    final SqlTaskStatus taskStatus = new SqlTaskStatus("taskId1", TaskState.RUNNING, null);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/v2/sql/task/")
            .jsonContent(jsonMapper, query),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(taskStatus)
    );

    assertEquals(taskStatus, brokerClient.submitSqlTask(query).get());
  }

  @Test
  public void testFetchExplainPlan() throws Exception
  {
    final ClientSqlQuery query = new ClientSqlQuery(
        "REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY ALL",
        null,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        null
    );
    final ClientSqlQuery explainQuery = new ClientSqlQuery(
        StringUtils.format("EXPLAIN PLAN FOR %s", query.getQuery()),
        null,
        false,
        false,
        false,
        null,
        null
    );

    final String plan = "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"inline\",\"data\":\"a,b,1\\nc,d,2\\n\"},\"inputFormat\":{\"type\":\"csv\",\"columns\":[\"x\",\"y\",\"z\"]},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"x\",\"y\",\"z\"],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"dummy\",\"sqlReplaceTimeChunks\":\"all\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\",\"LONG\"],\"granularity\":{\"type\":\"all\"},\"legacy\":false},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}],\"columnMappings\":[{\"queryColumn\":\"x\",\"outputColumn\":\"x\"},{\"queryColumn\":\"y\",\"outputColumn\":\"y\"},{\"queryColumn\":\"z\",\"outputColumn\":\"z\"}]}]";
    final String resources = "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"dst\",\"type\":\"DATASOURCE\"}]";
    final ExplainAttributes attributes = new ExplainAttributes("REPLACE", "foo", Granularities.ALL, null, "all");

    final List<Map<String, Object>> givenPlans = ImmutableList.of(
        ImmutableMap.of(
            "PLAN",
            plan,
            "RESOURCES",
            resources,
            "ATTRIBUTES",
            jsonMapper.writeValueAsString(attributes)
        )
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/v2/sql/task/")
            .jsonContent(jsonMapper, explainQuery),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(givenPlans)
    );

    assertEquals(
        ImmutableList.of(new ExplainPlan(plan, resources, attributes)),
        brokerClient.fetchExplainPlan(query).get()
    );
  }

}
