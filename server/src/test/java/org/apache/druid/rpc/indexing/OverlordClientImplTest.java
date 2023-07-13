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

package org.apache.druid.rpc.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.ClientKillUnusedSegmentsTaskQuery;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class OverlordClientImplTest
{
  private ObjectMapper jsonMapper;
  private MockServiceClient serviceClient;
  private OverlordClient overlordClient;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();
    serviceClient = new MockServiceClient();
    overlordClient = new OverlordClientImpl(serviceClient, jsonMapper);
  }

  @After
  public void tearDown()
  {
    serviceClient.verify();
  }

  @Test
  public void test_taskReportAsMap() throws Exception
  {
    final String taskId = "testTaskId";
    final Map<String, Object> response = ImmutableMap.of("test", "value");

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/task/testTaskId/reports"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(response)
    );

    final ListenableFuture<Map<String, Object>> future = overlordClient.taskReportAsMap(taskId);
    Assert.assertEquals(response, future.get());
  }

  @Test
  public void test_taskReportAsMap_notFound()
  {
    String taskId = "testTaskId";
    String errorMsg = "No task reports were found for this task. "
                      + "The task may not exist, or it may not have completed yet.";

    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/task/testTaskId/reports"),
        new HttpResponseException(
            new StringFullResponseHolder(
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND),
                StandardCharsets.UTF_8
            ).addChunk(errorMsg)
        )
    );

    final ListenableFuture<Map<String, Object>> future = overlordClient.taskReportAsMap(taskId);

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        future::get
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(HttpResponseException.class));
    Assert.assertEquals(
        HttpResponseStatus.NOT_FOUND.getCode(),
        ((HttpResponseException) e.getCause()).getResponse().getStatus().getCode()
    );
  }

  @Test
  public void testGetTaskReportEmpty()
  {
    final String taskID = "testTaskId";

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/task/" + taskID + "/reports"),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        StringUtils.toUtf8("{}")
    );

    final Map<String, Object> actualResponse =
        FutureUtils.getUnchecked(overlordClient.taskReportAsMap(taskID), true);
    Assert.assertEquals(Collections.emptyMap(), actualResponse);
  }

  @Test
  public void testGetTotalWorkerCapacityWithAutoScale() throws Exception
  {
    int currentClusterCapacity = 5;
    int maximumCapacityWithAutoScale = 10;

    // Mock response for /druid/indexer/v1/totalWorkerCapacity
    final IndexingTotalWorkerCapacityInfo indexingTotalWorkerCapacityInfo = new IndexingTotalWorkerCapacityInfo(
        currentClusterCapacity,
        maximumCapacityWithAutoScale
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/totalWorkerCapacity"),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(indexingTotalWorkerCapacityInfo)
    );

    final int actualResponse =
        FutureUtils.getUnchecked(overlordClient.getTotalWorkerCapacityWithAutoScale(), true);
    Assert.assertEquals(maximumCapacityWithAutoScale, actualResponse);
  }

  @Test
  public void testTaskPayload() throws ExecutionException, InterruptedException, JsonProcessingException
  {
    final String taskID = "taskId_1";
    final ClientTaskQuery clientTaskQuery = new ClientKillUnusedSegmentsTaskQuery(taskID, "test", null, null);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/task/" + taskID),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        DefaultObjectMapper.INSTANCE.writeValueAsBytes(new TaskPayloadResponse(taskID, clientTaskQuery))
    );

    Assert.assertEquals(
        clientTaskQuery,
        overlordClient.taskPayload(taskID).get().getPayload()
    );
  }
}
