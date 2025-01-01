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

package org.apache.druid.msq.dart.controller.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.msq.dart.controller.http.GetQueriesResponse;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

public class DartSqlClientImplTest
{
  private ObjectMapper jsonMapper;
  private MockServiceClient serviceClient;
  private DartSqlClient dartSqlClient;

  @BeforeEach
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();
    serviceClient = new MockServiceClient();
    dartSqlClient = new DartSqlClientImpl(serviceClient, jsonMapper);
  }

  @AfterEach
  public void tearDown()
  {
    serviceClient.verify();
  }

  @Test
  public void test_getMessages_all() throws Exception
  {
    final GetQueriesResponse getQueriesResponse = new GetQueriesResponse(
        ImmutableList.of(
            new DartQueryInfo(
                "sid",
                "did",
                "SELECT 1",
                "localhost:1001",
                "",
                "",
                DateTimes.of("2000"),
                ControllerHolder.State.RUNNING.toString()
            )
        )
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(getQueriesResponse)
    );

    final ListenableFuture<GetQueriesResponse> result = dartSqlClient.getRunningQueries(false);
    Assertions.assertEquals(getQueriesResponse, result.get());
  }

  @Test
  public void test_getMessages_selfOnly() throws Exception
  {
    final GetQueriesResponse getQueriesResponse = new GetQueriesResponse(
        ImmutableList.of(
            new DartQueryInfo(
                "sid",
                "did",
                "SELECT 1",
                "localhost:1001",
                "",
                "",
                DateTimes.of("2000"),
                ControllerHolder.State.RUNNING.toString()
            )
        )
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/?selfOnly"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(getQueriesResponse)
    );

    final ListenableFuture<GetQueriesResponse> result = dartSqlClient.getRunningQueries(true);
    Assertions.assertEquals(getQueriesResponse, result.get());
  }
}
