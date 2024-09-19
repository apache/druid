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

package org.apache.druid.grpc;

import com.google.common.collect.ImmutableMap;
import io.grpc.StatusRuntimeException;
import org.apache.druid.grpc.client.GrpcResponseHandler;
import org.apache.druid.grpc.proto.HealthOuterClass.HealthCheckRequest;
import org.apache.druid.grpc.proto.HealthOuterClass.HealthCheckResponse;
import org.apache.druid.grpc.proto.QueryOuterClass;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.grpc.proto.TestResults;
import org.apache.druid.grpc.proto.TestResults.QueryResult;
import org.apache.druid.grpc.server.GrpcQueryConfig;
import org.apache.druid.grpc.server.QueryDriver;
import org.apache.druid.grpc.server.QueryServer;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Simple test that runs the gRPC server, on top of a test SQL stack.
 * Uses a simple client to send a query to the server. This is a basic
 * sanity check of the gRPC stack. Uses allow-all security, which
 * does a sanity check of the auth chain.
 */
public class GrpcQueryTest extends BaseCalciteQueryTest
{
  private static QueryServer server;
  private static TestClient client;

  @BeforeEach
  public void setup() throws IOException
  {
    SqlTestFramework sqlTestFramework = queryFramework();
    SqlTestFramework.PlannerFixture plannerFixture = sqlTestFramework.plannerFixture(
        BaseCalciteQueryTest.PLANNER_CONFIG_DEFAULT,
        new AuthConfig()
    );

    QueryDriver driver = new QueryDriver(
        sqlTestFramework.queryJsonMapper(),
        plannerFixture.statementFactory(),
        sqlTestFramework.queryLifecycleFactory()
    );
    AuthenticatorMapper authMapper = new AuthenticatorMapper(
        ImmutableMap.of(
            "test",
            new AllowAllAuthenticator()
        )
    );
    GrpcQueryConfig config = new GrpcQueryConfig(50051);
    server = new QueryServer(config, driver, authMapper);
    try {
      server.start();
    }
    catch (IOException | RuntimeException e) {
      e.printStackTrace();
      throw e;
    }
    client = new TestClient(TestClient.DEFAULT_HOST);
  }

  @AfterEach
  public void tearDown() throws InterruptedException
  {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.stop();
      server.blockUntilShutdown();
    }
  }

  /**
   * Do a very basic query.
   */
  @Test
  public void testBasics_sql()
  {
    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery("SELECT * FROM foo")
                                       .setResultFormat(QueryResultFormat.CSV)
                                       .setQueryType(QueryOuterClass.QueryType.SQL)
                                       .build();

    QueryResponse response = client.getQueryClient().submitQuery(request);
    assertEquals(QueryStatus.OK, response.getStatus());
  }

  /**
   * Do a very basic query that outputs protobuf.
   */
  @Test
  public void testGrpcBasics_sql()
  {
    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery("SELECT dim1, dim2, dim3, cnt, m1, m2, unique_dim1, __time AS \"date\" FROM foo")
                                       .setQueryType(QueryOuterClass.QueryType.SQL)
                                       .setProtobufMessageName(QueryResult.class.getName())
                                       .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
                                       .build();

    QueryResponse response = client.getQueryClient().submitQuery(request);
    GrpcResponseHandler<QueryResult> handler = GrpcResponseHandler.of(QueryResult.class);
    List<QueryResult> queryResults = handler.get(response.getData());
    assertEquals(QueryStatus.OK, response.getStatus());
    assertEquals(6, queryResults.size());
  }

  @Test
  public void testGrpcBasics_native_timeseries()
  {
    String query = "{\n"
                   + "  \"queryType\": \"timeseries\",\n"
                   + "  \"dataSource\": \"foo\",\n"
                   + "  \"granularity\": \"day\",\n"
                   + "  \"descending\": \"true\",\n"
                   + "  \"aggregations\": [\n"
                   + "    {\n"
                   + "      \"type\": \"longSum\",\n"
                   + "      \"name\": \"timeseries\",\n"
                   + "      \"fieldName\": \"m2\"\n"
                   + "    }\n"
                   + "  ],\n"
                   + "  \"intervals\": [\n"
                   + "    \"2000-01-01T00:00:00.000/2000-01-05T00:00:00.000\"\n"
                   + "  ]\n"
                   + "}";

    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery(query)
                                       .setQueryType(QueryOuterClass.QueryType.NATIVE)
                                       .addTimeColumns("__time")
                                       .setProtobufMessageName(TestResults.NativeQueryResultTimeSeries.class.getName())
                                       .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
                                       .build();

    QueryResponse response = client.getQueryClient().submitQuery(request);
    GrpcResponseHandler<TestResults.NativeQueryResultTimeSeries> handler = GrpcResponseHandler.of(TestResults.NativeQueryResultTimeSeries.class);
    List<TestResults.NativeQueryResultTimeSeries> queryResults = handler.get(response.getData());
    assertEquals(QueryStatus.OK, response.getStatus());
    assertEquals(4, queryResults.size());

    QueryRequest requestSkipTime = QueryRequest.newBuilder()
                                               .setQuery(query)
                                               .setQueryType(QueryOuterClass.QueryType.NATIVE)
                                               .addSkipColumns("__time")
                                               .setProtobufMessageName(TestResults.NativeQueryResultTimeSeriesSkipTime.class.getName())
                                               .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
                                               .build();

    QueryResponse responseSkipTime = client.getQueryClient().submitQuery(requestSkipTime);
    GrpcResponseHandler<TestResults.NativeQueryResultTimeSeriesSkipTime> handlerSkipTime = GrpcResponseHandler.of(TestResults.NativeQueryResultTimeSeriesSkipTime.class);
    List<TestResults.NativeQueryResultTimeSeriesSkipTime> queryResultsSkipTime = handlerSkipTime.get(responseSkipTime.getData());
    assertEquals(QueryStatus.OK, response.getStatus());
    assertEquals(4, queryResultsSkipTime.size());
  }

  @Test
  public void testGrpcBasics_native_groupby_day_granularity()
  {
    String query = "{\n"
                   + "  \"queryType\": \"groupBy\",\n"
                   + "  \"dataSource\": \"wikipedia\",\n"
                   + "  \"dimensions\": [\n"
                   + "    {\n"
                   + "      \"type\": \"default\",\n"
                   + "      \"dimension\": \"countryName\",\n"
                   + "      \"outputType\": \"STRING\"\n"
                   + "    }\n"
                   + "  ],\n"
                   + "  \"aggregations\": [\n"
                   + "    {\n"
                   + "      \"type\": \"longSum\",\n"
                   + "      \"name\": \"aggregate\",\n"
                   + "      \"fieldName\": \"added\"\n"
                   + "    }\n"
                   + "  ],\n"
                   + "  \"filter\": {\n"
                   + "    \"type\": \"in\",\n"
                   + "    \"dimension\": \"countryName\",\n"
                   + "    \"values\": [\n"
                   + "      \"Albania\",\n"
                   + "      \"Angola\"\n"
                   + "    ]\n"
                   + "  },"
                   + "  \"intervals\": [\n"
                   + "    \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                   + "  ],\n"
                   + "  \"granularity\": \"day\"\n"
                   + "}";

    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery(query)
                                       .setQueryType(QueryOuterClass.QueryType.NATIVE)
                                       .putContext("timeFieldKey", "date")
                                       .addTimeColumns("__time")
                                       .setProtobufMessageName(TestResults.NativeQueryResultGroupby.class.getName())
                                       .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
                                       .build();

    QueryResponse response = client.getQueryClient().submitQuery(request);
    GrpcResponseHandler<TestResults.NativeQueryResultGroupby> handler = GrpcResponseHandler.of(TestResults.NativeQueryResultGroupby.class);
    List<TestResults.NativeQueryResultGroupby> queryResults = handler.get(response.getData());
    assertEquals(QueryStatus.OK, response.getStatus());
    assertEquals(2, queryResults.size());

  }

  @Test
  public void testGrpcBasics_native_groupby()
  {
    String query = "{\n"
                   + "  \"queryType\": \"groupBy\",\n"
                   + "  \"dataSource\": \"wikipedia\",\n"
                   + "  \"dimensions\": [\n"
                   + "    {\n"
                   + "      \"type\": \"default\",\n"
                   + "      \"dimension\": \"countryName\",\n"
                   + "      \"outputType\": \"STRING\"\n"
                   + "    }\n"
                   + "  ],\n"
                   + "  \"aggregations\": [\n"
                   + "    {\n"
                   + "      \"type\": \"longSum\",\n"
                   + "      \"name\": \"aggregate\",\n"
                   + "      \"fieldName\": \"added\"\n"
                   + "    }\n"
                   + "  ],\n"
                   + "  \"filter\": {\n"
                   + "    \"type\": \"in\",\n"
                   + "    \"dimension\": \"countryName\",\n"
                   + "    \"values\": [\n"
                   + "      \"Albania\",\n"
                   + "      \"Angola\"\n"
                   + "    ]\n"
                   + "  },"
                   + "  \"intervals\": [\n"
                   + "    \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                   + "  ],\n"
                   + "  \"granularity\": \"all\"\n"
                   + "}";

    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery(query)
                                       .setQueryType(QueryOuterClass.QueryType.NATIVE)
                                       .setProtobufMessageName(TestResults.NativeQueryResultGroupbyWithoutTime.class.getName())
                                       .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
                                       .build();

    QueryResponse response = client.getQueryClient().submitQuery(request);
    GrpcResponseHandler<TestResults.NativeQueryResultGroupbyWithoutTime> handler = GrpcResponseHandler.of(TestResults.NativeQueryResultGroupbyWithoutTime.class);
    List<TestResults.NativeQueryResultGroupbyWithoutTime> queryResults = handler.get(response.getData());
    assertEquals(QueryStatus.OK, response.getStatus());
    assertEquals(2, queryResults.size());
  }

  @Test
  public void testGrpcBasics_native_groupby_timeaggregate()
  {
    String query = "{\n"
                   + "  \"queryType\": \"groupBy\",\n"
                   + "  \"dataSource\": \"wikipedia\",\n"
                   + "  \"dimensions\": [\n"
                   + "    {\n"
                   + "      \"type\": \"default\",\n"
                   + "      \"dimension\": \"countryName\",\n"
                   + "      \"outputType\": \"STRING\"\n"
                   + "    },\n"
                   + "    {\n"
                   + "    \"type\":\"default\",\n"
                   + "      \"dimension\": \"__time\",\n"
                   + "      \"outputName\": \"timeCol\",\n"
                   + "      \"outputType\": \"LONG\"\n"
                   + "    }"
                   + "  ],\n"
                   + "  \"aggregations\": [\n"
                   + "    {\n"
                   + "      \"type\": \"longSum\",\n"
                   + "      \"name\": \"aggregate\",\n"
                   + "      \"fieldName\": \"added\"\n"
                   + "    }\n"
                   + "  ],\n"
                   + "  \"filter\": {\n"
                   + "    \"type\": \"in\",\n"
                   + "    \"dimension\": \"countryName\",\n"
                   + "    \"values\": [\n"
                   + "      \"Albania\",\n"
                   + "      \"Angola\"\n"
                   + "    ]\n"
                   + "  },"
                   + "  \"intervals\": [\n"
                   + "    \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"\n"
                   + "  ],\n"
                   + "  \"granularity\": \"day\"\n"
                   + "}";

    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery(query)
                                       .setQueryType(QueryOuterClass.QueryType.NATIVE)
                                       .addSkipColumns("__time")
                                       .addTimeColumns("timeCol")
                                       .setProtobufMessageName(TestResults.NativeQueryResultGroupbyTimeRenamed.class.getName())
                                       .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
                                       .build();

    QueryResponse response = client.getQueryClient().submitQuery(request);
    GrpcResponseHandler<TestResults.NativeQueryResultGroupbyTimeRenamed> handler = GrpcResponseHandler.of(TestResults.NativeQueryResultGroupbyTimeRenamed.class);
    List<TestResults.NativeQueryResultGroupbyTimeRenamed> queryResults = handler.get(response.getData());
    assertEquals(QueryStatus.OK, response.getStatus());
    assertEquals(6, queryResults.size());
  }

  @Test
  public void testGrpcEmptyResponse_sql()
  {
    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery("SELECT dim1, dim2, dim3, cnt, m1, m2, unique_dim1, __time AS \"date\" FROM foo where cnt = 100000")
                                       .setProtobufMessageName(QueryResult.class.getName())
                                       .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
                                       .setQueryType(QueryOuterClass.QueryType.SQL)
                                       .build();

    QueryResponse response = client.getQueryClient().submitQuery(request);
    GrpcResponseHandler<QueryResult> handler = GrpcResponseHandler.of(QueryResult.class);
    List<QueryResult> queryResults = handler.get(response.getData());
    assertEquals(QueryStatus.OK, response.getStatus());
    assertEquals(0, queryResults.size());
  }

  @Test
  public void test_health_check()
  {
    HealthCheckRequest healthCheckRequest = HealthCheckRequest
        .newBuilder()
        .setService("QueryService")
        .build();

    HealthCheckResponse healthCheckResponse = client
        .getHealthCheckClient()
        .check(healthCheckRequest);

    assertEquals(HealthCheckResponse.ServingStatus.SERVING, healthCheckResponse.getStatus());

    healthCheckRequest = HealthCheckRequest
        .newBuilder()
        .setService("")
        .build();

    healthCheckResponse = client
        .getHealthCheckClient()
        .check(healthCheckRequest);

    assertEquals(HealthCheckResponse.ServingStatus.SERVING, healthCheckResponse.getStatus());
  }

  @Test
  public void test_health_watch()
  {
    HealthCheckRequest healthCheckRequest = HealthCheckRequest
        .newBuilder()
        .setService("QueryService")
        .build();

    Iterator<HealthCheckResponse> streamingHealthCheckResponse = client
        .getHealthCheckClient()
        .watch(healthCheckRequest);

    assertTrue(streamingHealthCheckResponse.hasNext());
    assertEquals(HealthCheckResponse.ServingStatus.SERVING, streamingHealthCheckResponse.next().getStatus());

    Executors.newSingleThreadExecutor().submit(() -> {
      Iterator<HealthCheckResponse> secondRequest = client
          .getHealthCheckClient()
          .watch(healthCheckRequest);

      assertThrows(StatusRuntimeException.class, secondRequest::hasNext);
    });

    // stop the service from another thread
    Executors.newSingleThreadExecutor().submit(() -> {
      try {
        Thread.sleep(10_000);
        server.stop();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    // hasNext call would block until the status has changed
    // as soon as the server is stopped the status would change to NOT_SERVING
    if (streamingHealthCheckResponse.hasNext()) {
      assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING, streamingHealthCheckResponse.next().getStatus());
    }
  }
}
