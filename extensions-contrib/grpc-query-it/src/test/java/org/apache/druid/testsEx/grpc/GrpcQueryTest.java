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

package org.apache.druid.testsEx.grpc;

import com.google.protobuf.Timestamp;
import org.apache.druid.grpc.TestClient;
import org.apache.druid.grpc.client.GrpcResponseHandler;
import org.apache.druid.grpc.proto.AllTypes.AllTypesQueryResult;
import org.apache.druid.grpc.proto.QueryOuterClass.ColumnSchema;
import org.apache.druid.grpc.proto.QueryOuterClass.DruidType;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryParameter;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.testsEx.categories.GrpcQuery;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.codehaus.plexus.util.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for the rRPC query extension. To run in an IDE,
 * Launch your cluster as:<pre><code>
 * ./it.sh build
 * ./it.sh image
 * ./it.sh up GrpcQuery extensions-contrib/grpc-query-it
 * </code></pre>
 * Run the test in your IDE. Then:
 * <pre><code>
 * ./it.sh down GrpcQuery extensions-contrib/grpc-query-it
 * </code></pre>
 */
@RunWith(DruidTestRunner.class)
@Category(GrpcQuery.class)
public class GrpcQueryTest
{
  private static final String SQL_HEAD =
      "SELECT\n" +
      "  TIME_PARSE('2023-02-28T12:34:45') AS time_value,\n" +
      "  CAST(ORDINAL_POSITION AS BIGINT) AS long_value,\n" +
      "  CAST(12.5 AS FLOAT) AS float_value,\n" +
      "  CAST(34.5 AS DOUBLE) AS double_value,\n" +
      "  COLUMN_NAME AS string_value\n" +
      "FROM INFORMATION_SCHEMA.COLUMNS\n";
  private static final String SQL_TAIL =
      "ORDER BY long_value\n" +
      "LIMIT 3";
  private static final String SQL =
      SQL_HEAD +
      "WHERE TABLE_NAME = 'COLUMNS'\n" +
      SQL_TAIL;
  private final TestClient client = new TestClient("localhost:50051");

  @Test
  public void testCsv()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(SQL)
        .setResultFormat(QueryResultFormat.CSV)
        .build();
    QueryResponse response = client.client().submitQuery(request);
    verifyResponse(response);
    String expected =
        "2023-02-28T12:34:45.000Z,1,12.5,34.5,TABLE_CATALOG\n" +
        "2023-02-28T12:34:45.000Z,2,12.5,34.5,TABLE_SCHEMA\n" +
        "2023-02-28T12:34:45.000Z,3,12.5,34.5,TABLE_NAME\n";
    assertEquals(expected, response.getData().toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testQueryContext()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(SQL)
        .setResultFormat(QueryResultFormat.CSV)
        .putContext("sqlQueryId", "custom-query-id")
        .build();
    QueryResponse response = client.client().submitQuery(request);
    verifyResponse(response);
    assertEquals("custom-query-id", response.getQueryId());
    String expected =
        "2023-02-28T12:34:45.000Z,1,12.5,34.5,TABLE_CATALOG\n" +
        "2023-02-28T12:34:45.000Z,2,12.5,34.5,TABLE_SCHEMA\n" +
        "2023-02-28T12:34:45.000Z,3,12.5,34.5,TABLE_NAME\n";
    assertEquals(expected, response.getData().toString(StandardCharsets.UTF_8));
  }

  private void verifyResponse(QueryResponse response)
  {
    assertEquals(QueryStatus.OK, response.getStatus());
    assertFalse(response.hasErrorMessage());
    assertTrue(response.getQueryId().length() > 5);
    List<ColumnSchema> columns = response.getColumnsList();
    assertEquals(5, columns.size());
    ColumnSchema col = columns.get(0);
    assertEquals("time_value", col.getName());
    assertEquals("TIMESTAMP", col.getSqlType());
    assertEquals(DruidType.LONG, col.getDruidType());
    col = columns.get(1);
    assertEquals("long_value", col.getName());
    assertEquals("BIGINT", col.getSqlType());
    assertEquals(DruidType.LONG, col.getDruidType());
    col = columns.get(2);
    assertEquals("float_value", col.getName());
    assertEquals("FLOAT", col.getSqlType());
    assertEquals(DruidType.FLOAT, col.getDruidType());
    col = columns.get(3);
    assertEquals("double_value", col.getName());
    assertEquals("DOUBLE", col.getSqlType());
    assertEquals(DruidType.DOUBLE, col.getDruidType());
    col = columns.get(4);
    assertEquals("string_value", col.getName());
    assertEquals("VARCHAR", col.getSqlType());
    assertEquals(DruidType.STRING, col.getDruidType());
  }

  @Test
  public void testBadQuery()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery("SELECT * FROM unknown")
        .setResultFormat(QueryResultFormat.CSV)
        .build();
    QueryResponse response = client.client().submitQuery(request);
    assertEquals(QueryStatus.INVALID_SQL, response.getStatus());
    assertTrue(response.hasErrorMessage());
    assertTrue(response.getQueryId().length() > 5);
    assertFalse(response.hasData());
  }

  @Test
  public void testArrayLines()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(SQL)
        .setResultFormat(QueryResultFormat.JSON_ARRAY_LINES)
        .build();
    QueryResponse response = client.client().submitQuery(request);
    verifyResponse(response);
    String expected =
        "[\"2023-02-28T12:34:45.000Z\",1,12.5,34.5,\"TABLE_CATALOG\"]\n" +
        "[\"2023-02-28T12:34:45.000Z\",2,12.5,34.5,\"TABLE_SCHEMA\"]\n" +
        "[\"2023-02-28T12:34:45.000Z\",3,12.5,34.5,\"TABLE_NAME\"]";
    assertEquals(expected, response.getData().toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testArray()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(SQL)
        .setResultFormat(QueryResultFormat.JSON_ARRAY)
        .build();
    QueryResponse response = client.client().submitQuery(request);
    verifyResponse(response);
    String expected =
        "[[\"2023-02-28T12:34:45.000Z\",1,12.5,34.5,\"TABLE_CATALOG\"]," +
        "[\"2023-02-28T12:34:45.000Z\",2,12.5,34.5,\"TABLE_SCHEMA\"]," +
        "[\"2023-02-28T12:34:45.000Z\",3,12.5,34.5,\"TABLE_NAME\"]]\n";
    assertEquals(expected, response.getData().toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testProtobuf()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(SQL)
        .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
        .setProtobufMessageName(AllTypesQueryResult.class.getName())
        .build();
    QueryResponse response = client.client().submitQuery(request);
    verifyResponse(response);
    GrpcResponseHandler<AllTypesQueryResult> handler = GrpcResponseHandler.of(AllTypesQueryResult.class);
    List<AllTypesQueryResult> queryResults = handler.get(response.getData());
    assertEquals(3, queryResults.size());
    List<AllTypesQueryResult> expected = Arrays.asList(
        AllTypesQueryResult.newBuilder()
            .setTimeValue(Timestamp.newBuilder().setSeconds(1677587685))
            .setLongValue(1)
            .setFloatValue(12.5)
            .setDoubleValue(34.5)
            .setStringValue("TABLE_CATALOG")
            .build(),
        AllTypesQueryResult.newBuilder()
            .setTimeValue(Timestamp.newBuilder().setSeconds(1677587685))
            .setLongValue(2)
            .setFloatValue(12.5)
            .setDoubleValue(34.5)
            .setStringValue("TABLE_SCHEMA")
            .build(),
        AllTypesQueryResult.newBuilder()
            .setTimeValue(Timestamp.newBuilder().setSeconds(1677587685))
            .setLongValue(3)
            .setFloatValue(12.5)
            .setDoubleValue(34.5)
            .setStringValue("TABLE_NAME")
            .build()
      );
    assertEquals(expected, queryResults);
  }

  @Test
  public void testStringParameter()
  {
    String sql = SQL_HEAD +
        "WHERE TABLE_NAME = ?\n" +
        SQL_TAIL;
    QueryParameter value = QueryParameter.newBuilder()
        .setStringValue("COLUMNS")
        .build();
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(sql)
        .setResultFormat(QueryResultFormat.CSV)
        .addParameters(0, value)
        .build();
    QueryResponse response = client.client().submitQuery(request);
    verifyResponse(response);
    String expected =
        "2023-02-28T12:34:45.000Z,1,12.5,34.5,TABLE_CATALOG\n" +
        "2023-02-28T12:34:45.000Z,2,12.5,34.5,TABLE_SCHEMA\n" +
        "2023-02-28T12:34:45.000Z,3,12.5,34.5,TABLE_NAME\n";
    assertEquals(expected, response.getData().toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testUnknownProtobuf()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(SQL)
        .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
        .setProtobufMessageName("com.unknown.BogusClass")
        .build();
    QueryResponse response = client.client().submitQuery(request);
    assertEquals(QueryStatus.REQUEST_ERROR, response.getStatus());
    assertTrue(response.hasErrorMessage());
    assertTrue(response.getErrorMessage().startsWith("The Protobuf class [com.unknown.BogusClass] is not known."));
    assertTrue(response.getQueryId().length() > 5);
    assertFalse(response.hasData());
  }

  @Test
  public void testUnknownField()
  {
    String sql = StringUtils.replace(SQL, "string_value", "bogus_value");
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(sql)
        .setResultFormat(QueryResultFormat.PROTOBUF_INLINE)
        .setProtobufMessageName(AllTypesQueryResult.class.getName())
        .build();
    QueryResponse response = client.client().submitQuery(request);
    assertEquals(QueryStatus.REQUEST_ERROR, response.getStatus());
    assertTrue(response.hasErrorMessage());
    assertEquals(
        "Field [bogus_value] not found in Protobuf [class org.apache.druid.grpc.proto.AllTypes$AllTypesQueryResult]",
        response.getErrorMessage()
    );
    assertTrue(response.getQueryId().length() > 5);
    assertFalse(response.hasData());
  }
}
