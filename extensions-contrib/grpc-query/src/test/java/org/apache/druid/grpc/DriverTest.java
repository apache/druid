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

import org.apache.druid.grpc.proto.QueryOuterClass;
import org.apache.druid.grpc.proto.QueryOuterClass.ColumnSchema;
import org.apache.druid.grpc.proto.QueryOuterClass.DruidType;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.grpc.server.QueryDriver;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DriverTest extends BaseCalciteQueryTest
{
  private QueryDriver driver;

  @BeforeEach
  public void setup()
  {
    SqlTestFramework sqlTestFramework = queryFramework();
    SqlTestFramework.PlannerFixture plannerFixture = sqlTestFramework.plannerFixture(
        BaseCalciteQueryTest.PLANNER_CONFIG_DEFAULT,
        new AuthConfig()
    );
    driver = new QueryDriver(
        sqlTestFramework.queryJsonMapper(),
        plannerFixture.statementFactory(),
        sqlTestFramework.queryLifecycleFactory()
    );
  }

  @Test
  public void testBasics_sql()
  {
    String sql = "SELECT __time, dim2 FROM foo";
    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery(sql)
                                       .setResultFormat(QueryResultFormat.CSV)
                                       .setQueryType(QueryOuterClass.QueryType.SQL)
                                       .build();
    QueryResponse response = driver.submitQuery(request, CalciteTests.REGULAR_USER_AUTH_RESULT);

    assertEquals(QueryStatus.OK, response.getStatus());
    assertFalse(response.hasErrorMessage());
    assertTrue(response.getQueryId().length() > 5);
    List<ColumnSchema> columns = response.getColumnsList();
    assertEquals(2, columns.size());
    ColumnSchema col = columns.get(0);
    assertEquals("__time", col.getName());
    assertEquals("TIMESTAMP", col.getSqlType());
    assertEquals(DruidType.LONG, col.getDruidType());
    col = columns.get(1);
    assertEquals("dim2", col.getName());
    assertEquals("VARCHAR", col.getSqlType());
    assertEquals(DruidType.STRING, col.getDruidType());

    List<String> expectedResults = Arrays.asList(
        "2000-01-01T00:00:00.000Z,a",
        "2000-01-02T00:00:00.000Z,",
        "2000-01-03T00:00:00.000Z,",
        "2001-01-01T00:00:00.000Z,a",
        "2001-01-02T00:00:00.000Z,abc",
        "2001-01-03T00:00:00.000Z,"
    );
    String results = response.getData().toStringUtf8();
    assertEquals(expectedResults, Arrays.asList(results.split("\n")));
  }

  @Test
  public void testBasics_native_groupby()
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
                   + "      \"name\": \"sum\",\n"
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
                                       .setResultFormat(QueryResultFormat.CSV)
                                       .addTimeColumns("__time")
                                       .setQueryType(QueryOuterClass.QueryType.NATIVE)
                                       .build();
    QueryResponse response = driver.submitQuery(request, CalciteTests.REGULAR_USER_AUTH_RESULT);

    assertEquals(QueryStatus.OK, response.getStatus());
    assertFalse(response.hasErrorMessage());
    assertTrue(response.getQueryId().length() > 5);
    List<ColumnSchema> columns = response.getColumnsList();
    assertEquals(3, columns.size());
    ColumnSchema col = columns.get(0);
    assertEquals("__time", col.getName());
    assertEquals("", col.getSqlType());
    assertEquals(DruidType.LONG, col.getDruidType());
    col = columns.get(1);
    assertEquals("countryName", col.getName());
    assertEquals("", col.getSqlType());
    assertEquals(DruidType.STRING, col.getDruidType());

    col = columns.get(2);
    assertEquals("sum", col.getName());
    assertEquals("", col.getSqlType());
    assertEquals(DruidType.LONG, col.getDruidType());

    List<String> expectedResults = Arrays.asList(
        "2015-09-12T00:00:00.000Z,Albania,80",
        "2015-09-12T00:00:00.000Z,Angola,784"
    );
    String results = response.getData().toStringUtf8();
    assertEquals(expectedResults, Arrays.asList(results.split("\n")));
  }

  @Test
  public void testBasics_native_timeseries()
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
                   + "    \"2000-01-01T00:00:00.000/2000-01-04T00:00:00.000\"\n"
                   + "  ]\n"
                   + "}";

    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery(query)
                                       .setResultFormat(QueryResultFormat.CSV)
                                       .addTimeColumns("__time")
                                       .setQueryType(QueryOuterClass.QueryType.NATIVE)
                                       .build();
    QueryResponse response = driver.submitQuery(request, CalciteTests.REGULAR_USER_AUTH_RESULT);

    assertEquals(QueryStatus.OK, response.getStatus());

    assertEquals(QueryStatus.OK, response.getStatus());
    assertFalse(response.hasErrorMessage());
    assertTrue(response.getQueryId().length() > 5);
    List<ColumnSchema> columns = response.getColumnsList();
    assertEquals(2, columns.size());
    ColumnSchema col = columns.get(0);
    assertEquals("__time", col.getName());
    assertEquals("", col.getSqlType());
    assertEquals(DruidType.LONG, col.getDruidType());
    col = columns.get(1);
    assertEquals("timeseries", col.getName());
    assertEquals("", col.getSqlType());
    assertEquals(DruidType.LONG, col.getDruidType());

    List<String> expectedResults = Arrays.asList(
        "2000-01-03T00:00:00.000Z,3",
        "2000-01-02T00:00:00.000Z,2",
        "2000-01-01T00:00:00.000Z,1"
    );
    String results = response.getData().toStringUtf8();
    assertEquals(expectedResults, Arrays.asList(results.split("\n")));
  }
}
