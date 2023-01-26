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

import org.apache.druid.grpc.proto.QueryOuterClass.ColumnSchema;
import org.apache.druid.grpc.proto.QueryOuterClass.DruidType;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.grpc.server.QueryDriver;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DriverTest
{
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static QueryFrameworkFixture frameworkFixture;
  private static QueryDriver driver;

  @BeforeClass
  public static void setup() throws IOException
  {
    frameworkFixture = new QueryFrameworkFixture(temporaryFolder.newFolder());
    driver = new QueryDriver(
        frameworkFixture.jsonMapper(),
        frameworkFixture.statementFactory()
    );
  }

  @Test
  public void testBasics()
  {
    String sql = "SELECT __time, dim2 FROM foo";
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(sql)
        .setResultFormat(QueryResultFormat.CSV)
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
}
