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


import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test for system table queries.
 * In this test we're using deterministic table names to avoid flaky behavior of the test.
 */
public class SystemTableQueryTest extends QueryTestBase
{
  private String testDataSourceName;

  @Override
  public void beforeAll()
  {
    testDataSourceName = ingestBasicData();
  }

  @Test
  public void testSystemTableQueries_segmentsCount()
  {
    String query = StringUtils.format(
        "SELECT datasource, count(*) \n"
        + "FROM sys.segments \n"
        + "WHERE datasource='%s' \n"
        + "GROUP BY 1",
        testDataSourceName
    );

    String result = cluster.callApi().runSql(query);
    Assertions.assertEquals(StringUtils.format("%s,10", testDataSourceName), result);
  }

  @Test
  public void testSystemTableQueries_serverTypes()
  {
    String query = "SELECT server_type FROM sys.servers WHERE tier IS NOT NULL AND server_type <> 'indexer'";
    Assertions.assertEquals("historical", cluster.callApi().runSql(query));
  }
}
