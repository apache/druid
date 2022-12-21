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

package org.apache.druid.testsEx.query;

import static org.apache.druid.testsEx.utils.RegexMatchUtil.matchesRegex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.coordination.ServerManagerForQueryErrorTest;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.testsEx.indexer.AbstractIndexerTest;
import org.apache.druid.testsEx.categories.Query;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * This class tests various query failures.
 *
 * - SQL planning failures. Both {@link org.apache.calcite.sql.parser.SqlParseException}
 *   and {@link org.apache.calcite.tools.ValidationException} are tested using SQLs that must fail.
 * - Various query errors from historicals. These tests use {@link ServerManagerForQueryErrorTest} to make
 *   the query to always throw an exception. They verify the error code returned by
 *   {@link org.apache.druid.sql.http.SqlResource} and {@link org.apache.druid.server.QueryResource}.
 */
@RunWith(DruidTestRunner.class)
@Category(Query.class)
public class ITQueryErrorTest
{
  private static final String WIKIPEDIA_DATA_SOURCE = "wikipedia_editstream";
  /**
   * A simple query used for error tests from historicals. What query is does not matter because the query is always
   * expected to fail.
   *
   * @see ServerManagerForQueryErrorTest#buildQueryRunnerForSegment
   */
  private static final String NATIVE_QUERY_RESOURCE =
      "/queries/native_query_error_from_historicals_test.json";
  private static final String SQL_QUERY_RESOURCE =
      "/queries/sql_error_from_historicals_test.json";
  /**
   * A simple sql query template used for plan failure tests.
   */
  private static final String SQL_PLAN_FAILURE_RESOURCE = "/queries/sql_plan_failure_query.json";

  @Inject
  private DataLoaderHelper dataLoaderHelper;
  @Inject
  private TestQueryHelper queryHelper;
  @Inject
  private SqlTestQueryHelper sqlHelper;
  @Inject
  private ObjectMapper jsonMapper;

  @Before
  public void before()
  {
    // ensure that wikipedia segments are loaded completely
    dataLoaderHelper.waitUntilDatasourceIsReady(WIKIPEDIA_DATA_SOURCE);
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testSqlParseException() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*400.*"));
    // test a sql without SELECT
    sqlHelper.testQueriesFromString(buildSqlPlanFailureQuery("FROM t WHERE col = 'a'"));
  }

  @Test
  public void testSqlValidationException() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*400.*"));
    // test a sql that selects unknown column
    sqlHelper.testQueriesFromString(
        buildSqlPlanFailureQuery(StringUtils.format("SELECT unknown_col FROM %s LIMIT 1", WIKIPEDIA_DATA_SOURCE))
    );
  }

  @Test
  public void testSqlTimeout() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*504.*"));
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.QUERY_TIMEOUT_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testSqlCapacityExceeded() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*429.*"));
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testSqlUnsupported() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*501.*"));
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.QUERY_UNSUPPORTED_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testSqlResourceLimitExceeded() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*400.*"));
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testSqlFailure() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*500.*"));
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.QUERY_FAILURE_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testQueryTimeout() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*504.*"));
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.QUERY_TIMEOUT_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testQueryCapacityExceeded() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*429.*"));
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testQueryUnsupported() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*501.*"));
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.QUERY_UNSUPPORTED_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testResourceLimitExceeded() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*400.*"));
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY)
    );
  }

  @Test
  public void testQueryFailure() throws Exception
  {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(matchesRegex("(?s).*500.*"));
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.QUERY_FAILURE_TEST_CONTEXT_KEY)
    );
  }

  private String buildSqlPlanFailureQuery(String sql) throws IOException
  {
    return StringUtils.replace(
        AbstractIndexerTest.getResourceAsString(SQL_PLAN_FAILURE_RESOURCE),
        "%%QUERY%%",
        sql
    );
  }

  private String buildHistoricalErrorSqlQuery(String contextKey) throws IOException
  {
    return StringUtils.replace(
        AbstractIndexerTest.getResourceAsString(SQL_QUERY_RESOURCE),
        "%%CONTEXT%%",
        jsonMapper.writeValueAsString(buildTestContext(contextKey))
    );
  }

  private String buildHistoricalErrorTestQuery(String contextKey) throws IOException
  {
    return StringUtils.replace(
        AbstractIndexerTest.getResourceAsString(NATIVE_QUERY_RESOURCE),
        "%%CONTEXT%%",
        jsonMapper.writeValueAsString(buildTestContext(contextKey))
    );
  }

  private static Map<String, Object> buildTestContext(String key)
  {
    final Map<String, Object> context = new HashMap<>();
    // Disable cache so that each run hits historical.
    context.put(QueryContexts.USE_CACHE_KEY, false);
    context.put(key, true);
    return context;
  }
}
