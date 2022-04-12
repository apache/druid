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

package org.apache.druid.tests.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.coordination.ServerManagerForQueryErrorTest;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class tests various query failures.
 *
 * - SQL planning failures. Both {@link org.apache.calcite.sql.parser.SqlParseException}
 *   and {@link org.apache.calcite.tools.ValidationException} are tested using SQLs that must fail.
 * - Various query errors from historicals. These tests use {@link ServerManagerForQueryErrorTest} to make
 *   the query to always throw an exception. They verify the error code returned by
 *   {@link org.apache.druid.sql.http.SqlResource} and {@link org.apache.druid.server.QueryResource}.
 */
@Test(groups = TestNGGroup.QUERY_ERROR)
@Guice(moduleFactory = DruidTestModuleFactory.class)
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

  @BeforeMethod
  public void before()
  {
    // ensure that wikipedia segments are loaded completely
    dataLoaderHelper.waitUntilDatasourceIsReady(WIKIPEDIA_DATA_SOURCE);
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*400.*")
  public void testSqlParseException() throws Exception
  {
    // test a sql without SELECT
    sqlHelper.testQueriesFromString(buildSqlPlanFailureQuery("FROM t WHERE col = 'a'"));
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*400.*")
  public void testSqlValidationException() throws Exception
  {
    // test a sql that selects unknown column
    sqlHelper.testQueriesFromString(
        buildSqlPlanFailureQuery(StringUtils.format("SELECT unknown_col FROM %s LIMIT 1", WIKIPEDIA_DATA_SOURCE))
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*504.*")
  public void testSqlTimeout() throws Exception
  {
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.QUERY_TIMEOUT_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*429.*")
  public void testSqlCapacityExceeded() throws Exception
  {
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*501.*")
  public void testSqlUnsupported() throws Exception
  {
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.QUERY_UNSUPPORTED_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*400.*")
  public void testSqlResourceLimitExceeded() throws Exception
  {
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*500.*")
  public void testSqlFailure() throws Exception
  {
    sqlHelper.testQueriesFromString(
        buildHistoricalErrorSqlQuery(ServerManagerForQueryErrorTest.QUERY_FAILURE_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*504.*")
  public void testQueryTimeout() throws Exception
  {
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.QUERY_TIMEOUT_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*429.*")
  public void testQueryCapacityExceeded() throws Exception
  {
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*501.*")
  public void testQueryUnsupported() throws Exception
  {
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.QUERY_UNSUPPORTED_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*400.*")
  public void testResourceLimitExceeded() throws Exception
  {
    queryHelper.testQueriesFromString(
        buildHistoricalErrorTestQuery(ServerManagerForQueryErrorTest.RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY)
    );
  }

  @Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "(?s).*500.*")
  public void testQueryFailure() throws Exception
  {
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
