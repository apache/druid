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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.Query;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.ResultsVerifier;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Gathers per-test information for a SQL query test. Information is of
 * three kinds:
 * <ul>
 * <li>Configuration: provided by the {@link QueryTestConfig}
 * implementation for the particular way that tests will run.</li>
 * <li>Inputs: the information passed to the query planner to prepare or execute
 * the query.</li>
 * <li>Expected results: the output, resources, exceptions other outputs from the
 * query.</li>
 * </ul>
 * <p>
 * Fields of this class are accessed by the code which executes the class. There
 * is little harm in skipping the usual suite of "getter" methods in test code.
 */
public class QueryTestBuilder
{
  /**
   * Implement to provide the execution framework that the tests require.
   * The {@link #analyze(QueryTestBuilder)} method builds up the classes that
   * will run the test, since some verification depends on context, such as that
   * provided by {@link BaseCalciteQueryTest}.
   */
  public interface QueryTestConfig
  {
    QueryTestRunner analyze(QueryTestBuilder builder);
    QueryLogHook queryLogHook();
    ExpectedException expectedException();
    ObjectMapper jsonMapper();
    SqlStatementFactory statementFactory(PlannerConfig plannerConfig, AuthConfig authConfig);
  }

  protected final QueryTestConfig config;
  protected PlannerConfig plannerConfig = BaseCalciteQueryTest.PLANNER_CONFIG_DEFAULT;
  protected Map<String, Object> queryContext = BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT;
  protected List<SqlParameter> parameters = CalciteTestBase.DEFAULT_PARAMETERS;
  protected String sql;
  protected AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
  protected List<Query<?>> expectedQueries;
  protected List<Object[]> expectedResults;
  protected RowSignature expectedResultSignature;
  protected List<ResourceAction> expectedResources;
  protected ResultsVerifier expectedResultsVerifier;
  protected @Nullable Consumer<ExpectedException> expectedExceptionInitializer;
  protected boolean skipVectorize;
  protected boolean queryCannotVectorize;
  protected AuthConfig authConfig = new AuthConfig();

  public QueryTestBuilder(final QueryTestConfig config)
  {
    this.config = config;
  }

  public QueryTestBuilder plannerConfig(PlannerConfig plannerConfig)
  {
    this.plannerConfig = plannerConfig;
    return this;
  }

  public QueryTestBuilder queryContext(Map<String, Object> queryContext)
  {
    this.queryContext = queryContext;
    return this;
  }

  public QueryTestBuilder parameters(List<SqlParameter> parameters)
  {
    this.parameters = parameters;
    return this;
  }

  public QueryTestBuilder sql(String sql)
  {
    this.sql = sql;
    return this;
  }

  public QueryTestBuilder authResult(AuthenticationResult authenticationResult)
  {
    this.authenticationResult = authenticationResult;
    return this;
  }

  public QueryTestBuilder expectedQuery(Query<?> expectedQuery)
  {
    if (expectedQuery == null) {
      return this;
    }
    return expectedQueries(Collections.singletonList(expectedQuery));
  }

  public QueryTestBuilder expectedQueries(List<Query<?>> expectedQueries)
  {
    this.expectedQueries = expectedQueries;
    return this;
  }

  public QueryTestBuilder expectedResults(
      final List<Object[]> expectedResults
  )
  {
    this.expectedResults = expectedResults;
    return this;
  }

  public QueryTestBuilder expectedSignature(
      final RowSignature expectedResultSignature
  )
  {
    this.expectedResultSignature = expectedResultSignature;
    return this;
  }

  public QueryTestBuilder expectedResults(ResultsVerifier expectedResultsVerifier)
  {
    this.expectedResultsVerifier = expectedResultsVerifier;
    return this;
  }

  public QueryTestBuilder expectedResources(List<ResourceAction> expectedResources)
  {
    this.expectedResources = expectedResources;
    return this;
  }

  public QueryTestBuilder expectedException(Consumer<ExpectedException> expectedExceptionInitializer)
  {
    this.expectedExceptionInitializer = expectedExceptionInitializer;
    return this;
  }

  public QueryTestBuilder skipVectorize()
  {
    return skipVectorize(true);
  }

  public QueryTestBuilder skipVectorize(boolean skipVectorize)
  {
    this.skipVectorize = skipVectorize;
    return this;
  }

  public QueryTestBuilder cannotVectorize()
  {
    return cannotVectorize(true);
  }

  public QueryTestBuilder cannotVectorize(boolean cannotVectorize)
  {
    this.queryCannotVectorize = cannotVectorize;
    return this;
  }

  public QueryTestBuilder authConfig(AuthConfig authConfig)
  {
    this.authConfig = authConfig;
    return this;
  }

  public QueryTestRunner build()
  {
    return config.analyze(this);
  }

  public void run()
  {
    build().run();
  }
}
