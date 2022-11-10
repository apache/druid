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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CalciteIngestionDmlTest extends BaseCalciteQueryTest
{
  protected static final Map<String, Object> DEFAULT_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .put(QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID)
                  .build();

  protected static final RowSignature FOO_TABLE_SIGNATURE =
      RowSignature.builder()
                  .addTimeColumn()
                  .add("dim1", ColumnType.STRING)
                  .add("dim2", ColumnType.STRING)
                  .add("dim3", ColumnType.STRING)
                  .add("cnt", ColumnType.LONG)
                  .add("m1", ColumnType.FLOAT)
                  .add("m2", ColumnType.DOUBLE)
                  .add("unique_dim1", HyperUniquesAggregatorFactory.TYPE)
                  .build();

  protected final ExternalDataSource externalDataSource = new ExternalDataSource(
      new InlineInputSource("a,b,1\nc,d,2\n"),
      new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0),
      RowSignature.builder()
                  .add("x", ColumnType.STRING)
                  .add("y", ColumnType.STRING)
                  .add("z", ColumnType.LONG)
                  .build()
  );

  protected boolean didTest = false;

  public CalciteIngestionDmlTest()
  {
    super(IngestionTestSqlEngine.INSTANCE);
  }

  @After
  public void tearDown()
  {
    // Catch situations where tests forgot to call "verify" on their tester.
    if (!didTest) {
      throw new ISE("Test was not run; did you call verify() on a tester?");
    }
  }

  protected String externSql(final ExternalDataSource externalDataSource)
  {
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      return StringUtils.format(
          "TABLE(extern(%s, %s, %s))",
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputSource())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputFormat())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getSignature()))
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  protected Map<String, Object> queryContextWithGranularity(Granularity granularity)
  {
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    String granularityString = null;
    try {
      granularityString = queryJsonMapper.writeValueAsString(granularity);
    }
    catch (JsonProcessingException e) {
      Assert.fail(e.getMessage());
    }
    return ImmutableMap.of(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY, granularityString);
  }

  protected IngestionDmlTester testIngestionQuery()
  {
    return new IngestionDmlTester();
  }

  public class IngestionDmlTester
  {
    private String sql;
    private PlannerConfig plannerConfig = new PlannerConfig();
    private Map<String, Object> queryContext = DEFAULT_CONTEXT;
    private AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
    private String expectedTargetDataSource;
    private RowSignature expectedTargetSignature;
    private List<ResourceAction> expectedResources;
    private Query<?> expectedQuery;
    private Matcher<Throwable> validationErrorMatcher;

    private IngestionDmlTester()
    {
      // Nothing to do.
    }

    public IngestionDmlTester sql(final String sql)
    {
      this.sql = sql;
      return this;
    }

    protected IngestionDmlTester sql(final String sqlPattern, final Object arg, final Object... otherArgs)
    {
      final Object[] args = new Object[otherArgs.length + 1];
      args[0] = arg;
      System.arraycopy(otherArgs, 0, args, 1, otherArgs.length);
      this.sql = StringUtils.format(sqlPattern, args);
      return this;
    }

    public IngestionDmlTester context(final Map<String, Object> context)
    {
      this.queryContext = context;
      return this;
    }

    public IngestionDmlTester authentication(final AuthenticationResult authenticationResult)
    {
      this.authenticationResult = authenticationResult;
      return this;
    }

    public IngestionDmlTester expectTarget(
        final String expectedTargetDataSource,
        final RowSignature expectedTargetSignature
    )
    {
      this.expectedTargetDataSource = Preconditions.checkNotNull(expectedTargetDataSource, "expectedTargetDataSource");
      this.expectedTargetSignature = Preconditions.checkNotNull(expectedTargetSignature, "expectedTargetSignature");
      return this;
    }

    public IngestionDmlTester expectResources(final ResourceAction... expectedResources)
    {
      this.expectedResources = Arrays.asList(expectedResources);
      return this;
    }

    @SuppressWarnings("rawtypes")
    public IngestionDmlTester expectQuery(final Query expectedQuery)
    {
      this.expectedQuery = expectedQuery;
      return this;
    }

    public IngestionDmlTester expectValidationError(Matcher<Throwable> validationErrorMatcher)
    {
      this.validationErrorMatcher = validationErrorMatcher;
      return this;
    }

    public IngestionDmlTester expectValidationError(Class<? extends Throwable> clazz)
    {
      return expectValidationError(CoreMatchers.instanceOf(clazz));
    }

    public IngestionDmlTester expectValidationError(Class<? extends Throwable> clazz, String message)
    {
      return expectValidationError(
          CoreMatchers.allOf(
              CoreMatchers.instanceOf(clazz),
              ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo(message))
          )
      );
    }

    public void verify()
    {
      if (didTest) {
        // It's good form to only do one test per method.
        // This also helps us ensure that "verify" actually does get called.
        throw new ISE("Use one @Test method per tester");
      }

      didTest = true;

      if (sql == null) {
        throw new ISE("Test must have SQL statement");
      }

      try {
        log.info("SQL: %s", sql);

        if (validationErrorMatcher != null) {
          verifyValidationError();
        } else {
          verifySuccess();
        }
      }
      catch (RuntimeException e) {
        throw e;
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void verifyValidationError()
    {
      if (expectedTargetDataSource != null) {
        throw new ISE("Test must not have expectedTargetDataSource");
      }

      if (expectedResources != null) {
        throw new ISE("Test must not have expectedResources");
      }

      if (expectedQuery != null) {
        throw new ISE("Test must not have expectedQuery");
      }

      queryLogHook.clearRecordedQueries();
      final Throwable e = Assert.assertThrows(
          Throwable.class,
          () -> {
            getSqlStatementFactory(plannerConfig).directStatement(sqlQuery()).execute();
          }
      );

      MatcherAssert.assertThat(e, validationErrorMatcher);
      Assert.assertTrue(queryLogHook.getRecordedQueries().isEmpty());
    }

    private void verifySuccess()
    {
      if (expectedTargetDataSource == null) {
        throw new ISE("Test must have expectedTargetDataSource");
      }

      if (expectedResources == null) {
        throw new ISE("Test must have expectedResources");
      }

      testBuilder()
          .sql(sql)
          .queryContext(queryContext)
          .authResult(authenticationResult)
          .plannerConfig(plannerConfig)
          .expectedResources(expectedResources)
          .run();

      testBuilder()
          .sql(sql)
          .queryContext(queryContext)
          .authResult(authenticationResult)
          .plannerConfig(plannerConfig)
          .expectedQuery(expectedQuery)
          .expectedResults(Collections.singletonList(new Object[]{expectedTargetDataSource, expectedTargetSignature}))
          .run();
    }

    private SqlQueryPlus sqlQuery()
    {
      return SqlQueryPlus.builder(sql)
          .context(queryContext)
          .auth(authenticationResult)
          .build();
    }
  }

  protected static ResourceAction viewRead(final String viewName)
  {
    return new ResourceAction(new Resource(viewName, ResourceType.VIEW), Action.READ);
  }

  protected static ResourceAction dataSourceRead(final String dataSource)
  {
    return new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ);
  }

  protected static ResourceAction dataSourceWrite(final String dataSource)
  {
    return new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE);
  }
}
