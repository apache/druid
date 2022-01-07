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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CalciteInsertDmlTest extends BaseCalciteQueryTest
{
  private static final Map<String, Object> DEFAULT_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .put(PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_ID)
                  .build();

  private static final RowSignature FOO_TABLE_SIGNATURE =
      RowSignature.builder()
                  .addTimeColumn()
                  .add("cnt", ColumnType.LONG)
                  .add("dim1", ColumnType.STRING)
                  .add("dim2", ColumnType.STRING)
                  .add("dim3", ColumnType.STRING)
                  .add("m1", ColumnType.FLOAT)
                  .add("m2", ColumnType.DOUBLE)
                  .add("unique_dim1", HyperUniquesAggregatorFactory.TYPE)
                  .build();

  private final ExternalDataSource externalDataSource = new ExternalDataSource(
      new InlineInputSource("a,b,1\nc,d,2\n"),
      new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0),
      RowSignature.builder()
                  .add("x", ColumnType.STRING)
                  .add("y", ColumnType.STRING)
                  .add("z", ColumnType.LONG)
                  .build()
  );

  private boolean didTest = false;

  @After
  @Override
  public void tearDown() throws Exception
  {
    super.tearDown();

    // Catch situations where tests forgot to call "verify" on their tester.
    if (!didTest) {
      throw new ISE("Test was not run; did you call verify() on a tester?");
    }
  }

  @Test
  public void testInsertFromTable()
  {
    testInsertQuery()
        .sql("INSERT INTO dst SELECT * FROM foo")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertFromView()
  {
    testInsertQuery()
        .sql("INSERT INTO dst SELECT * FROM view.aview")
        .expectTarget("dst", RowSignature.builder().add("dim1_firstchar", ColumnType.STRING).build())
        .expectResources(viewRead("aview"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING))
                .filters(selector("dim2", "a", null))
                .columns("v0")
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertIntoExistingTable()
  {
    testInsertQuery()
        .sql("INSERT INTO foo SELECT * FROM foo")
        .expectTarget("foo", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertIntoQualifiedTable()
  {
    testInsertQuery()
        .sql("INSERT INTO druid.dst SELECT * FROM foo")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertIntoInvalidDataSourceName()
  {
    testInsertQuery()
        .sql("INSERT INTO \"in/valid\" SELECT dim1, dim2 FROM foo")
        .expectValidationError(SqlPlanningException.class, "INSERT dataSource cannot contain the '/' character.")
        .verify();
  }

  @Test
  public void testInsertUsingColumnList()
  {
    testInsertQuery()
        .sql("INSERT INTO dst (foo, bar) SELECT dim1, dim2 FROM foo")
        .expectValidationError(SqlPlanningException.class, "INSERT with target column list is not supported.")
        .verify();
  }

  @Test
  public void testUpsert()
  {
    testInsertQuery()
        .sql("UPSERT INTO dst SELECT * FROM foo")
        .expectValidationError(SqlPlanningException.class, "UPSERT is not supported.")
        .verify();
  }

  @Test
  public void testInsertIntoSystemTable()
  {
    testInsertQuery()
        .sql("INSERT INTO INFORMATION_SCHEMA.COLUMNS SELECT * FROM foo")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot INSERT into [INFORMATION_SCHEMA.COLUMNS] because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testInsertIntoView()
  {
    testInsertQuery()
        .sql("INSERT INTO view.aview SELECT * FROM foo")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot INSERT into [view.aview] because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testInsertFromUnauthorizedDataSource()
  {
    testInsertQuery()
        .sql("INSERT INTO dst SELECT * FROM \"%s\"", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertIntoUnauthorizedDataSource()
  {
    testInsertQuery()
        .sql("INSERT INTO \"%s\" SELECT * FROM foo", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertIntoNonexistentSchema()
  {
    testInsertQuery()
        .sql("INSERT INTO nonexistent.dst SELECT * FROM foo")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot INSERT into [nonexistent.dst] because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testInsertFromExternal()
  {
    testInsertQuery()
        .sql("INSERT INTO dst SELECT * FROM %s", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .build()
        )
        .verify();
  }

  @Test
  public void testExplainInsertFromExternal() throws Exception
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final ScanQuery expectedQuery = newScanQueryBuilder()
        .dataSource(externalDataSource)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("x", "y", "z")
        .context(
            queryJsonMapper.readValue(
                "{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}",
                JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
            )
        )
        .build();

    final String expectedExplanation =
        "DruidQueryRel(query=["
        + queryJsonMapper.writeValueAsString(expectedQuery)
        + "], signature=[{x:STRING, y:STRING, z:LONG}])\n";

    // Use testQuery for EXPLAIN (not testInsertQuery).
    testQuery(
        new PlannerConfig(),
        StringUtils.format("EXPLAIN PLAN FOR INSERT INTO dst SELECT * FROM %s", externSql(externalDataSource)),
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                expectedExplanation,
                "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"dst\",\"type\":\"DATASOURCE\"}]"
            }
        )
    );

    // Not using testInsertQuery, so must set didTest manually to satisfy the check in tearDown.
    didTest = true;
  }

  @Test
  public void testExplainInsertFromExternalUnauthorized()
  {
    // Use testQuery for EXPLAIN (not testInsertQuery).
    Assert.assertThrows(
        ForbiddenException.class,
        () ->
            testQuery(
                StringUtils.format("EXPLAIN PLAN FOR INSERT INTO dst SELECT * FROM %s", externSql(externalDataSource)),
                ImmutableList.of(),
                ImmutableList.of()
            )
    );

    // Not using testInsertQuery, so must set didTest manually to satisfy the check in tearDown.
    didTest = true;
  }

  @Test
  public void testInsertFromExternalUnauthorized()
  {
    testInsertQuery()
        .sql("INSERT INTO dst SELECT * FROM %s", externSql(externalDataSource))
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertFromExternalProjectSort()
  {
    // INSERT with a particular column ordering.

    testInsertQuery()
        .sql("INSERT INTO dst SELECT x || y AS xy, z FROM %s ORDER BY 1, 2", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", RowSignature.builder().add("xy", ColumnType.STRING).add("z", ColumnType.LONG).build())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"x\",\"y\")", ColumnType.STRING))
                .columns("v0", "z")
                .orderBy(
                    ImmutableList.of(
                        new ScanQuery.OrderBy("v0", ScanQuery.Order.ASCENDING),
                        new ScanQuery.OrderBy("z", ScanQuery.Order.ASCENDING)
                    )
                )
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertFromExternalAggregate()
  {
    // INSERT with rollup.

    testInsertQuery()
        .sql(
            "INSERT INTO dst SELECT x, SUM(z) AS sum_z, COUNT(*) AS cnt FROM %s GROUP BY 1",
            externSql(externalDataSource)
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget(
            "dst",
            RowSignature.builder()
                        .add("x", ColumnType.STRING)
                        .add("sum_z", ColumnType.LONG)
                        .add("cnt", ColumnType.LONG)
                        .build()
        )
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            GroupByQuery.builder()
                        .setDataSource(externalDataSource)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("x", "d0")))
                        .setAggregatorSpecs(
                            new LongSumAggregatorFactory("a0", "z"),
                            new CountAggregatorFactory("a1")
                        )
                        .build()
        )
        .verify();
  }

  @Test
  public void testInsertFromExternalAggregateAll()
  {
    // INSERT with rollup into a single row (no GROUP BY exprs).

    testInsertQuery()
        .sql(
            "INSERT INTO dst SELECT COUNT(*) AS cnt FROM %s",
            externSql(externalDataSource)
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget(
            "dst",
            RowSignature.builder()
                        .add("cnt", ColumnType.LONG)
                        .build()
        )
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            GroupByQuery.builder()
                        .setDataSource(externalDataSource)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .build()
        )
        .verify();
  }

  private String externSql(final ExternalDataSource externalDataSource)
  {
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

  private InsertDmlTester testInsertQuery()
  {
    return new InsertDmlTester();
  }

  public class InsertDmlTester
  {
    private String sql;
    private PlannerConfig plannerConfig = new PlannerConfig();
    private Map<String, Object> queryContext = DEFAULT_CONTEXT;
    private AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
    private String expectedTargetDataSource;
    private RowSignature expectedTargetSignature;
    private List<ResourceAction> expectedResources;
    private Query expectedQuery;
    private Matcher<Throwable> validationErrorMatcher;

    private InsertDmlTester()
    {
      // Nothing to do.
    }

    public InsertDmlTester sql(final String sql)
    {
      this.sql = sql;
      return this;
    }

    private InsertDmlTester sql(final String sqlPattern, final Object arg, final Object... otherArgs)
    {
      final Object[] args = new Object[otherArgs.length + 1];
      args[0] = arg;
      System.arraycopy(otherArgs, 0, args, 1, otherArgs.length);
      this.sql = StringUtils.format(sqlPattern, args);
      return this;
    }

    public InsertDmlTester context(final Map<String, Object> context)
    {
      this.queryContext = context;
      return this;
    }

    public InsertDmlTester authentication(final AuthenticationResult authenticationResult)
    {
      this.authenticationResult = authenticationResult;
      return this;
    }

    public InsertDmlTester expectTarget(
        final String expectedTargetDataSource,
        final RowSignature expectedTargetSignature
    )
    {
      this.expectedTargetDataSource = Preconditions.checkNotNull(expectedTargetDataSource, "expectedTargetDataSource");
      this.expectedTargetSignature = Preconditions.checkNotNull(expectedTargetSignature, "expectedTargetSignature");
      return this;
    }

    public InsertDmlTester expectResources(final ResourceAction... expectedResources)
    {
      this.expectedResources = Arrays.asList(expectedResources);
      return this;
    }

    @SuppressWarnings("rawtypes")
    public InsertDmlTester expectQuery(final Query expectedQuery)
    {
      this.expectedQuery = expectedQuery;
      return this;
    }

    public InsertDmlTester expectValidationError(Matcher<Throwable> validationErrorMatcher)
    {
      this.validationErrorMatcher = validationErrorMatcher;
      return this;
    }

    public InsertDmlTester expectValidationError(Class<? extends Throwable> clazz)
    {
      return expectValidationError(CoreMatchers.instanceOf(clazz));
    }

    public InsertDmlTester expectValidationError(Class<? extends Throwable> clazz, String message)
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
        queryLogHook.clearRecordedQueries();

        if (validationErrorMatcher != null) {
          verifyValidationError();
        } else {
          verifySuccess();
        }
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

      final SqlLifecycleFactory sqlLifecycleFactory = getSqlLifecycleFactory(
          plannerConfig,
          createOperatorTable(),
          createMacroTable(),
          CalciteTests.TEST_AUTHORIZER_MAPPER,
          queryJsonMapper
      );

      final SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
      sqlLifecycle.initialize(sql, queryContext);

      final Throwable e = Assert.assertThrows(
          Throwable.class,
          () -> sqlLifecycle.validateAndAuthorize(authenticationResult)
      );

      MatcherAssert.assertThat(e, validationErrorMatcher);
      Assert.assertTrue(queryLogHook.getRecordedQueries().isEmpty());
    }

    private void verifySuccess() throws Exception
    {
      if (expectedTargetDataSource == null) {
        throw new ISE("Test must have expectedTargetDataSource");
      }

      if (expectedResources == null) {
        throw new ISE("Test must have expectedResources");
      }

      final List<Query> expectedQueries =
          expectedQuery == null
          ? Collections.emptyList()
          : Collections.singletonList(recursivelyOverrideContext(expectedQuery, queryContext));

      Assert.assertEquals(
          ImmutableSet.copyOf(expectedResources),
          analyzeResources(plannerConfig, sql, authenticationResult)
      );

      final List<Object[]> results =
          getResults(plannerConfig, queryContext, Collections.emptyList(), sql, authenticationResult);

      verifyResults(
          sql,
          expectedQueries,
          Collections.singletonList(new Object[]{expectedTargetDataSource, expectedTargetSignature}),
          results
      );
    }
  }

  private static ResourceAction viewRead(final String viewName)
  {
    return new ResourceAction(new Resource(viewName, ResourceType.VIEW), Action.READ);
  }

  private static ResourceAction dataSourceRead(final String dataSource)
  {
    return new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ);
  }

  private static ResourceAction dataSourceWrite(final String dataSource)
  {
    return new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE);
  }
}
