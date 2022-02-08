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
import org.apache.druid.java.util.common.granularity.Granularity;
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
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
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

  private static final Map<String, Object> PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT = ImmutableMap.of(
      DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
      "{\"type\":\"all\"}"
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
        .sql("INSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertFromView()
  {
    testInsertQuery()
        .sql("INSERT INTO dst SELECT * FROM view.aview PARTITIONED BY ALL TIME")
        .expectTarget("dst", RowSignature.builder().add("dim1_firstchar", ColumnType.STRING).build())
        .expectResources(viewRead("aview"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING))
                .filters(selector("dim2", "a", null))
                .columns("v0")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertIntoExistingTable()
  {
    testInsertQuery()
        .sql("INSERT INTO foo SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectTarget("foo", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertIntoQualifiedTable()
  {
    testInsertQuery()
        .sql("INSERT INTO druid.dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertIntoInvalidDataSourceName()
  {
    testInsertQuery()
        .sql("INSERT INTO \"in/valid\" SELECT dim1, dim2 FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "INSERT dataSource cannot contain the '/' character.")
        .verify();
  }

  @Test
  public void testInsertUsingColumnList()
  {
    testInsertQuery()
        .sql("INSERT INTO dst (foo, bar) SELECT dim1, dim2 FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "INSERT with target column list is not supported.")
        .verify();
  }

  @Test
  public void testUpsert()
  {
    testInsertQuery()
        .sql("UPSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "UPSERT is not supported.")
        .verify();
  }

  @Test
  public void testInsertIntoSystemTable()
  {
    testInsertQuery()
        .sql("INSERT INTO INFORMATION_SCHEMA.COLUMNS SELECT * FROM foo PARTITIONED BY ALL TIME")
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
        .sql("INSERT INTO view.aview SELECT * FROM foo PARTITIONED BY ALL TIME")
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
        .sql("INSERT INTO dst SELECT * FROM \"%s\" PARTITIONED BY ALL TIME", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertIntoUnauthorizedDataSource()
  {
    testInsertQuery()
        .sql("INSERT INTO \"%s\" SELECT * FROM foo PARTITIONED BY ALL TIME", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertIntoNonexistentSchema()
  {
    testInsertQuery()
        .sql("INSERT INTO nonexistent.dst SELECT * FROM foo PARTITIONED BY ALL TIME")
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
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertWithPartitionedBy()
  {
    // Test correctness of the query when only PARTITIONED BY clause is present
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.FLOAT)
                                                  .add("dim1", ColumnType.STRING)
                                                  .build();

    testInsertQuery()
        .sql(
            "INSERT INTO druid.dst SELECT __time, FLOOR(m1) as floor_m1, dim1 FROM foo PARTITIONED BY TIME_FLOOR(__time, 'PT1H')")
        .expectTarget("dst", targetRowSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "v0")
                .virtualColumns(expressionVirtualColumn("v0", "floor(\"m1\")", ColumnType.FLOAT))
                .context(queryContextWithGranularity(Granularities.HOUR))
                .build()
        )
        .verify();
  }

  @Test
  public void testPartitionedBySupportedClauses()
  {
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("dim1", ColumnType.STRING)
                                                  .build();

    Map<String, Granularity> partitionedByArgumentToGranularityMap =
        ImmutableMap.<String, Granularity>builder()
                    .put("HOUR", Granularities.HOUR)
                    .put("DAY", Granularities.DAY)
                    .put("MONTH", Granularities.MONTH)
                    .put("YEAR", Granularities.YEAR)
                    .put("ALL TIME", Granularities.ALL)
                    .put("FLOOR(__time TO QUARTER)", Granularities.QUARTER)
                    .put("TIME_FLOOR(__time, 'PT1H')", Granularities.HOUR)
                    .build();

    partitionedByArgumentToGranularityMap.forEach((partitionedByArgument, expectedGranularity) -> {
      Map<String, Object> queryContext = null;
      try {
        queryContext = ImmutableMap.of(
            DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY, queryJsonMapper.writeValueAsString(expectedGranularity)
        );
      }
      catch (JsonProcessingException e) {
        // Won't reach here
        Assert.fail(e.getMessage());
      }

      testInsertQuery()
          .sql(StringUtils.format(
              "INSERT INTO druid.dst SELECT __time, dim1 FROM foo PARTITIONED BY %s",
              partitionedByArgument
          ))
          .expectTarget("dst", targetRowSignature)
          .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
          .expectQuery(
              newScanQueryBuilder()
                  .dataSource("foo")
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("__time", "dim1")
                  .context(queryContext)
                  .build()
          )
          .verify();
      didTest = false;
    });
    didTest = true;
  }

  @Test
  public void testInsertWithClusteredBy()
  {
    // Test correctness of the query when only CLUSTERED BY clause is present
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.FLOAT)
                                                  .add("dim1", ColumnType.STRING)
                                                  .add("EXPR$3", ColumnType.DOUBLE)
                                                  .build();
    testInsertQuery()
        .sql(
            "INSERT INTO druid.dst "
            + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) FROM foo "
            + "PARTITIONED BY FLOOR(__time TO DAY) CLUSTERED BY 2, dim1 DESC, CEIL(m2)"
        )
        .expectTarget("dst", targetRowSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "v0", "v1")
                .virtualColumns(
                    expressionVirtualColumn("v0", "floor(\"m1\")", ColumnType.FLOAT),
                    expressionVirtualColumn("v1", "ceil(\"m2\")", ColumnType.DOUBLE)
                )
                .orderBy(
                    ImmutableList.of(
                        new ScanQuery.OrderBy("v0", ScanQuery.Order.ASCENDING),
                        new ScanQuery.OrderBy("dim1", ScanQuery.Order.DESCENDING),
                        new ScanQuery.OrderBy("v1", ScanQuery.Order.ASCENDING)
                    )
                )
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertWithPartitionedByAndClusteredBy()
  {
    // Test correctness of the query when both PARTITIONED BY and CLUSTERED BY clause is present
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.FLOAT)
                                                  .add("dim1", ColumnType.STRING)
                                                  .build();

    testInsertQuery()
        .sql(
            "INSERT INTO druid.dst SELECT __time, FLOOR(m1) as floor_m1, dim1 FROM foo PARTITIONED BY DAY CLUSTERED BY 2, dim1")
        .expectTarget("dst", targetRowSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "v0")
                .virtualColumns(expressionVirtualColumn("v0", "floor(\"m1\")", ColumnType.FLOAT))
                .orderBy(
                    ImmutableList.of(
                        new ScanQuery.OrderBy("v0", ScanQuery.Order.ASCENDING),
                        new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING)
                    )
                )
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertWithPartitionedByAndLimitOffset()
  {
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.FLOAT)
                                                  .add("dim1", ColumnType.STRING)
                                                  .build();

    testInsertQuery()
        .sql(
            "INSERT INTO druid.dst SELECT __time, FLOOR(m1) as floor_m1, dim1 FROM foo LIMIT 10 OFFSET 20 PARTITIONED BY DAY")
        .expectTarget("dst", targetRowSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "v0")
                .virtualColumns(expressionVirtualColumn("v0", "floor(\"m1\")", ColumnType.FLOAT))
                .limit(10)
                .offset(20)
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertWithClusteredByAndOrderBy() throws Exception
  {
    try {
      testQuery(
          StringUtils.format(
              "INSERT INTO dst SELECT * FROM %s ORDER BY 2 PARTITIONED BY ALL TIME",
              externSql(externalDataSource)
          ),
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assert.fail("Exception should be thrown");
    }
    catch (SqlPlanningException e) {
      Assert.assertEquals(
          "Cannot have ORDER BY on an INSERT query, use CLUSTERED BY instead.",
          e.getMessage()
      );
    }
    didTest = true;
  }

  @Test
  public void testInsertWithPartitionedByContainingInvalidGranularity() throws Exception
  {
    // Throws a ValidationException, which gets converted to a SqlPlanningException before throwing to end user
    try {
      testQuery(
          "INSERT INTO dst SELECT * FROM foo PARTITIONED BY 'invalid_granularity'",
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assert.fail("Exception should be thrown");
    }
    catch (SqlPlanningException e) {
      Assert.assertEquals(
          "Encountered 'invalid_granularity' after PARTITIONED BY. Expected HOUR, DAY, MONTH, YEAR, ALL TIME, FLOOR function or TIME_FLOOR function",
          e.getMessage()
      );
    }
    didTest = true;
  }

  @Test
  public void testInsertWithOrderBy() throws Exception
  {
    try {
      testQuery(
          StringUtils.format(
              "INSERT INTO dst SELECT * FROM %s ORDER BY 2 PARTITIONED BY ALL TIME",
              externSql(externalDataSource)
          ),
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assert.fail("Exception should be thrown");
    }
    catch (SqlPlanningException e) {
      Assert.assertEquals(
          "Cannot have ORDER BY on an INSERT query, use CLUSTERED BY instead.",
          e.getMessage()
      );
    }
    finally {
      didTest = true;
    }
  }

  // Currently EXPLAIN PLAN FOR doesn't work with the modified syntax
  @Ignore
  @Test
  public void testExplainInsertWithPartitionedByAndClusteredBy()
  {
    Assert.assertThrows(
        SqlPlanningException.class,
        () ->
            testQuery(
                StringUtils.format(
                    "EXPLAIN PLAN FOR INSERT INTO dst SELECT * FROM %s PARTITIONED BY DAY CLUSTERED BY 1",
                    externSql(externalDataSource)
                ),
                ImmutableList.of(),
                ImmutableList.of()
            )
    );
    didTest = true;
  }

  @Ignore
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
        StringUtils.format(
            "EXPLAIN PLAN FOR INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME",
            externSql(externalDataSource)
        ),
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

  @Ignore
  @Test
  public void testExplainInsertFromExternalUnauthorized()
  {
    // Use testQuery for EXPLAIN (not testInsertQuery).
    Assert.assertThrows(
        ForbiddenException.class,
        () ->
            testQuery(
                StringUtils.format(
                    "EXPLAIN PLAN FOR INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME",
                    externSql(externalDataSource)
                ),
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
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertFromExternalProjectSort()
  {
    // INSERT with a particular column ordering.

    testInsertQuery()
        .sql(
            "INSERT INTO dst SELECT x || y AS xy, z FROM %s PARTITIONED BY ALL TIME CLUSTERED BY 1, 2",
            externSql(externalDataSource)
        )
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
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
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
            "INSERT INTO dst SELECT x, SUM(z) AS sum_z, COUNT(*) AS cnt FROM %s GROUP BY 1 PARTITIONED BY ALL TIME",
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
                        .setContext(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
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
            "INSERT INTO dst SELECT COUNT(*) AS cnt FROM %s PARTITIONED BY ALL TIME",
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
                        .setContext(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
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

  private Map<String, Object> queryContextWithGranularity(Granularity granularity)
  {
    String granularityString = null;
    try {
      granularityString = queryJsonMapper.writeValueAsString(granularity);
    }
    catch (JsonProcessingException e) {
      Assert.fail(e.getMessage());
    }
    return ImmutableMap.of(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY, granularityString);
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
