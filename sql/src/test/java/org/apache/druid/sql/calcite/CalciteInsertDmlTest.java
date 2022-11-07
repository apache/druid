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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.IngestHandler;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CalciteInsertDmlTest extends CalciteIngestionDmlTest
{
  private static final Map<String, Object> PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT = ImmutableMap.of(
      DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
      "{\"type\":\"all\"}"
  );

  @Test
  public void testInsertFromTable()
  {
    testIngestionQuery()
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
  public void testInsertFromViewA()
  {
    testIngestionQuery()
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
  public void testInsertFromViewC()
  {
    final RowSignature expectedSignature =
        RowSignature.builder()
                    .add("dim1_firstchar", ColumnType.STRING)
                    .add("dim2", ColumnType.STRING)
                    .add("l2", ColumnType.LONG)
                    .build();

    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM view.cview PARTITIONED BY ALL TIME")
        .expectTarget("dst", expectedSignature)
        .expectResources(viewRead("cview"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource("foo")
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(selector("dim2", "a", null))
                                .columns("dim1", "dim2")
                                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource("numfoo")
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns("dim2", "l2")
                                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                                .build()
                        ),
                        "j0.",
                        "(\"dim2\" == \"j0.dim2\")",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING),
                    expressionVirtualColumn("v1", "'a'", ColumnType.STRING)
                )
                .columns("j0.l2", "v0", "v1")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertIntoExistingTable()
  {
    testIngestionQuery()
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
    testIngestionQuery()
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
    testIngestionQuery()
        .sql("INSERT INTO \"in/valid\" SELECT dim1, dim2 FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "INSERT dataSource cannot contain the '/' character.")
        .verify();
  }

  @Test
  public void testInsertUsingColumnList()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst (foo, bar) SELECT dim1, dim2 FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "INSERT with a target column list is not supported.")
        .verify();
  }

  @Test
  public void testUpsert()
  {
    testIngestionQuery()
        .sql("UPSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "UPSERT is not supported.")
        .verify();
  }

  @Test
  public void testSelectFromSystemTable()
  {
    // TestInsertSqlEngine does not include ALLOW_BINDABLE_PLAN, so cannot query system tables.

    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM INFORMATION_SCHEMA.COLUMNS PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot query table INFORMATION_SCHEMA.COLUMNS with SQL engine 'ingestion-test'."
        )
        .verify();
  }

  @Test
  public void testInsertIntoSystemTable()
  {
    testIngestionQuery()
        .sql("INSERT INTO INFORMATION_SCHEMA.COLUMNS SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot INSERT into INFORMATION_SCHEMA.COLUMNS because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testInsertIntoView()
  {
    testIngestionQuery()
        .sql("INSERT INTO view.aview SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot INSERT into view.aview because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testInsertFromUnauthorizedDataSource()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM \"%s\" PARTITIONED BY ALL TIME", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertIntoUnauthorizedDataSource()
  {
    testIngestionQuery()
        .sql("INSERT INTO \"%s\" SELECT * FROM foo PARTITIONED BY ALL TIME", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertIntoNonexistentSchema()
  {
    testIngestionQuery()
        .sql("INSERT INTO nonexistent.dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot INSERT into nonexistent.dst because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testInsertFromExternal()
  {
    testIngestionQuery()
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

    testIngestionQuery()
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
                    .put("ALL", Granularities.ALL)
                    .put("ALL TIME", Granularities.ALL)
                    .put("FLOOR(__time TO QUARTER)", Granularities.QUARTER)
                    .put("TIME_FLOOR(__time, 'PT1H')", Granularities.HOUR)
                    .build();

    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
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

      testIngestionQuery()
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
                                                  .add("ceil_m2", ColumnType.DOUBLE)
                                                  .build();
    testIngestionQuery()
        .sql(
            "INSERT INTO druid.dst "
            + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo "
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
  public void testInsertWithoutPartitionedByWithClusteredBy()
  {
    testIngestionQuery()
        .sql(
            "INSERT INTO druid.dst "
            + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo "
            + "CLUSTERED BY 2, dim1 DESC, CEIL(m2)"
        )
        .expectValidationError(
            SqlPlanningException.class,
            "CLUSTERED BY found before PARTITIONED BY. In Druid, the CLUSTERED BY clause must follow the PARTITIONED BY clause"
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

    testIngestionQuery()
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

    testIngestionQuery()
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
  public void testInsertWithClusteredByAndOrderBy()
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
          "Cannot have ORDER BY on an INSERT statement, use CLUSTERED BY instead.",
          e.getMessage()
      );
    }
    didTest = true;
  }

  @Test
  public void testInsertWithPartitionedByContainingInvalidGranularity()
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
  public void testInsertWithOrderBy()
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
          "Cannot have ORDER BY on an INSERT statement, use CLUSTERED BY instead.",
          e.getMessage()
      );
    }
    finally {
      didTest = true;
    }
  }

  @Test
  public void testInsertWithoutPartitionedBy()
  {
    SqlPlanningException e = Assert.assertThrows(
        SqlPlanningException.class,
        () ->
            testQuery(
                StringUtils.format("INSERT INTO dst SELECT * FROM %s", externSql(externalDataSource)),
                ImmutableList.of(),
                ImmutableList.of()
            )
    );
    Assert.assertEquals("INSERT statements must specify PARTITIONED BY clause explicitly", e.getMessage());
    didTest = true;
  }

  @Test
  public void testExplainInsertFromExternal() throws IOException
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    final ScanQuery expectedQuery = newScanQueryBuilder()
        .dataSource(externalDataSource)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("x", "y", "z")
        .context(
            queryJsonMapper.readValue(
                "{\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}",
                JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
            )
        )
        .build();

    final String expectedExplanation =
        "DruidQueryRel(query=["
        + queryJsonMapper.writeValueAsString(expectedQuery)
        + "], signature=[{x:STRING, y:STRING, z:LONG}])\n";

    // Use testQuery for EXPLAIN (not testIngestionQuery).
    testQuery(
        PlannerConfig.builder().useNativeQueryExplain(false).build(),
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        StringUtils.format(
            "EXPLAIN PLAN FOR INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME",
            externSql(externalDataSource)
        ),
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    expectedExplanation,
                    "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"dst\",\"type\":\"DATASOURCE\"}]"
                }
            ),
            null
        ),
        null
    );

    // Not using testIngestionQuery, so must set didTest manually to satisfy the check in tearDown.
    didTest = true;
  }

  @Test
  public void testExplainInsertFromExternalUnauthorized()
  {
    // Use testQuery for EXPLAIN (not testIngestionQuery).
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

    // Not using testIngestionQuery, so must set didTest manually to satisfy the check in tearDown.
    didTest = true;
  }

  @Test
  public void testSurfaceErrorsWhenInsertingThroughIncorrectSelectStatment()
  {
    assertQueryIsUnplannable(
        "INSERT INTO druid.dst SELECT dim2, dim1, m1 FROM foo2 UNION SELECT dim1, dim2, m1 FROM foo PARTITIONED BY ALL TIME",
        "Possible error: SQL requires 'UNION' but only 'UNION ALL' is supported."
    );

    // Not using testIngestionQuery, so must set didTest manually to satisfy the check in tearDown.
    didTest = true;
  }

  @Test
  public void testInsertFromExternalUnauthorized()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertFromExternalProjectSort()
  {
    // INSERT with a particular column ordering.

    testIngestionQuery()
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

    testIngestionQuery()
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

    testIngestionQuery()
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

  @Test
  public void testInsertWithInvalidSelectStatement()
  {
    testIngestionQuery()
        .sql("INSERT INTO t SELECT channel, added as count FROM foo PARTITIONED BY ALL") // count is a keyword
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Encountered \"as count\""))
            )
        )
        .verify();
  }

  @Test
  public void testInsertWithUnnamedColumnInSelectStatement()
  {
    testIngestionQuery()
        .sql("INSERT INTO t SELECT dim1, dim2 || '-lol' FROM foo PARTITIONED BY ALL")
        .expectValidationError(
            SqlPlanningException.class,
            IngestHandler.UNNAMED_INGESTION_COLUMN_ERROR
        )
        .verify();
  }

  @Test
  public void testInsertWithInvalidColumnNameInIngest()
  {
    testIngestionQuery()
        .sql("INSERT INTO t SELECT __time, dim1 AS EXPR$0 FROM foo PARTITIONED BY ALL")
        .expectValidationError(
            SqlPlanningException.class,
            IngestHandler.UNNAMED_INGESTION_COLUMN_ERROR
        )
        .verify();
  }

  @Test
  public void testInsertWithUnnamedColumnInNestedSelectStatement()
  {
    testIngestionQuery()
        .sql("INSERT INTO test "
             + "SELECT __time, * FROM "
             + "(SELECT __time, LOWER(dim1) FROM foo) PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            IngestHandler.UNNAMED_INGESTION_COLUMN_ERROR
        )
        .verify();
  }

  @Test
  public void testInsertQueryWithInvalidGranularity()
  {
    testIngestionQuery()
        .sql("insert into foo1 select __time, dim1 FROM foo partitioned by time_floor(__time, 'PT2H')")
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                    "The granularity specified in PARTITIONED BY is not supported. "
                    + "Please use an equivalent of these granularities: second, minute, five_minute, ten_minute, "
                    + "fifteen_minute, thirty_minute, hour, six_hour, eight_hour, day, week, month, quarter, year, all."))
            )
        )
        .verify();
  }

  @Test
  public void testInsertOnExternalDataSourceWithIncompatibleTimeColumnSignature()
  {
    ExternalDataSource restrictedSignature = new ExternalDataSource(
        new InlineInputSource("100\nc200\n"),
        new CsvInputFormat(ImmutableList.of("__time"), null, false, false, 0),
        RowSignature.builder()
                    .add("__time", ColumnType.STRING)
                    .build()
    );
    testIngestionQuery()
        .sql(
            "INSERT INTO dst SELECT __time FROM %s PARTITIONED BY ALL TIME",
            externSql(restrictedSignature)
        )
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                    "EXTERN function with __time column can be used when __time column is of type long"))
            )
        )
        .verify();
  }

  @Test
  public void testInsertWithSqlOuterLimit()
  {
    HashMap<String, Object> context = new HashMap<>(DEFAULT_CONTEXT);
    context.put(PlannerContext.CTX_SQL_OUTER_LIMIT, 100);

    testIngestionQuery()
        .context(context)
        .sql("INSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "sqlOuterLimit cannot be provided with INSERT."
        )
        .verify();
  }
}
