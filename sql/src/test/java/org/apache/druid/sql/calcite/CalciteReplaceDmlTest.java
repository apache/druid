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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CalciteReplaceDmlTest extends CalciteIngestionDmlTest
{
  private static final Map<String, Object> REPLACE_ALL_TIME_CHUNKS = ImmutableMap.of(
      DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
      "{\"type\":\"all\"}",
      DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS,
      DruidSqlParserUtils.ALL
  );

  protected Map<String, Object> addReplaceTimeChunkToQueryContext(Map<String, Object> context, String replaceTimeChunks)
  {
    return ImmutableMap.<String, Object>builder()
                       .putAll(context)
                       .put(DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS, replaceTimeChunks)
                       .build();
  }

  @Test
  public void testReplaceFromTableWithReplaceAll()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableWithDeleteWhereClause()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00' "
             + "SELECT * FROM foo PARTITIONED BY DAY")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(
                    addReplaceTimeChunkToQueryContext(
                        queryContextWithGranularity(Granularities.DAY),
                        "2000-01-01T00:00:00.000Z/2000-01-02T00:00:00.000Z"
                    )
                )
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableWithTimeZoneInQueryContext()
  {
    HashMap<String, Object> context = new HashMap<>(DEFAULT_CONTEXT);
    context.put(PlannerContext.CTX_SQL_TIME_ZONE, "+05:30");
    testIngestionQuery()
        .context(context)
        .sql("REPLACE INTO dst OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01 05:30:00' AND __time < TIMESTAMP '2000-01-02 05:30:00' "
             + "SELECT * FROM foo PARTITIONED BY DAY")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(
                    addReplaceTimeChunkToQueryContext(
                        queryContextWithGranularity(Granularities.DAY),
                        "2000-01-01T00:00:00.000Z/2000-01-02T00:00:00.000Z"
                    )
                )
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableWithIntervalLargerThanOneGranularity()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE "
             + "__time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-05-01' "
             + "SELECT * FROM foo PARTITIONED BY MONTH")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(
                    addReplaceTimeChunkToQueryContext(
                        queryContextWithGranularity(Granularities.MONTH),
                        "2000-01-01T00:00:00.000Z/2000-05-01T00:00:00.000Z"
                    )
                )
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableWithComplexDeleteWhereClause()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE "
             + "__time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-02-01' "
             + "OR __time >= TIMESTAMP '2000-03-01' AND __time < TIMESTAMP '2000-04-01' "
             + "SELECT * FROM foo PARTITIONED BY MONTH")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(
                    addReplaceTimeChunkToQueryContext(
                        queryContextWithGranularity(Granularities.MONTH),
                        "2000-01-01T00:00:00.000Z/2000-02-01T00:00:00.000Z,2000-03-01T00:00:00.000Z/2000-04-01T00:00:00.000Z"
                    )
                )
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableWithBetweenClause()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE "
             + "__time BETWEEN TIMESTAMP '2000-01-01' AND TIMESTAMP '2000-01-31 23:59:59.999' "
             + "SELECT * FROM foo PARTITIONED BY MONTH")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(
                    addReplaceTimeChunkToQueryContext(
                        queryContextWithGranularity(Granularities.MONTH),
                        "2000-01-01T00:00:00.000Z/2000-02-01T00:00:00.000Z"
                    )
                )
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceForUnsupportedDeleteWhereClause()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE __time LIKE '20__-02-01' SELECT * FROM foo PARTITIONED BY MONTH")
        .expectValidationError(
            SqlPlanningException.class,
            "Unsupported operation in OVERWRITE WHERE clause: LIKE"
        )
        .verify();
  }

  @Test
  public void testReplaceForInvalidDeleteWhereClause()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE TRUE SELECT * FROM foo PARTITIONED BY MONTH")
        .expectValidationError(
            SqlPlanningException.class,
            "Invalid OVERWRITE WHERE clause"
        )
        .verify();
  }

  @Test
  public void testReplaceForDeleteWhereClauseOnUnsupportedColumns()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE dim1 > TIMESTAMP '2000-01-05 00:00:00' SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Only __time column is supported in OVERWRITE WHERE clause"
        )
        .verify();
  }


  @Test
  public void testReplaceWithOrderBy()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT * FROM foo ORDER BY dim1 PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "Cannot have ORDER BY on a REPLACE statement, use CLUSTERED BY instead.")
        .verify();
  }

  @Test
  public void testReplaceForMisalignedPartitionInterval()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE __time >= TIMESTAMP '2000-01-05 00:00:00' AND __time <= TIMESTAMP '2000-01-06 00:00:00' SELECT * FROM foo PARTITIONED BY MONTH")
        .expectValidationError(
            SqlPlanningException.class,
            "OVERWRITE WHERE clause contains an interval [2000-01-05T00:00:00.000Z/2000-01-06T00:00:00.001Z] which is not aligned with PARTITIONED BY granularity {type=period, period=P1M, timeZone=UTC, origin=null}"
        )
        .verify();
  }

  @Test
  public void testReplaceForInvalidPartition()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE __time >= TIMESTAMP '2000-01-05 00:00:00' AND __time <= TIMESTAMP '2000-02-05 00:00:00' SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "OVERWRITE WHERE clause contains an interval [2000-01-05T00:00:00.000Z/2000-02-05T00:00:00.001Z] which is not aligned with PARTITIONED BY granularity AllGranularity"
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableWithEmptyInterval()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE "
             + "__time < TIMESTAMP '2000-01-01' AND __time > TIMESTAMP '2000-01-01' "
             + "SELECT * FROM foo PARTITIONED BY MONTH")
        .expectValidationError(
            SqlPlanningException.class,
            "Intervals for replace are empty"
        )
        .verify();
  }

  @Test
  public void testReplaceForWithInvalidInterval()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE WHERE __time >= TIMESTAMP '2000-01-INVALID0:00' AND __time <= TIMESTAMP '2000-02-05 00:00:00' SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class)
        .verify();
  }

  @Test
  public void testReplaceForWithoutPartitionSpec()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class)
        .verify();
  }

  @Test
  public void testReplaceFromView()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT * FROM view.aview PARTITIONED BY ALL TIME")
        .expectTarget("dst", RowSignature.builder().add("dim1_firstchar", ColumnType.STRING).build())
        .expectResources(viewRead("aview"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING))
                .filters(selector("dim2", "a", null))
                .columns("v0")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceIntoQualifiedTable()
  {
    testIngestionQuery()
        .sql("REPLACE INTO druid.dst OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceContainingWithList()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL WITH foo_data AS (SELECT * FROM foo) SELECT dim1, dim3 FROM foo_data PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", RowSignature.builder()
                                         .add("dim1", ColumnType.STRING)
                                         .add("dim3", ColumnType.STRING)
                                         .build()
        )
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim3")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceIntoInvalidDataSourceName()
  {
    testIngestionQuery()
        .sql("REPLACE INTO \"in/valid\" OVERWRITE ALL SELECT dim1, dim2 FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "REPLACE dataSource cannot contain the '/' character.")
        .verify();
  }

  @Test
  public void testReplaceUsingColumnList()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst (foo, bar) OVERWRITE ALL SELECT dim1, dim2 FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "REPLACE with a target column list is not supported.")
        .verify();
  }

  @Test
  public void testReplaceWithoutPartitionedBy()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT __time, FLOOR(m1) as floor_m1, dim1 FROM foo")
        .expectValidationError(SqlPlanningException.class, "REPLACE statements must specify PARTITIONED BY clause explicitly")
        .verify();
  }

  @Test
  public void testReplaceWithoutPartitionedByWithClusteredBy()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT __time, FLOOR(m1) as floor_m1, dim1 FROM foo CLUSTERED BY dim1")
        .expectValidationError(SqlPlanningException.class, "CLUSTERED BY found before PARTITIONED BY. In Druid, the CLUSTERED BY clause must follow the PARTITIONED BY clause")
        .verify();
  }

  @Test
  public void testReplaceWithoutOverwriteClause()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "Missing time chunk information in OVERWRITE clause for REPLACE. Use OVERWRITE WHERE <__time based condition> or OVERWRITE ALL to overwrite the entire table.")
        .verify();
  }

  @Test
  public void testReplaceWithoutCompleteOverwriteClause()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "Missing time chunk information in OVERWRITE clause for REPLACE. Use OVERWRITE WHERE <__time based condition> or OVERWRITE ALL to overwrite the entire table.")
        .verify();
  }

  @Test
  public void testReplaceIntoSystemTable()
  {
    testIngestionQuery()
        .sql("REPLACE INTO INFORMATION_SCHEMA.COLUMNS OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot REPLACE into INFORMATION_SCHEMA.COLUMNS because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testReplaceIntoView()
  {
    testIngestionQuery()
        .sql("REPLACE INTO view.aview OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot REPLACE into view.aview because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testReplaceFromUnauthorizedDataSource()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT * FROM \"%s\" PARTITIONED BY ALL TIME", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testReplaceIntoUnauthorizedDataSource()
  {
    testIngestionQuery()
        .sql("REPLACE INTO \"%s\" OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testReplaceIntoNonexistentSchema()
  {
    testIngestionQuery()
        .sql("REPLACE INTO nonexistent.dst OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot REPLACE into nonexistent.dst because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testReplaceFromExternal()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceWithPartitionedByAndLimitOffset()
  {
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.FLOAT)
                                                  .add("dim1", ColumnType.STRING)
                                                  .build();

    testIngestionQuery()
        .sql(
            "REPLACE INTO druid.dst OVERWRITE ALL SELECT __time, FLOOR(m1) as floor_m1, dim1 FROM foo LIMIT 10 OFFSET 20 PARTITIONED BY DAY")
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
                .context(
                    addReplaceTimeChunkToQueryContext(
                        queryContextWithGranularity(Granularities.DAY),
                        DruidSqlParserUtils.ALL
                    )
                )
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceWithClusteredBy()
  {
    // Test correctness of the query when CLUSTERED BY clause is present
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.FLOAT)
                                                  .add("dim1", ColumnType.STRING)
                                                  .build();

    testIngestionQuery()
        .sql(
            "REPLACE INTO druid.dst OVERWRITE ALL SELECT __time, FLOOR(m1) as floor_m1, dim1 FROM foo PARTITIONED BY DAY CLUSTERED BY 2, dim1")
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
                .context(
                    addReplaceTimeChunkToQueryContext(
                        queryContextWithGranularity(Granularities.DAY),
                        DruidSqlParserUtils.ALL)
                )
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceWithPartitionedByContainingInvalidGranularity()
  {
    // Throws a ValidationException, which gets converted to a SqlPlanningException before throwing to end user
    try {
      testQuery(
          "REPLACE INTO dst OVERWRITE ALL SELECT * FROM foo PARTITIONED BY 'invalid_granularity'",
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assert.fail("Exception should be thrown");
    }
    catch (SqlPlanningException e) {
      assertEquals(
          "Encountered 'invalid_granularity' after PARTITIONED BY. Expected HOUR, DAY, MONTH, YEAR, ALL TIME, FLOOR function or TIME_FLOOR function",
          e.getMessage()
      );
    }
    didTest = true;
  }

  @Test
  public void testExplainReplaceFromExternal() throws IOException
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
                "{\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"dummy\",\"sqlReplaceTimeChunks\":\"all\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}",
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
            "EXPLAIN PLAN FOR REPLACE INTO dst OVERWRITE ALL SELECT * FROM %s PARTITIONED BY ALL TIME",
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
  public void testExplainReplaceFromExternalUnauthorized()
  {
    // Use testQuery for EXPLAIN (not testIngestionQuery).
    Assert.assertThrows(
        ForbiddenException.class,
        () ->
            testQuery(
                StringUtils.format(
                    "EXPLAIN PLAN FOR REPLACE INTO dst OVERWRITE ALL SELECT * FROM %s PARTITIONED BY ALL TIME",
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
  public void testReplaceFromExternalUnauthorized()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testReplaceFromExternalProjectSort()
  {
    testIngestionQuery()
        .sql(
            "REPLACE INTO dst OVERWRITE ALL SELECT x || y AS xy, z FROM %s PARTITIONED BY ALL TIME",
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
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromExternalAggregate()
  {
    testIngestionQuery()
        .sql(
            "REPLACE INTO dst OVERWRITE ALL SELECT x, SUM(z) AS sum_z, COUNT(*) AS cnt FROM %s GROUP BY 1 PARTITIONED BY ALL TIME",
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
                        .setContext(REPLACE_ALL_TIME_CHUNKS)
                        .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromExternalAggregateAll()
  {
    testIngestionQuery()
        .sql(
            "REPLACE INTO dst OVERWRITE ALL SELECT COUNT(*) AS cnt FROM %s PARTITIONED BY ALL TIME",
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
                        .setContext(REPLACE_ALL_TIME_CHUNKS)
                        .build()
        )
        .verify();
  }

  @Test
  public void testReplaceWithSqlOuterLimit()
  {
    HashMap<String, Object> context = new HashMap<>(DEFAULT_CONTEXT);
    context.put(PlannerContext.CTX_SQL_OUTER_LIMIT, 100);

    testIngestionQuery()
        .context(context)
        .sql("REPLACE INTO dst OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "sqlOuterLimit cannot be provided with REPLACE.")
        .verify();
  }
}
