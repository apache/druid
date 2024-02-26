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
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
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
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.druid.segment.column.ColumnType.DOUBLE;
import static org.apache.druid.segment.column.ColumnType.FLOAT;
import static org.apache.druid.segment.column.ColumnType.LONG;
import static org.apache.druid.segment.column.ColumnType.STRING;

public class CalciteInsertDmlTest extends CalciteIngestionDmlTest
{
  protected static final Map<String, Object> PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT = ImmutableMap.of(
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
                .filters(equality("dim2", "a", ColumnType.STRING))
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
                                .filters(equality("dim2", "a", ColumnType.STRING))
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
        .expectValidationError(
            DruidExceptionMatcher.invalidInput().expectMessageIs(
                "Invalid value for field [table]: Value [in/valid] cannot contain '/'."
            )
        )
        .verify();
  }

  @Test
  public void testInsertUsingColumnList()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst (foo, bar) SELECT dim1, dim2 FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            invalidSqlIs("Operation [INSERT] cannot be run with a target column list, given [dst (`foo`, `bar`)]")
        )
        .verify();
  }

  @Test
  public void testUpsert()
  {
    testIngestionQuery()
        .sql("UPSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(invalidSqlIs("UPSERT is not supported."))
        .verify();
  }

  @Test
  public void testSelectFromSystemTable()
  {
    // TestInsertSqlEngine does not include ALLOW_BINDABLE_PLAN, so cannot query system tables.

    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM INFORMATION_SCHEMA.COLUMNS PARTITIONED BY ALL TIME")
        .expectValidationError(
            DruidException.class,
            "Cannot query table(s) [INFORMATION_SCHEMA.COLUMNS] with SQL engine [ingestion-test]"
        )
        .verify();
  }

  @Test
  public void testInsertIntoSystemTable()
  {
    testIngestionQuery()
        .sql("INSERT INTO INFORMATION_SCHEMA.COLUMNS SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(invalidSqlIs(
            "Table [INFORMATION_SCHEMA.COLUMNS] does not support operation [INSERT] because it is not a Druid datasource"
        ))
        .verify();
  }

  @Test
  public void testInsertIntoView()
  {
    testIngestionQuery()
        .sql("INSERT INTO view.aview SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            invalidSqlIs("Table [view.aview] does not support operation [INSERT] because it is not a Druid datasource")
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
        .expectValidationError(invalidSqlIs(
            "Table [nonexistent.dst] does not support operation [INSERT] because it is not a Druid datasource"
        ))
        .verify();
  }

  @Test
  public void testInsertFromExternal()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  @Test
  public void testInsertFromExternalWithInputSourceSecurityEnabled()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .authConfig(AuthConfig.newBuilder().setEnableInputSourceSecurity(true).build())
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), externalRead("inline"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  @Test
  public void testUnauthorizedInsertFromExternalWithInputSourceSecurityEnabled()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .authentication(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .authConfig(AuthConfig.newBuilder().setEnableInputSourceSecurity(true).build())
        .expectLogicalPlanFrom("insertFromExternal")
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testInsertFromExternalWithSchema()
  {
    String extern;
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      extern = StringUtils.format(
          "TABLE(extern(%s, %s))",
          Calcites.escapeStringLiteral(
              queryJsonMapper.writeValueAsString(
                  new InlineInputSource("a,b,1\nc,d,2\n")
              )
          ),
          Calcites.escapeStringLiteral(
              queryJsonMapper.writeValueAsString(
                  new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0)
              )
          )
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    testIngestionQuery()
        .sql(
            "INSERT INTO dst SELECT * FROM %s\n" +
            "  (x VARCHAR, y VARCHAR, z BIGINT)\n" +
            "PARTITIONED BY ALL TIME",
            extern
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  @Test
  public void testInsertFromExternalWithSchemaWithInputsourceSecurity()
  {
    String extern;
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      extern = StringUtils.format(
          "TABLE(extern(%s, %s))",
          Calcites.escapeStringLiteral(
              queryJsonMapper.writeValueAsString(
                  new InlineInputSource("a,b,1\nc,d,2\n")
              )
          ),
          Calcites.escapeStringLiteral(
              queryJsonMapper.writeValueAsString(
                  new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0)
              )
          )
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s\n" +
             "  (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME",
             extern
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .authConfig(AuthConfig.newBuilder().setEnableInputSourceSecurity(true).build())
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), externalRead("inline"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  @Test
  public void testInsertFromExternalFunctionalStyleWithSchemaWithInputsourceSecurity()
  {
    String extern;
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      extern = StringUtils.format(
          "TABLE(extern("
          + "inputSource => '%s',"
          + "inputFormat => '%s'))",
          queryJsonMapper.writeValueAsString(
              new InlineInputSource("a,b,1\nc,d,2\n")
          ),
          queryJsonMapper.writeValueAsString(
              new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0)
          )
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s\n" +
             "  (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME",
             extern
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .authConfig(AuthConfig.newBuilder().setEnableInputSourceSecurity(true).build())
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), externalRead("inline"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  @Test
  public void testInsertFromExternalWithoutSecuritySupport()
  {
    InputSource inputSource =
        new TestFileInputSource(ImmutableList.of(new File("/tmp/foo.csv").getAbsoluteFile()));
    final ExternalDataSource externalDataSource = new ExternalDataSource(
        inputSource,
        new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0),
        RowSignature.builder()
                    .add("x", ColumnType.STRING)
                    .add("y", ColumnType.STRING)
                    .add("z", ColumnType.LONG)
                    .build()
    );
    String extern;
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      extern = StringUtils.format(
          "TABLE(extern("
          + "inputSource => '%s',"
          + "inputFormat => '%s'))",
          queryJsonMapper.writeValueAsString(inputSource),
          queryJsonMapper.writeValueAsString(
              new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0)
          )
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s\n" +
             "  (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME",
             extern
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .authConfig(AuthConfig.newBuilder().setEnableInputSourceSecurity(false).build())
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("InsertFromExternalWithoutSecuritySupport")
        .verify();
  }

  @Test
  public void testInsertFromExternalWithoutSecuritySupportWithInputsourceSecurityEnabled()
  {
    String extern;
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      extern = StringUtils.format(
          "TABLE(extern("
          + "inputSource => '%s',"
          + "inputFormat => '%s'))",
          queryJsonMapper.writeValueAsString(
              new TestFileInputSource(ImmutableList.of(new File("/tmp/foo.csv").getAbsoluteFile()))),
          queryJsonMapper.writeValueAsString(
              new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0)
          )
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s\n" +
             "  (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME",
             extern
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .authConfig(AuthConfig.newBuilder().setEnableInputSourceSecurity(true).build())
        .expectLogicalPlanFrom("insertFromExternal")
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(CalciteIngestDmlTestException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("getTypes()"))
            )
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
        .expectLogicalPlanFrom("insertWithPartitionedBy")
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
  public void testPartitionedBySupportedGranularityLiteralClauses()
  {
    final RowSignature targetRowSignature = RowSignature.builder()
                                                        .add("__time", ColumnType.LONG)
                                                        .add("dim1", ColumnType.STRING)
                                                        .build();

    final Map<String, Granularity> partitionedByToGranularity =
        Arrays.stream(GranularityType.values())
              .collect(Collectors.toMap(GranularityType::name, GranularityType::getDefaultGranularity));

    final ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    partitionedByToGranularity.forEach((partitionedByArgument, expectedGranularity) -> {
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
              "INSERT INTO druid.dst SELECT __time, dim1 FROM foo PARTITIONED BY '%s'",
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
  public void testExplainPlanInsertWithClusteredBy() throws JsonProcessingException
  {
    skipVectorize();

    final String resources = "[{\"name\":\"dst\",\"type\":\"DATASOURCE\"},{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"INSERT\",\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"dst\"},\"partitionedBy\":\"DAY\",\"clusteredBy\":[\"floor_m1\",\"dim1\",\"CEIL(\\\"m2\\\")\"]}";

    final String sql = "EXPLAIN PLAN FOR INSERT INTO druid.dst "
                       + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo "
                       + "PARTITIONED BY FLOOR(__time TO DAY) CLUSTERED BY 2, dim1, CEIL(m2)";

    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    final ScanQuery expectedQuery = newScanQueryBuilder()
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
                new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING),
                new ScanQuery.OrderBy("v1", ScanQuery.Order.ASCENDING)
            )
        )
        .context(
            queryJsonMapper.readValue(
                "{\"sqlInsertSegmentGranularity\":\"\\\"DAY\\\"\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}",
                JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
            )
        )
        .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.FLOAT, ColumnType.DOUBLE)
        .build();

    final String legacyExplanation =
        "DruidQueryRel(query=["
        + queryJsonMapper.writeValueAsString(expectedQuery)
        + "], signature=[{__time:LONG, v0:FLOAT, dim1:STRING, v1:DOUBLE}])\n";


    // Use testQuery for EXPLAIN (not testIngestionQuery).
    testQuery(
        PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        sql,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    legacyExplanation,
                    resources,
                    attributes
                }
            ),
            null
        ),
        null
    );

    // Test correctness of the query when only the CLUSTERED BY clause is present
    final String explanation =
        "["
        + "{\"query\":{\"queryType\":\"scan\","
        + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
        + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\",\"expression\":\"floor(\\\"m1\\\")\",\"outputType\":\"FLOAT\"},"
        + "{\"type\":\"expression\",\"name\":\"v1\",\"expression\":\"ceil(\\\"m2\\\")\",\"outputType\":\"DOUBLE\"}],"
        + "\"resultFormat\":\"compactedList\","
        + "\"orderBy\":[{\"columnName\":\"v0\",\"order\":\"ascending\"},{\"columnName\":\"dim1\",\"order\":\"ascending\"},"
        + "{\"columnName\":\"v1\",\"order\":\"ascending\"}],\"columns\":[\"__time\",\"dim1\",\"v0\",\"v1\"],\"legacy\":false,"
        + "\"context\":{\"sqlInsertSegmentGranularity\":\"\\\"DAY\\\"\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"LONG\",\"STRING\",\"FLOAT\",\"DOUBLE\"],\"granularity\":{\"type\":\"all\"}},"
        + "\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"v0\",\"type\":\"FLOAT\"},{\"name\":\"dim1\",\"type\":\"STRING\"},"
        + "{\"name\":\"v1\",\"type\":\"DOUBLE\"}],"
        + "\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"v0\",\"outputColumn\":\"floor_m1\"},"
        + "{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"},{\"queryColumn\":\"v1\",\"outputColumn\":\"ceil_m2\"}]"
        + "}]";

    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        sql,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    explanation,
                    resources,
                    attributes
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
  public void testExplainPlanInsertWithAsSubQueryClusteredBy()
  {
    skipVectorize();

    final String resources = "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"INSERT\",\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"foo\"},\"partitionedBy\":{\"type\":\"all\"},\"clusteredBy\":[\"namespace\",\"country\"]}";

    final String sql = "EXPLAIN PLAN FOR\n"
                       + "INSERT INTO \"foo\"\n"
                       + "WITH dd AS (\n"
                       + "SELECT * FROM TABLE(\n"
                       + "  EXTERN(\n"
                       + "    '{\"type\":\"inline\",\"data\":\"{\\\" \\\": 1681794225551, \\\"namespace\\\": \\\"day1\\\", \\\"country\\\": \\\"one\\\"}\\n{\\\"__time\\\": 1681794225558, \\\"namespace\\\": \\\"day2\\\", \\\"country\\\": \\\"two\\\"}\"}',\n"
                       + "    '{\"type\":\"json\"}',\n"
                       + "    '[{\"name\":\"__time\",\"type\":\"long\"},{\"name\":\"namespace\",\"type\":\"string\"},{\"name\":\"country\",\"type\":\"string\"}]'\n"
                       + "  )\n"
                       + "))\n"
                       + "\n"
                       + "SELECT\n"
                       + " __time,\n"
                       + "  namespace,\n"
                       + "  country\n"
                       + "FROM dd\n"
                       + "PARTITIONED BY ALL\n"
                       + "CLUSTERED BY 2, 3";

    final String legacyExplanation = "DruidQueryRel("
                                     + "query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"external\","
                                     + "\"inputSource\":{\"type\":\"inline\",\"data\":\"{\\\" \\\": 1681794225551, \\\"namespace\\\": \\\"day1\\\", \\\"country\\\": \\\"one\\\"}\\n"
                                     + "{\\\"__time\\\": 1681794225558, \\\"namespace\\\": \\\"day2\\\", \\\"country\\\": \\\"two\\\"}\"},"
                                     + "\"inputFormat\":{\"type\":\"json\",\"keepNullColumns\":false,\"assumeNewlineDelimited\":false,\"useJsonNodeReader\":false},"
                                     + "\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"namespace\",\"type\":\"STRING\"},{\"name\":\"country\",\"type\":\"STRING\"}]},"
                                     + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                     + "\"resultFormat\":\"compactedList\",\"orderBy\":[{\"columnName\":\"namespace\",\"order\":\"ascending\"},{\"columnName\":\"country\",\"order\":\"ascending\"}],"
                                     + "\"columns\":[\"__time\",\"country\",\"namespace\"],\"legacy\":false,\"context\":{\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\","
                                     + "\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"LONG\",\"STRING\",\"STRING\"],\"granularity\":{\"type\":\"all\"}}],"
                                     + " signature=[{__time:LONG, namespace:STRING, country:STRING}])\n";

    // Use testQuery for EXPLAIN (not testIngestionQuery).
    testQuery(
        PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        sql,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    legacyExplanation,
                    resources,
                    attributes
                }
            ),
            null
        ),
        null
    );

    // Test correctness of the query when only the CLUSTERED BY clause is present
    final String explanation = "[{\"query\":{\"queryType\":\"scan\"," + "\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"inline\","
                               + "\"data\":\"{\\\" \\\": 1681794225551, \\\"namespace\\\": \\\"day1\\\", \\\"country\\\": \\\"one\\\"}\\n"
                               + "{\\\"__time\\\": 1681794225558, \\\"namespace\\\": \\\"day2\\\", \\\"country\\\": \\\"two\\\"}\"},"
                               + "\"inputFormat\":{\"type\":\"json\",\"keepNullColumns\":false,\"assumeNewlineDelimited\":false,\"useJsonNodeReader\":false},"
                               + "\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"namespace\",\"type\":\"STRING\"},{\"name\":\"country\",\"type\":\"STRING\"}]},"
                               + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                               + "\"resultFormat\":\"compactedList\",\"orderBy\":[{\"columnName\":\"namespace\",\"order\":\"ascending\"},{\"columnName\":\"country\",\"order\":\"ascending\"}],"
                               + "\"columns\":[\"__time\",\"country\",\"namespace\"],\"legacy\":false,\"context\":{\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\","
                               + "\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"LONG\",\"STRING\",\"STRING\"],\"granularity\":{\"type\":\"all\"}},"
                               + "\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"namespace\",\"type\":\"STRING\"},{\"name\":\"country\",\"type\":\"STRING\"}],"
                               + "\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"namespace\",\"outputColumn\":\"namespace\"},"
                               + "{\"queryColumn\":\"country\",\"outputColumn\":\"country\"}]}]";

    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        sql,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    explanation,
                    resources,
                    attributes
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
  public void testExplainPlanInsertJoinQuery()
  {
    skipVectorize();

    final String resources = "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"my_table\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"INSERT\",\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"my_table\"},\"partitionedBy\":\"HOUR\",\"clusteredBy\":[\"__time\",\"isRobotAlias\",\"countryCapital\",\"regionName\"]}";

    final String sql = "EXPLAIN PLAN FOR\n"
                       + "INSERT INTO my_table\n"
                       + "WITH\n"
                       + "wikidata AS (SELECT * FROM TABLE(\n"
                       + "  EXTERN(\n"
                       + "    '{\"type\":\"http\",\"uris\":[\"https://boo.gz\"]}',\n"
                       + "    '{\"type\":\"json\"}',\n"
                       + "    '[{\"name\":\"isRobot\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"cityName\",\"type\":\"string\"},{\"name\":\"countryIsoCode\",\"type\":\"string\"},{\"name\":\"regionName\",\"type\":\"string\"}]'\n"
                       + "  )\n"
                       + ")),\n"
                       + "countries AS (SELECT * FROM TABLE(\n"
                       + "  EXTERN(\n"
                       + "    '{\"type\":\"http\",\"uris\":[\"https://foo.tsv\"]}',\n"
                       + "    '{\"type\":\"tsv\",\"findColumnsFromHeader\":true}',\n"
                       + "    '[{\"name\":\"Country\",\"type\":\"string\"},{\"name\":\"Capital\",\"type\":\"string\"},"
                       + "{\"name\":\"ISO3\",\"type\":\"string\"},{\"name\":\"ISO2\",\"type\":\"string\"}]'\n"
                       + "  )\n"
                       + "))\n"
                       + "SELECT\n"
                       + "  TIME_PARSE(\"timestamp\") AS __time,\n"
                       + "  isRobot AS isRobotAlias,\n"
                       + "  countries.Capital AS countryCapital,\n"
                       + "  regionName\n"
                       + "FROM wikidata\n"
                       + "LEFT JOIN countries ON wikidata.countryIsoCode = countries.ISO2\n"
                       + "PARTITIONED BY HOUR\n"
                       + "CLUSTERED BY 1, 2, 3, regionName";

    final String legacyExplanation = "DruidJoinQueryRel(condition=[=($3, $6)], joinType=[left], query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"__join__\"},"
                                     + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\","
                                     + "\"expression\":\"timestamp_parse(\\\"timestamp\\\",null,'UTC')\",\"outputType\":\"LONG\"}],\"resultFormat\":\"compactedList\",\"orderBy\":[{\"columnName\":\"v0\",\"order\":\"ascending\"},{\"columnName\":\"isRobot\",\"order\":\"ascending\"},"
                                     + "{\"columnName\":\"Capital\",\"order\":\"ascending\"},{\"columnName\":\"regionName\",\"order\":\"ascending\"}],\"columns\":[\"Capital\",\"isRobot\",\"regionName\",\"v0\"],\"legacy\":false,\"context\":{\"sqlInsertSegmentGranularity\":\"\\\"HOUR\\\"\",\"sqlQueryId\":\"dummy\","
                                     + "\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\",\"STRING\",\"LONG\"],\"granularity\":{\"type\":\"all\"}}], signature=[{v0:LONG, isRobot:STRING, Capital:STRING, regionName:STRING}])\n"
                                     + "  DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"http\",\"uris\":[\"https://boo.gz\"]},\"inputFormat\":{\"type\":\"json\",\"keepNullColumns\":false,\"assumeNewlineDelimited\":false,"
                                     + "\"useJsonNodeReader\":false},\"signature\":[{\"name\":\"isRobot\",\"type\":\"STRING\"},{\"name\":\"timestamp\",\"type\":\"STRING\"},{\"name\":\"cityName\",\"type\":\"STRING\"},{\"name\":\"countryIsoCode\",\"type\":\"STRING\"},{\"name\":\"regionName\",\"type\":\"STRING\"}]},"
                                     + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"cityName\",\"countryIsoCode\",\"isRobot\",\"regionName\",\"timestamp\"],\"legacy\":false,"
                                     + "\"context\":{\"sqlInsertSegmentGranularity\":\"\\\"HOUR\\\"\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\"],\"granularity\":{\"type\":\"all\"}}], signature=[{isRobot:STRING, timestamp:STRING, cityName:STRING, countryIsoCode:STRING, regionName:STRING}])\n"
                                     + "  DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"http\",\"uris\":[\"https://foo.tsv\"]},\"inputFormat\":{\"type\":\"tsv\",\"delimiter\":\"\\t\",\"findColumnsFromHeader\":true},"
                                     + "\"signature\":[{\"name\":\"Country\",\"type\":\"STRING\"},{\"name\":\"Capital\",\"type\":\"STRING\"},{\"name\":\"ISO3\",\"type\":\"STRING\"},{\"name\":\"ISO2\",\"type\":\"STRING\"}]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                     + "\"resultFormat\":\"compactedList\",\"columns\":[\"Capital\",\"ISO2\"],\"legacy\":false,\"context\":{\"sqlInsertSegmentGranularity\":\"\\\"HOUR\\\"\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\"],\"granularity\":{\"type\":\"all\"}}], signature=[{Capital:STRING, ISO2:STRING}])\n";
    // Use testQuery for EXPLAIN (not testIngestionQuery).
    testQuery(
        PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        sql,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    legacyExplanation,
                    resources,
                    attributes
                }
            ),
            null
        ),
        null
    );

    // Test correctness of the query when only the CLUSTERED BY clause is present
    final String explanation = "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"join\",\"left\":{\"type\":\"external\",\"inputSource\":{\"type\":\"http\",\"uris\":[\"https://boo.gz\"]},\"inputFormat\":{\"type\":\"json\",\"keepNullColumns\":false,"
                               + "\"assumeNewlineDelimited\":false,\"useJsonNodeReader\":false},\"signature\":[{\"name\":\"isRobot\",\"type\":\"STRING\"},{\"name\":\"timestamp\",\"type\":\"STRING\"},{\"name\":\"cityName\",\"type\":\"STRING\"},{\"name\":\"countryIsoCode\",\"type\":\"STRING\"},"
                               + "{\"name\":\"regionName\",\"type\":\"STRING\"}]},\"right\":{\"type\":\"query\",\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"http\",\"uris\":[\"https://foo.tsv\"]},\"inputFormat\":{\"type\":\"tsv\",\"delimiter\":\"\\t\",\"findColumnsFromHeader\":true},"
                               + "\"signature\":[{\"name\":\"Country\",\"type\":\"STRING\"},{\"name\":\"Capital\",\"type\":\"STRING\"},{\"name\":\"ISO3\",\"type\":\"STRING\"},{\"name\":\"ISO2\",\"type\":\"STRING\"}]},\"intervals\":{\"type\":\"intervals\","
                               + "\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"Capital\",\"ISO2\"],\"legacy\":false,\"context\":{\"sqlInsertSegmentGranularity\":\"\\\"HOUR\\\"\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\","
                               + "\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\"],\"granularity\":{\"type\":\"all\"}}},\"rightPrefix\":\"j0.\",\"condition\":\"(\\\"countryIsoCode\\\" == \\\"j0.ISO2\\\")\",\"joinType\":\"LEFT\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                               + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\",\"expression\":\"timestamp_parse(\\\"timestamp\\\",null,'UTC')\",\"outputType\":\"LONG\"}],\"resultFormat\":\"compactedList\",\"orderBy\":[{\"columnName\":\"v0\",\"order\":\"ascending\"},{\"columnName\":\"isRobot\",\"order\":\"ascending\"},"
                               + "{\"columnName\":\"j0.Capital\",\"order\":\"ascending\"},{\"columnName\":\"regionName\",\"order\":\"ascending\"}],\"columns\":[\"isRobot\",\"j0.Capital\",\"regionName\",\"v0\"],\"legacy\":false,\"context\":{\"sqlInsertSegmentGranularity\":\"\\\"HOUR\\\"\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\","
                               + "\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\",\"STRING\",\"LONG\"],\"granularity\":{\"type\":\"all\"}},\"signature\":[{\"name\":\"v0\",\"type\":\"LONG\"},{\"name\":\"isRobot\",\"type\":\"STRING\"},{\"name\":\"j0.Capital\",\"type\":\"STRING\"},{\"name\":\"regionName\",\"type\":\"STRING\"}],\"columnMappings\":[{\"queryColumn\":\"v0\",\"outputColumn\":\"__time\"},"
                               + "{\"queryColumn\":\"isRobot\",\"outputColumn\":\"isRobotAlias\"},{\"queryColumn\":\"j0.Capital\",\"outputColumn\":\"countryCapital\"},{\"queryColumn\":\"regionName\",\"outputColumn\":\"regionName\"}]}]";

    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        sql,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    explanation,
                    resources,
                    attributes
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
  public void testExplainPlanInsertWithClusteredByDescThrowsException()
  {
    skipVectorize();

    final String sql = "EXPLAIN PLAN FOR INSERT INTO druid.dst "
                       + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo "
                       + "PARTITIONED BY FLOOR(__time TO DAY) CLUSTERED BY 2, dim1 DESC, CEIL(m2)";

    testIngestionQuery()
        .sql(sql)
        .expectValidationError(
            invalidSqlIs("Invalid CLUSTERED BY clause [`dim1` DESC]: cannot sort in descending order.")
        )
        .verify();
  }

  @Test
  public void testInsertWithClusteredBy()
  {
    // Test correctness of the query when only the CLUSTERED BY clause is present
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
            + "PARTITIONED BY FLOOR(__time TO DAY) CLUSTERED BY 2, dim1, CEIL(m2)"
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
                        new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING),
                        new ScanQuery.OrderBy("v1", ScanQuery.Order.ASCENDING)
                    )
                )
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .expectLogicalPlanFrom("insertWithClusteredBy")
        .verify();
  }

  @Test
  public void testInsertPeriodFormGranularityWithClusteredBy()
  {
    // Test correctness of the query when only the CLUSTERED BY clause is present
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
            + "PARTITIONED BY P1D CLUSTERED BY 2, dim1, CEIL(m2)"
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
                        new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING),
                        new ScanQuery.OrderBy("v1", ScanQuery.Order.ASCENDING)
                    )
                )
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .expectLogicalPlanFrom("insertPartitionedByP1DWithClusteredBy")
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
        .expectValidationError(invalidSqlIs(
            "CLUSTERED BY found before PARTITIONED BY, CLUSTERED BY must come after the PARTITIONED BY clause"
        ))
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
    catch (DruidException e) {
      MatcherAssert.assertThat(e, invalidSqlIs(
          "Cannot use an ORDER BY clause on a Query of type [INSERT], use CLUSTERED BY instead"
      ));
    }
    didTest = true;
  }

  @Test
  public void testInsertWithPartitionedByContainingInvalidGranularity()
  {
    try {
      testQuery(
          "INSERT INTO dst SELECT * FROM foo PARTITIONED BY 'invalid_granularity'",
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assert.fail("Exception should be thrown");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          invalidSqlIs(
              "Invalid granularity['invalid_granularity'] specified after PARTITIONED BY clause."
              + " Expected 'SECOND', 'MINUTE', 'FIVE_MINUTE', 'TEN_MINUTE', 'FIFTEEN_MINUTE', 'THIRTY_MINUTE', 'HOUR',"
              + " 'SIX_HOUR', 'EIGHT_HOUR', 'DAY', 'MONTH', 'QUARTER', 'YEAR', 'ALL', ALL TIME, FLOOR()"
              + " or TIME_FLOOR()"
          ));
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
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          invalidSqlIs("Cannot use an ORDER BY clause on a Query of type [INSERT], use CLUSTERED BY instead")
      );
    }
    finally {
      didTest = true;
    }
  }

  @Test
  public void testInsertWithoutPartitionedBy()
  {
    DruidException e = Assert.assertThrows(
        DruidException.class,
        () ->
            testQuery(
                StringUtils.format("INSERT INTO dst SELECT * FROM %s", externSql(externalDataSource)),
                ImmutableList.of(),
                ImmutableList.of()
            )
    );

    MatcherAssert.assertThat(
        e,
        invalidSqlIs("Operation [INSERT] requires a PARTITIONED BY to be explicitly defined, but none was found.")
    );
    didTest = true;
  }

  @Test
  public void testExplainInsertFromExternal() throws IOException
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String query = StringUtils.format(
        "EXPLAIN PLAN FOR INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME",
        externSql(externalDataSource)
    );

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
        .columnTypes(STRING, STRING, LONG)
        .build();

    final String legacyExplanation =
        "DruidQueryRel(query=["
        + queryJsonMapper.writeValueAsString(expectedQuery)
        + "], signature=[{x:STRING, y:STRING, z:LONG}])\n";

    final String explanation =
        "["
        + "{\"query\":{\"queryType\":\"scan\","
        + "\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"inline\",\"data\":\"a,b,1\\nc,d,2\\n\"},"
        + "\"inputFormat\":{\"type\":\"csv\",\"columns\":[\"x\",\"y\",\"z\"]},"
        + "\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}]},"
        + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
        + "\"resultFormat\":\"compactedList\",\"columns\":[\"x\",\"y\",\"z\"],\"legacy\":false,"
        + "\"context\":{\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},"
        + "\"columnTypes\":[\"STRING\",\"STRING\",\"LONG\"],\"granularity\":{\"type\":\"all\"}},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}],"
        + "\"columnMappings\":[{\"queryColumn\":\"x\",\"outputColumn\":\"x\"},{\"queryColumn\":\"y\",\"outputColumn\":\"y\"},{\"queryColumn\":\"z\",\"outputColumn\":\"z\"}]"
        + "}]";

    final String resources = "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"dst\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"INSERT\",\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"dst\"},\"partitionedBy\":{\"type\":\"all\"}}";

    // Use testQuery for EXPLAIN (not testIngestionQuery).
    testQuery(
        PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        query,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    legacyExplanation,
                    resources,
                    attributes
                }
            ),
            null
        ),
        null
    );


    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        query,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    explanation,
                    resources,
                    attributes
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
  public void testExplainPlanForInsertWithClusteredBy() throws JsonProcessingException
  {
    skipVectorize();

    final String query = "EXPLAIN PLAN FOR INSERT INTO druid.dst "
                       + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo "
                       + "PARTITIONED BY FLOOR(__time TO DAY) CLUSTERED BY 2, dim1 ASC, CEIL(m2)";

    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    final ScanQuery expectedQuery = newScanQueryBuilder()
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
                new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING),
                new ScanQuery.OrderBy("v1", ScanQuery.Order.ASCENDING)
            )
        )
        .context(
            queryJsonMapper.readValue(
                "{\"sqlInsertSegmentGranularity\":\"\\\"DAY\\\"\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}",
                JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
            )
        )
        .columnTypes(LONG, STRING, FLOAT, DOUBLE)
        .build();


    final String legacyExplanation =
        "DruidQueryRel(query=["
        + queryJsonMapper.writeValueAsString(expectedQuery)
        + "], signature=[{__time:LONG, v0:FLOAT, dim1:STRING, v1:DOUBLE}])\n";

    final String explanation =
        "["
        + "{\"query\":{\"queryType\":\"scan\","
        + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
        + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\",\"expression\":\"floor(\\\"m1\\\")\",\"outputType\":\"FLOAT\"},"
        + "{\"type\":\"expression\",\"name\":\"v1\",\"expression\":\"ceil(\\\"m2\\\")\",\"outputType\":\"DOUBLE\"}],"
        + "\"resultFormat\":\"compactedList\","
        + "\"orderBy\":[{\"columnName\":\"v0\",\"order\":\"ascending\"},{\"columnName\":\"dim1\",\"order\":\"ascending\"},"
        + "{\"columnName\":\"v1\",\"order\":\"ascending\"}],\"columns\":[\"__time\",\"dim1\",\"v0\",\"v1\"],\"legacy\":false,"
        + "\"context\":{\"sqlInsertSegmentGranularity\":\"\\\"DAY\\\"\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"LONG\",\"STRING\",\"FLOAT\",\"DOUBLE\"],\"granularity\":{\"type\":\"all\"}},"
        + "\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"v0\",\"type\":\"FLOAT\"},{\"name\":\"dim1\",\"type\":\"STRING\"},"
        + "{\"name\":\"v1\",\"type\":\"DOUBLE\"}],"
        + "\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"v0\",\"outputColumn\":\"floor_m1\"},"
        + "{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"},{\"queryColumn\":\"v1\",\"outputColumn\":\"ceil_m2\"}]"
        + "}]";

    final String resources = "[{\"name\":\"dst\",\"type\":\"DATASOURCE\"},{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"INSERT\",\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"dst\"},\"partitionedBy\":\"DAY\",\"clusteredBy\":[\"floor_m1\",\"dim1\",\"CEIL(\\\"m2\\\")\"]}";

    // Use testQuery for EXPLAIN (not testIngestionQuery).
    testQuery(
        PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        query,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    legacyExplanation,
                    resources,
                    attributes
                }
            ),
            null
        ),
        null
    );

    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        query,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        new DefaultResultsVerifier(
            ImmutableList.of(
                new Object[]{
                    explanation,
                    resources,
                    attributes
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
        "SQL requires 'UNION' but only 'UNION ALL' is supported."
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
  public void testInsertWithOverwriteClause()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(DruidException.class, "An OVERWRITE clause is not allowed with INSERT statements. Use REPLACE statements if overwriting existing segments is required or remove the OVERWRITE clause.")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
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
    // This test fails because "count" is a reserved word and it is being used without quotes.  So SQL is considering
    // it a token instead of a name.  It would be nice if our message was more direct telling the person that they
    // used a reserved word instead of making them know that a "token" means Calcite is seeing a reserved word.  But,
    // that's an improvement for another day.
    testIngestionQuery()
        .sql("INSERT INTO t SELECT channel, added as count FROM foo PARTITIONED BY ALL") // count is a keyword
        .expectValidationError(invalidSqlContains("Received an unexpected token [as count]"))
        .verify();
  }

  @Test
  public void testInsertWithUnnamedColumnInSelectStatement()
  {
    testIngestionQuery()
        .sql("INSERT INTO t SELECT dim1, dim2 || '-lol' FROM foo PARTITIONED BY ALL")
        .expectValidationError(invalidSqlContains("Insertion requires columns to be named"))
        .verify();
  }

  @Test
  public void testInsertWithInvalidColumnNameInIngest()
  {
    testIngestionQuery()
        .sql("INSERT INTO t SELECT __time, dim1 AS EXPR$0 FROM foo PARTITIONED BY ALL")
        .expectValidationError(invalidSqlContains("Insertion requires columns to be named"))
        .verify();
  }

  @Test
  public void testInsertWithUnnamedColumnInNestedSelectStatement()
  {
    testIngestionQuery()
        .sql("INSERT INTO test "
             + "SELECT __time, * FROM "
             + "(SELECT __time, LOWER(dim1) FROM foo) PARTITIONED BY ALL TIME")
        .expectValidationError(invalidSqlContains("Insertion requires columns to be named"))
        .verify();
  }

  @Test
  public void testInsertQueryWithInvalidGranularity()
  {
    testIngestionQuery()
        .sql("insert into foo1 select __time, dim1 FROM foo partitioned by time_floor(__time, 'PT2H')")
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(DruidException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                    "Invalid granularity[`time_floor`(`__time`, 'PT2H')] specified after PARTITIONED BY clause."
                    + " Expected 'SECOND', 'MINUTE', 'FIVE_MINUTE', 'TEN_MINUTE', 'FIFTEEN_MINUTE', 'THIRTY_MINUTE',"
                    + " 'HOUR', 'SIX_HOUR', 'EIGHT_HOUR', 'DAY', 'MONTH', 'QUARTER', 'YEAR', 'ALL',"
                    + " ALL TIME, FLOOR() or TIME_FLOOR()"))
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
                CoreMatchers.instanceOf(DruidException.class),
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
            invalidSqlIs("Context parameter [sqlOuterLimit] cannot be provided on operator [INSERT]")
        )
        .verify();
  }

  @Test
  public void testErrorWithUnableToConstructColumnSignatureWithExtern()
  {
    final String sqlString = "insert into dst \n"
                             + "select time_parse(\"time\") as __time, * \n"
                             + "from table( \n"
                             + "extern(\n"
                             + "'{\"type\": \"s3\", \"uris\": [\\\"s3://imply-eng-datasets/qa/IngestionTest/wikipedia/files/wikiticker-2015-09-12-sampled.mini.json.gz\\\"]}',\n"
                             + "'{\"type\": \"json\"}',\n"
                             + "'[{\"name\": \"time\", \"type\": \"string\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"countryName\": \"string\"}]'\n"
                             + ")\n"
                             + ")\n"
                             + "partitioned by DAY\n"
                             + "clustered by channel";
    HashMap<String, Object> context = new HashMap<>(DEFAULT_CONTEXT);
    testIngestionQuery().context(context).sql(sqlString)
                        .expectValidationError(
                            new DruidExceptionMatcher(
                                DruidException.Persona.USER,
                                DruidException.Category.INVALID_INPUT,
                                "invalidInput"
                            ).expectMessageContains(
                                "Cannot construct instance of `org.apache.druid.segment.column.ColumnSignature`, problem: Column name must be provided and non-empty"
                            )
                        )
                        .verify();
  }

  @Test
  public void testErrorWhenBothRowSignatureAndExtendsProvidedToExtern()
  {
    final String sqlString = "insert into dst \n"
                             + "select time_parse(\"time\") as __time, * \n"
                             + "from table( \n"
                             + "extern(\n"
                             + "'{\"type\": \"s3\", \"uris\": [\\\"s3://imply-eng-datasets/qa/IngestionTest/wikipedia/files/wikiticker-2015-09-12-sampled.mini.json.gz\\\"]}',\n"
                             + "'{\"type\": \"json\"}',\n"
                             + "'[{\"name\": \"time\", \"type\": \"string\"}, {\"name\": \"channel\", \"type\": \"string\"}]'\n"
                             + ")\n"
                             + ") EXTEND (\"time\" VARCHAR, \"channel\" VARCHAR)\n"
                             + "partitioned by DAY\n"
                             + "clustered by channel";
    HashMap<String, Object> context = new HashMap<>(DEFAULT_CONTEXT);
    testIngestionQuery().context(context).sql(sqlString)
                        .expectValidationError(
                            new DruidExceptionMatcher(
                                DruidException.Persona.USER,
                                DruidException.Category.INVALID_INPUT,
                                "invalidInput"
                            ).expectMessageContains(
                                "EXTERN requires either a [signature] value or an EXTEND clause, but not both"
                            )
                        )
                        .verify();
  }

  @Test
  public void testErrorWhenNoneOfRowSignatureAndExtendsProvidedToExtern()
  {
    final String sqlString = "insert into dst \n"
                             + "select time_parse(\"time\") as __time, * \n"
                             + "from table( \n"
                             + "extern(\n"
                             + "'{\"type\": \"s3\", \"uris\": [\\\"s3://imply-eng-datasets/qa/IngestionTest/wikipedia/files/wikiticker-2015-09-12-sampled.mini.json.gz\\\"]}',\n"
                             + "'{\"type\": \"json\"}'\n"
                             + ")\n"
                             + ")\n"
                             + "partitioned by DAY\n"
                             + "clustered by channel";
    HashMap<String, Object> context = new HashMap<>(DEFAULT_CONTEXT);
    testIngestionQuery().context(context).sql(sqlString)
                        .expectValidationError(
                            new DruidExceptionMatcher(
                                DruidException.Persona.USER,
                                DruidException.Category.INVALID_INPUT,
                                "invalidInput"
                            ).expectMessageContains(
                                "EXTERN requires either a [signature] value or an EXTEND clause"
                            )
                        )
                        .verify();
  }

  @Test
  public void testErrorWhenInputSourceInvalid()
  {
    final String sqlString = "insert into dst \n"
                             + "select time_parse(\"time\") as __time, * \n"
                             + "from table( \n"
                             + "extern(\n"
                             + "'{\"type\": \"local\"}',\n"
                             + "'{\"type\": \"json\"}',\n"
                             + "'[{\"name\": \"time\", \"type\": \"string\"}, {\"name\": \"channel\", \"type\": \"string\"}]'\n"
                             + ")\n"
                             + ")\n"
                             + "partitioned by DAY\n"
                             + "clustered by channel";
    HashMap<String, Object> context = new HashMap<>(DEFAULT_CONTEXT);
    testIngestionQuery().context(context).sql(sqlString)
                        .expectValidationError(
                            new DruidExceptionMatcher(
                                DruidException.Persona.USER,
                                DruidException.Category.INVALID_INPUT,
                                "invalidInput"
                            ).expectMessageContains(
                                "Invalid value for the field [inputSource]. Reason:"
                            )
                        )
                        .verify();
  }
}
