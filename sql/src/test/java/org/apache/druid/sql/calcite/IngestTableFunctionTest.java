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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.external.BaseUserDefinedTableMacro;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the input-source-specific table functions: http, inline and localfiles.
 * Each of these use meta-metadata defined by the catalog to identify the allowed
 * function arguments. The table functions work best with by-name argument syntax.
 * <p>
 * The tests first verify the baseline EXTERN form, then do the same ingest using
 * the simpler functions. Verification against both the logical plan and native
 * query ensure that the resulting MSQ task is identical regardless of the path
 * taken.
 */
@SqlTestFrameworkConfig.ComponentSupplier(IngestTableFunctionTest.ExportComponentSupplier.class)
public class IngestTableFunctionTest extends CalciteIngestionDmlTest
{
  protected static URI toURI(String uri)
  {
    try {
      return new URI(uri);
    }
    catch (URISyntaxException e) {
      throw new ISE("Bad URI: %s", uri);
    }
  }

  protected final ExternalDataSource httpDataSource = new ExternalDataSource(
      new HttpInputSource(
          Collections.singletonList(toURI("http://foo.com/bar.csv")),
          "bob",
          new DefaultPasswordProvider("secret"),
          SystemFields.none(),
          null,
          new HttpInputSourceConfig(null, null)
      ),
      new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0, null),
      RowSignature.builder()
                  .add("x", ColumnType.STRING)
                  .add("y", ColumnType.STRING)
                  .add("z", ColumnType.LONG)
                  .build()
  );
  protected final ExternalDataSource localDataSource = new ExternalDataSource(
      new LocalInputSource(
          null,
          null,
          Arrays.asList(new File("/tmp/foo.csv"), new File("/tmp/bar.csv")),
          SystemFields.none()
      ),
      new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0, null),
      RowSignature.builder()
                  .add("x", ColumnType.STRING)
                  .add("y", ColumnType.STRING)
                  .add("z", ColumnType.LONG)
                  .build()
  );

  /**
   * Basic use of EXTERN
   */
  @Test
  public void testHttpExtern()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(httpDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  /**
   * Http function
   */
  @Test
  public void testHttpFunction()
  {
    String extern = "TABLE(http("
             + "userName => 'bob',"
             + "password => 'secret',"
             + "uris => ARRAY['http://foo.com/bar.csv'],"
             + "format => 'csv'))"
             + "  (x VARCHAR, y VARCHAR, z BIGINT)";
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", extern)
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  /**
   * Http function
   */
  @Test
  public void testHttpFunctionWithInputsourceSecurity()
  {
    String extern = "TABLE(http("
                    + "userName => 'bob',"
                    + "password => 'secret',"
                    + "uris => ARRAY['http://foo.com/bar.csv'],"
                    + "format => 'csv'))"
                    + "  (x VARCHAR, y VARCHAR, z BIGINT)";
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", extern)
        .authConfig(AuthConfig.newBuilder().setEnableInputSourceSecurity(true).build())
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), externalRead("http"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  protected String externSqlByName(final ExternalDataSource externalDataSource)
  {
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      return StringUtils.format(
          "TABLE(extern(inputSource => %s,\n" +
          "             inputFormat => %s,\n" +
          "             signature => %s))",
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputSource())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputFormat())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getSignature()))
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * EXTERN with parameters by name. Logical plan and native query are identical
   * to the basic EXTERN.
   */
  @Test
  public void testHttpExternByName()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\nFROM %s\nPARTITIONED BY ALL TIME", externSqlByName(httpDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  private String toJson(final Object value)
  {
    try {
      return queryFramework().queryJsonMapper().writeValueAsString(value);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * EXTERN with the input source given by the {@code inputSource} SQL expression, and the
   * other arguments given as literals.
   */
  private String externSqlWithInputSource(final String inputSource, final ExternalDataSource externalDataSource)
  {
    return StringUtils.format(
        "TABLE(extern(%s, %s, %s))",
        inputSource,
        Calcites.escapeStringLiteral(toJson(externalDataSource.getInputFormat())),
        Calcites.escapeStringLiteral(toJson(externalDataSource.getSignature()))
    );
  }

  /**
   * EXTERN with the input source given as a bound parameter.
   */
  @Test
  public void testHttpExternWithParameter()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSqlWithInputSource("?", httpDataSource))
        .parameters(Collections.singletonList(new SqlParameter(SqlType.VARCHAR, toJson(httpDataSource.getInputSource()))))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  /**
   * A CAST of a bound parameter is accepted, since the value is not a literal in the SQL text.
   */
  @Test
  public void testHttpExternWithCastParameter()
  {
    testIngestionQuery()
        .sql(
            "INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME",
            externSqlWithInputSource("CAST(? AS VARCHAR)", httpDataSource)
        )
        .parameters(Collections.singletonList(new SqlParameter(SqlType.VARCHAR, toJson(httpDataSource.getInputSource()))))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  @Test
  public void testHttpExternCastLiteralRejected()
  {
    final String inputSource = StringUtils.format(
        "CAST(%s AS VARCHAR)",
        Calcites.escapeStringLiteral(toJson(httpDataSource.getInputSource()))
    );
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSqlWithInputSource(inputSource, httpDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageContains(
                "Argument [inputSource] of table function [EXTERN] must be a literal, not an expression. Remove the CAST."
            )
        )
        .verify();
  }

  @Test
  public void testHttpExternByNameCastLiteralRejected()
  {
    testIngestionQuery()
        .sql(
            "INSERT INTO dst SELECT *\n" +
            "FROM TABLE(extern(inputSource => %s,\n" +
            "                  inputFormat => CAST(%s AS VARCHAR),\n" +
            "                  signature => %s))\n" +
            "PARTITIONED BY ALL TIME",
            Calcites.escapeStringLiteral(toJson(httpDataSource.getInputSource())),
            Calcites.escapeStringLiteral(toJson(httpDataSource.getInputFormat())),
            Calcites.escapeStringLiteral(toJson(httpDataSource.getSignature()))
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageContains(
                "Argument [inputFormat] of table function [EXTERN] must be a literal, not an expression. Remove the CAST."
            )
        )
        .verify();
  }

  /**
   * Expressions other than a CAST were never accepted.
   */
  @Test
  public void testHttpExternExpressionRejected()
  {
    final String json = toJson(httpDataSource.getInputSource());
    final String inputSource = StringUtils.format(
        "%s || %s",
        Calcites.escapeStringLiteral(json.substring(0, 10)),
        Calcites.escapeStringLiteral(json.substring(10))
    );
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSqlWithInputSource(inputSource, httpDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(e -> {
          Assertions.assertInstanceOf(DruidException.class, e);
          Assertions.assertTrue(
              e.getMessage().contains("All arguments of call to macro EXTERN should be literal"),
              e.getMessage()
          );
        })
        .verify();
  }

  /**
   * HTTP with parameters by name. Logical plan and native query are identical
   * to the basic EXTERN.
   */
  @Test
  public void testHttpFn()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT x, y, z\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
             "                password => 'secret',\n" +
             "                uris => ARRAY['http://foo.com/bar.csv'],\n" +
             "                format => 'csv'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  @Test
  public void testHttpFnSkipHeaderRows()
  {
    final int skipHeaderRows = 3;

    testIngestionQuery()
        .sql(StringUtils.format(
            """
            INSERT INTO dst SELECT * FROM TABLE(http(
                userName => 'bob',
                password => 'secret',
                uris => ARRAY['http://foo.com/bar.csv'],
                format => 'csv',
                skipHeaderRows => %d
            )) EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME
            """,
            skipHeaderRows))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(new ExternalDataSource(
                    httpDataSource.getInputSource(),
                    new CsvInputFormat(ImmutableList.of("x", "y", "z"),
                                       null,
                                       false,
                                       false,
                                       skipHeaderRows,
                                       null),
                    httpDataSource.getSignature()
                ))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * A BIGINT parameter accepts a dynamic parameter that is bound as an exact integer.
   */
  @Test
  public void testHttpFnSkipHeaderRowsWithParameter()
  {
    final int skipHeaderRows = 3;

    testIngestionQuery()
        .sql("""
            INSERT INTO dst SELECT * FROM TABLE(http(
                userName => 'bob',
                password => 'secret',
                uris => ARRAY['http://foo.com/bar.csv'],
                format => 'csv',
                skipHeaderRows => ?
            )) EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME
            """)
        .parameters(Collections.singletonList(new SqlParameter(SqlType.BIGINT, (long) skipHeaderRows)))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(new ExternalDataSource(
                    httpDataSource.getInputSource(),
                    new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, skipHeaderRows, null),
                    httpDataSource.getSignature()
                ))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * A BIGINT parameter accepts only an exact integer that fits in 64 bits.
   * {@code 1e2} is integral, but it is rejected because the exponent makes it an approximate DOUBLE.
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', quoteCharacter = '"', value = {
      "1e2 | is of type [DOUBLE]",
      "1.5 | is of type [DECIMAL]",
      "'3' | is of type [VARCHAR]",
      "TRUE | is of type [BOOLEAN]",
      "9223372036854775808 | is out of range"
  })
  public void testHttpFnRejectsNonIntegerSkipHeaderRows(final String skipHeaderRows, final String actual)
  {
    testIngestionQuery()
        .sql(StringUtils.format(
            """
            INSERT INTO dst SELECT * FROM TABLE(http(
                uris => ARRAY['http://foo.com/bar.csv'],
                format => 'csv',
                skipHeaderRows => %s
            )) EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME
            """,
            skipHeaderRows
        ))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageIs(
                StringUtils.format(
                    "Argument [skipHeaderRows] of table function [http] must be a literal of type [BIGINT]"
                    + " or a compatible type, but %s.",
                    actual
                )
            )
        )
        .verify();
  }

  @Test
  public void testHttpFn2()
  {
    final ExternalDataSource httpDataSource = new ExternalDataSource(
        new HttpInputSource(
            Arrays.asList(toURI("http://example.com/foo.csv"), toURI("http://example.com/bar.csv")),
            "bob",
            new DefaultPasswordProvider("secret"),
            SystemFields.none(),
            ImmutableMap.of("Accept", "application/ndjson", "a", "b"),
            new HttpInputSourceConfig(null, Sets.newHashSet("Accept", "a"))
        ),
        new CsvInputFormat(ImmutableList.of("timestamp", "isRobot"), null, false, false, 0, null),
        RowSignature.builder()
                    .add("timestamp", ColumnType.STRING)
                    .add("isRobot", ColumnType.STRING)
                    .build()
    );
    RowSignature expectedSig = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("isRobot", ColumnType.STRING)
        .build();
    testIngestionQuery()
        .sql("INSERT INTO w000\n" +
             "SELECT\n" +
             "  TIME_PARSE(\"timestamp\") AS __time,\n" +
             "  isRobot\n" +
             "FROM TABLE(http(\n" +
             "  userName => 'bob',\n" +
             "  password => 'secret',\n" +
             "  uris => ARRAY['http://example.com/foo.csv', 'http://example.com/bar.csv'],\n" +
             "  format => 'csv',\n" +
             "  headers=> '{\"Accept\":\"application/ndjson\", \"a\": \"b\" }'\n" +
             "  )\n" +
             ") EXTEND (\"timestamp\" VARCHAR, isRobot VARCHAR)\n" +
             "PARTITIONED BY HOUR")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("w000", expectedSig)
        .expectResources(dataSourceRead("w000"), dataSourceWrite("w000"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "timestamp_parse(\"timestamp\",null,'UTC')", ColumnType.LONG))
                .columns("v0", "isRobot")
                .columnTypes(ColumnType.LONG, ColumnType.STRING)
                .build()
         )
        .verify();
  }

  @Test
  public void testExplainHttpFn()
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String query =
        "EXPLAIN PLAN FOR\n" +
        "INSERT INTO dst SELECT x, y, z\n" +
        "FROM TABLE(http(userName => 'bob',\n" +
        "                password => 'secret',\n" +
        "                uris => ARRAY['http://foo.com/bar.csv'],\n" +
        "                format => 'csv'))\n" +
        "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
        "PARTITIONED BY ALL TIME";
    final String explanation = "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"http\",\"uris\":[\"http://foo.com/bar.csv\"],\"httpAuthenticationUsername\":\"bob\",\"httpAuthenticationPassword\":{\"type\":\"default\",\"password\":\"secret\"},\"requestHeaders\":{}},\"inputFormat\":{\"type\":\"csv\",\"columns\":[\"x\",\"y\",\"z\"]},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"x\",\"y\",\"z\"],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\",\"LONG\"],\"granularity\":{\"type\":\"all\"},\"legacy\":false},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}],\"columnMappings\":[{\"queryColumn\":\"x\",\"outputColumn\":\"x\"},{\"queryColumn\":\"y\",\"outputColumn\":\"y\"},{\"queryColumn\":\"z\",\"outputColumn\":\"z\"}]}]";
    final String resources = "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"dst\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"INSERT\",\"targetDataSource\":\"dst\",\"partitionedBy\":{\"type\":\"all\"}}";

    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        query,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation, resources, attributes}
        )
    );
    didTest = true;
  }

  @Test
  public void testExplainHttpFnUnauthorized()
  {
    final String query =
        "EXPLAIN PLAN FOR\n" +
        "INSERT INTO dst SELECT x, y, z\n" +
        "FROM TABLE(http(userName => 'bob',\n" +
        "                password => 'secret',\n" +
        "                uris => ARRAY['http://foo.com/bar.csv'],\n" +
        "                format => 'csv'))\n" +
        "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
        "PARTITIONED BY ALL TIME";
    didTest = true; // Else the framework will complain
    ForbiddenException e = assertThrows(
        ForbiddenException.class,
        () -> testBuilder()
            .plannerConfig(PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN)
            .sql(query)
            // Regular user does not have permission on extern or other table functions
            .authResult(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .run()
    );
    Assertions.assertEquals(Access.DEFAULT_ERROR_MESSAGE, e.getMessage());
  }

  @Test
  public void testHttpFnWithParameters()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
            "                 password => 'secret',\n" +
             "                uris => ?,\n" +
             "                format => 'csv'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .parameters(Collections.singletonList(new SqlParameter(SqlType.ARRAY, new String[] {"http://foo.com/bar.csv"})))
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  @Test
  public void testHttpFnWithCastParameter()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
             "                password => 'secret',\n" +
             "                uris => ARRAY['http://foo.com/bar.csv'],\n" +
             "                format => CAST(? AS VARCHAR)))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .parameters(Collections.singletonList(new SqlParameter(SqlType.VARCHAR, "csv")))
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  @Test
  public void testHttpFnWithCastArrayParameter()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
             "                password => 'secret',\n" +
             "                uris => CAST(? AS VARCHAR ARRAY),\n" +
             "                format => 'csv'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .parameters(Collections.singletonList(new SqlParameter(SqlType.ARRAY, new String[] {"http://foo.com/bar.csv"})))
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  @Test
  public void testHttpFnCastLiteralRejected()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
             "                password => CAST('secret' AS VARCHAR),\n" +
             "                uris => ARRAY['http://foo.com/bar.csv'],\n" +
             "                format => 'csv'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageContains(
                "Argument [password] of table function [http] must be a literal, not an expression. Remove the CAST."
            )
        )
        .verify();
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "123 | INTEGER",
      "123.45 | DECIMAL",
      "1e2 | DOUBLE",
      "TRUE | BOOLEAN",
      "DATE '2000-02-03' | DATE",
      "TIME '09:08:07' | TIME",
      "{d '2000-02-03'} | DATE",
      "{t '09:08:07'} | TIME",
      "DECIMAL '123.45' | DECIMAL"
  })
  public void testHttpFnRejectsNonStringUserName(final String userName, final String actualType)
  {
    testIngestionQuery()
        .sql(StringUtils.format(
            """
            INSERT INTO dst SELECT *
            FROM TABLE(http(uris => ARRAY['http://foo.com/bar.csv'],
                            format => 'csv',
                            password => 'secret',
                            userName => %s))
                 EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME
            """,
            userName
        ))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageIs(
                StringUtils.format(
                    "Argument [userName] of table function [http] must be a literal of type [VARCHAR]"
                    + " or a compatible type, but is of type [%s].",
                    actualType
                )
            )
        )
        .verify();
  }

  @Test
  public void testHttpFnRejectsNonStringArrayElement()
  {
    testIngestionQuery()
        .sql("""
            INSERT INTO dst SELECT *
            FROM TABLE(http(uris => ARRAY[123], format => 'csv'))
                 EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME
            """)
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageIs(
                "Argument [uris] of table function [http] must be a literal of type [VARCHAR ARRAY]"
                + " or a compatible type, but element [1] is of type [INTEGER]."
            )
        )
        .verify();
  }

  /**
   * The error names the position of the NULL element, since the type of the array alone does not explain the failure.
   */
  @Test
  public void testHttpFnRejectsNullArrayElement()
  {
    testIngestionQuery()
        .sql("""
            INSERT INTO dst SELECT *
            FROM TABLE(http(uris => ARRAY['http://foo.com/bar.csv', NULL], format => 'csv'))
                 EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME
            """)
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageIs(
                "Argument [uris] of table function [http] must be a literal of type [VARCHAR ARRAY]"
                + " or a compatible type, but element [2] is NULL."
            )
        )
        .verify();
  }

  @Test
  public void testHttpFnRejectsNonArrayUris()
  {
    testIngestionQuery()
        .sql("""
            INSERT INTO dst SELECT *
            FROM TABLE(http(uris => 'http://foo.com/bar.csv', format => 'csv'))
                 EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME
            """)
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageIs(
                "Argument [uris] of table function [http] must be a literal of type [VARCHAR ARRAY]"
                + " or a compatible type, but is of type [VARCHAR]."
            )
        )
        .verify();
  }

  /**
   * Fails when a table function declares a parameter type that argument conversion does not handle.
   * {@link BaseUserDefinedTableMacro#convertArguments} converts only VARCHAR, BIGINT, BOOLEAN and VARCHAR ARRAY.
   * If this test fails for a new type, add the conversion and its tests, then extend this check.
   */
  @Test
  public void testTableFunctionParameterTypesAreSupported()
  {
    // Not using testIngestionQuery, so must set didTest manually to satisfy the check in tearDown.
    didTest = true;

    int numTableFunctions = 0;
    for (final SqlOperator operator : queryFramework().operatorTable().getOperatorList()) {
      if (!(operator instanceof BaseUserDefinedTableMacro)) {
        continue;
      }
      numTableFunctions++;
      final SqlOperandMetadata metadata = ((BaseUserDefinedTableMacro) operator).getOperandTypeChecker();
      final List<RelDataType> types = metadata.paramTypes(DruidTypeSystem.TYPE_FACTORY);
      final List<String> names = metadata.paramNames();
      for (int i = 0; i < types.size(); i++) {
        final RelDataType type = types.get(i);
        final RelDataType componentType = type.getComponentType();
        final boolean supported = componentType == null
                                  ? type.getSqlTypeName() == SqlTypeName.VARCHAR
                                    || type.getSqlTypeName() == SqlTypeName.BIGINT
                                    || type.getSqlTypeName() == SqlTypeName.BOOLEAN
                                  : componentType.getSqlTypeName() == SqlTypeName.VARCHAR;
        Assertions.assertTrue(
            supported,
            StringUtils.format(
                "Parameter [%s] of table function [%s] has type [%s], which convertArguments does not handle",
                names.get(i),
                operator.getName(),
                type
            )
        );
      }
    }
    // Guards against the loop silently checking nothing.
    Assertions.assertTrue(numTableFunctions > 0, "No table functions found in the operator table");
  }

  /**
   * Array elements of different lengths are coerced to a common VARCHAR(n) type, which adds a CAST to each element.
   * Those CASTs are not rejected.
   */
  @Test
  public void testHttpFnWithUrisOfDifferentLengths()
  {
    final ExternalDataSource differentLengthDataSource = new ExternalDataSource(
        new HttpInputSource(
            Arrays.asList(toURI("http://example.com/foo.csv"), toURI("http://example.com/a-longer-name.csv")),
            "bob",
            new DefaultPasswordProvider("secret"),
            SystemFields.none(),
            null,
            new HttpInputSourceConfig(null, null)
        ),
        httpDataSource.getInputFormat(),
        httpDataSource.getSignature()
    );
    testIngestionQuery()
        .sql("""
            INSERT INTO dst SELECT *
            FROM TABLE(http(userName => 'bob',
                            password => 'secret',
                            uris => ARRAY['http://example.com/foo.csv', 'http://example.com/a-longer-name.csv'],
                            format => 'csv'))
                 EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME""")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", differentLengthDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(differentLengthDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testHttpFnInfixCastLiteralRejected()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
             "                password => 'secret',\n" +
             "                uris => ARRAY['http://foo.com/bar.csv'],\n" +
             "                format => 'csv'::VARCHAR))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageContains(
                "Argument [format] of table function [http] must be a literal, not an expression. Remove the CAST."
            )
        )
        .verify();
  }

  @Test
  public void testHttpFnArrayElementCastLiteralRejected()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
             "                password => 'secret',\n" +
             "                uris => ARRAY[CAST('http://foo.com/bar.csv' AS VARCHAR)],\n" +
             "                format => 'csv'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageContains(
                "Argument [uris] of table function [http] must be a literal, not an expression. Remove the CAST."
            )
        )
        .verify();
  }

  @Test
  public void testHttpFnDifferentLengthArrayElementCastLiteralRejected()
  {
    testIngestionQuery()
        .sql("""
            INSERT INTO dst SELECT *
            FROM TABLE(http(userName => 'bob',
                            password => 'secret',
                            uris => ARRAY['http://foo.com/a-longer-name.csv', CAST('http://foo.com/bar.csv' AS VARCHAR)],
                            format => 'csv'))
                 EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
            PARTITIONED BY ALL TIME""")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageContains(
                "Argument [uris] of table function [http] must be a literal, not an expression. Remove the CAST."
            )
        )
        .verify();
  }

  @Test
  public void testHttpJson()
  {
    final ExternalDataSource httpDataSource = new ExternalDataSource(
        new HttpInputSource(
            Collections.singletonList(toURI("http://foo.com/bar.json")),
            "bob",
            new DefaultPasswordProvider("secret"),
            SystemFields.none(),
            null,
            new HttpInputSourceConfig(null, null)
        ),
        new JsonInputFormat(null, null, null, null, null),
        RowSignature.builder()
                    .add("x", ColumnType.STRING)
                    .add("y", ColumnType.STRING)
                    .add("z", ColumnType.NESTED_DATA)
                    .add("a", ColumnType.STRING_ARRAY)
                    .add("b", ColumnType.LONG_ARRAY)
                    .add("c", ColumnType.FLOAT_ARRAY)
                    .add("d", ColumnType.DOUBLE_ARRAY)
                    .build()
        );
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
            "                 password => 'secret',\n" +
             "                uris => ARRAY['http://foo.com/bar.json'],\n" +
             "                format => 'json'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z TYPE('COMPLEX<json>'), a VARCHAR ARRAY, b BIGINT ARRAY, c FLOAT ARRAY, d DOUBLE ARRAY)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z", "a", "b", "c", "d")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.ofComplex("json"), ColumnType.ofArray(ColumnType.STRING), ColumnType.ofArray(ColumnType.LONG), ColumnType.ofArray(ColumnType.FLOAT), ColumnType.ofArray(ColumnType.DOUBLE))
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .verify();
  }

  /**
   * The COMPLEX type prefix in an EXTEND clause matches case-insensitively (like all other type names) and is
   * normalized to the canonical form; the complex type name inside the brackets keeps its casing.
   */
  @Test
  public void testHttpJsonLowercaseComplexTypePrefix()
  {
    final ExternalDataSource httpDataSource = new ExternalDataSource(
        new HttpInputSource(
            Collections.singletonList(toURI("http://foo.com/bar.json")),
            "bob",
            new DefaultPasswordProvider("secret"),
            SystemFields.none(),
            null,
            new HttpInputSourceConfig(null, null)
        ),
        new JsonInputFormat(null, null, null, null, null),
        RowSignature.builder()
                    .add("x", ColumnType.STRING)
                    .add("z", ColumnType.NESTED_DATA)
                    .build()
        );
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
            "                 password => 'secret',\n" +
             "                uris => ARRAY['http://foo.com/bar.json'],\n" +
             "                format => 'json'))\n" +
             "     EXTEND (x VARCHAR, z TYPE('complex<json>'))\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "z")
                .columnTypes(ColumnType.STRING, ColumnType.ofComplex("json"))
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .verify();
  }

  /**
   * A malformed complex type in an EXTEND clause (missing the closing bracket) is rejected rather than silently
   * resolving to a different type.
   */
  @Test
  public void testHttpJsonMalformedComplexTypeRejected()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(userName => 'bob',\n" +
            "                 password => 'secret',\n" +
             "                uris => ARRAY['http://foo.com/bar.json'],\n" +
             "                format => 'json'))\n" +
             "     EXTEND (x VARCHAR, z TYPE('complex<json'))\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(e -> {
          Assertions.assertInstanceOf(DruidException.class, e);
          Assertions.assertTrue(e.getMessage().contains("Column [z] has an unsupported type"));
        })
        .verify();
  }

  /**
   * Basic use of an inline input source via EXTERN
   */
  @Test
  public void testInlineExtern()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  protected String externSqlByNameNoSig(final ExternalDataSource externalDataSource)
  {
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      return StringUtils.format(
          "TABLE(extern(inputSource => %s, inputFormat => %s))",
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputSource())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputFormat()))
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  protected String externClauseFromSig(final ExternalDataSource externalDataSource)
  {
    RowSignature sig = externalDataSource.getSignature();
    StringBuilder buf = new StringBuilder("(");
    for (int i = 0; i < sig.size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(sig.getColumnName(i)).append(" ");
      ColumnType type = sig.getColumnType(i).get();
      if (type == ColumnType.STRING) {
        buf.append(Columns.SQL_VARCHAR);
      } else if (type == ColumnType.LONG) {
        buf.append(Columns.SQL_BIGINT);
      } else if (type == ColumnType.DOUBLE) {
        buf.append(Columns.DOUBLE);
      } else if (type == ColumnType.FLOAT) {
        buf.append(Columns.FLOAT);
      } else {
        throw new UOE("Unsupported native type %s", type);
      }
    }
    return buf.append(")").toString();
  }

  /**
   * Use an inline input source with EXTERN and EXTEND
   */
  @Test
  public void testInlineExternWithExtend()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "  FROM %s\n" +
             "  %s\n" +
             "  PARTITIONED BY ALL TIME",
             externSqlByNameNoSig(externalDataSource),
             externClauseFromSig(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  /**
   * Inline with parameters by name. Logical plan and native query are identical
   * to the basic EXTERN.
   */
  @Test
  public void testInlineFn()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(inline(data => ARRAY['a,b,1', 'c,d,2'],\n" +
             "                  format => 'csv'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  /**
   * Basic use of LOCALFILES
   */
  @Test
  public void testLocalExtern()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(localDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", localDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("localExtern")
        .verify();
  }

  /**
   * Localfiles with parameters by name. Logical plan and native query are identical
   * to the basic EXTERN.
   */
  @Test
  public void testLocalFilesFn()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(localfiles(files => ARRAY['/tmp/foo.csv', '/tmp/bar.csv'],\n" +
             "                  format => 'csv'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", localDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("localExtern")
        .verify();
  }

  /**
   * Local with parameters by name. Shows that the EXTERN keyword is optional.
   * Logical plan and native query are identical to the basic EXTERN.
   */
  @Test
  public void testLocalFnOmitExtend()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(localfiles(files => ARRAY['/tmp/foo.csv', '/tmp/bar.csv'],\n" +
             "                  format => 'csv'))\n" +
             "     (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", localDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("localExtern")
        .verify();
  }

  /**
   * Local with a table alias an explicit column references.
   */
  @Test
  public void testLocalFnWithAlias()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst\n" +
             "SELECT myTable.x, myTable.y, myTable.z\n" +
             "FROM TABLE(localfiles(files => ARRAY['/tmp/foo.csv', '/tmp/bar.csv'],\n" +
             "                  format => 'csv'))\n" +
             "     (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "     As myTable\n" +
             "PARTITIONED BY ALL TIME"
         )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", localDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("localExtern")
        .verify();
  }

  /**
   * Local with NOT NULL on columns, which is ignored.
   */
  @Test
  public void testLocalFnNotNull()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst\n" +
             "SELECT myTable.x, myTable.y, myTable.z\n" +
             "FROM TABLE(localfiles(files => ARRAY['/tmp/foo.csv', '/tmp/bar.csv'],\n" +
             "                  format => 'csv'))\n" +
             "     (x VARCHAR NOT NULL, y VARCHAR NOT NULL, z BIGINT NOT NULL)\n" +
             "     As myTable\n" +
             "PARTITIONED BY ALL TIME"
         )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", localDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("localExtern")
        .verify();
  }

  @Test
  public void testInlineFnWithCastParameter()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(inline(data => ARRAY['a,b,1', 'c,d,2'],\n" +
             "                  format => CAST(? AS VARCHAR)))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .parameters(Collections.singletonList(new SqlParameter(SqlType.VARCHAR, "csv")))
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  @Test
  public void testInlineFnCastLiteralRejected()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(inline(data => ARRAY['a,b,1', 'c,d,2'],\n" +
             "                  format => CAST('csv' AS VARCHAR)))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageContains(
                "Argument [format] of table function [inline] must be a literal, not an expression. Remove the CAST."
            )
        )
        .verify();
  }

  @Test
  public void testLocalFilesFnWithCastParameter()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(localfiles(files => ARRAY['/tmp/foo.csv', '/tmp/bar.csv'],\n" +
             "                  format => CAST(? AS VARCHAR)))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .parameters(Collections.singletonList(new SqlParameter(SqlType.VARCHAR, "csv")))
        .expectTarget("dst", localDataSource.getSignature())
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("localExtern")
        .verify();
  }

  @Test
  public void testLocalFilesFnCastLiteralRejected()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(localfiles(files => ARRAY['/tmp/foo.csv', '/tmp/bar.csv'],\n" +
             "                  format => CAST('csv' AS VARCHAR)))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidExceptionMatcher.invalidSqlInput().expectMessageContains(
                "Argument [format] of table function [localfiles] must be a literal, not an expression. Remove the CAST."
            )
        )
        .verify();
  }

  protected static class ExportComponentSupplier extends IngestionDmlComponentSupplier
  {
    public ExportComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public DruidModule getOverrideModule()
    {
      return DruidModuleCollection.of(
            super.getOverrideModule(),
            new LocalOverrideModule()
          );
    }

    public static class LocalOverrideModule implements DruidModule
    {
      @Override
      public void configure(Binder binder)
      {
        binder
            .bind(HttpInputSourceConfig.class)
            .toInstance(new HttpInputSourceConfig(null, ImmutableSet.of("Accept", "a")));
      }
    }
  }
}
