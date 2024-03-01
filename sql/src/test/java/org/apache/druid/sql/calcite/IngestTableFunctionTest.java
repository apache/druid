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
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;

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
          new HttpInputSourceConfig(null)
      ),
      new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0),
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), externalRead("http"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("httpExtern")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("httpExtern")
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
            new HttpInputSourceConfig(null)
        ),
        new CsvInputFormat(ImmutableList.of("timestamp", "isRobot"), null, false, false, 0),
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
             "  format => 'csv'\n" +
             "  )\n" +
             ") EXTEND (\"timestamp\" VARCHAR, isRobot VARCHAR)\n" +
             "PARTITIONED BY HOUR")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("w000", expectedSig)
        .expectResources(dataSourceWrite("w000"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "timestamp_parse(\"timestamp\",null,'UTC')", ColumnType.LONG))
                .columns("isRobot", "v0")
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
    final String explanation = "[{" +
        "\"query\":{\"queryType\":\"scan\"," +
        "\"dataSource\":{\"type\":\"external\"," +
        "\"inputSource\":{\"type\":\"http\",\"uris\":[\"http://foo.com/bar.csv\"],\"httpAuthenticationUsername\":\"bob\",\"httpAuthenticationPassword\":{\"type\":\"default\",\"password\":\"secret\"}}," +
        "\"inputFormat\":{\"type\":\"csv\",\"columns\":[\"x\",\"y\",\"z\"]},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}]}," +
        "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]}," +
        "\"resultFormat\":\"compactedList\",\"columns\":[\"x\",\"y\",\"z\"],\"legacy\":false," +
        "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"," +
        "\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\"," +
        "\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}," +
        "\"columnTypes\":[\"STRING\",\"STRING\",\"LONG\"],\"granularity\":{\"type\":\"all\"}}," +
        "\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}]," +
        "\"columnMappings\":[{\"queryColumn\":\"x\",\"outputColumn\":\"x\"},{\"queryColumn\":\"y\",\"outputColumn\":\"y\"},{\"queryColumn\":\"z\",\"outputColumn\":\"z\"}]}]";
    final String resources = "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"dst\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"INSERT\",\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"dst\"},\"partitionedBy\":{\"type\":\"all\"}}";

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
    testBuilder()
        .plannerConfig(PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN)
        .sql(query)
        // Regular user does not have permission on extern or other table functions
        .authResult(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .expectedException(expected -> {
          expected.expect(ForbiddenException.class);
          expected.expectMessage(Access.DEFAULT_ERROR_MESSAGE);
        })
        .run();
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("httpExtern")
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
            new HttpInputSourceConfig(null)
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("a", "b", "c", "d", "x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  protected final ExternalDataSource localDataSource = new ExternalDataSource(
      new LocalInputSource(
          null,
          null,
          Arrays.asList(new File("/tmp/foo.csv"), new File("/tmp/bar.csv")),
          SystemFields.none()
      ),
      new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0),
      RowSignature.builder()
                  .add("x", ColumnType.STRING)
                  .add("y", ColumnType.STRING)
                  .add("z", ColumnType.LONG)
                  .build()
  );

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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
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
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(localDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("localExtern")
        .verify();
  }
}
