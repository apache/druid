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

package org.apache.druid.catalog.sql;

import com.google.common.collect.ImmutableList;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.CachedMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.CalciteIngestionDmlTest;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * Test the use of catalog specs to drive MSQ ingestion.
 */
public class CatalogIngestionTest extends CalciteIngestionDmlTest
{
  @ClassRule
  public static final TestDerbyConnector.DerbyConnectorRule DERBY_CONNECTION_RULE =
      new TestDerbyConnector.DerbyConnectorRule();

  /**
   * Signature for the foo datasource after applying catalog metadata.
   */
  private static final RowSignature FOO_SIGNATURE = RowSignature.builder()
      .add("__time", ColumnType.LONG)
      .add("extra1", ColumnType.STRING)
      .add("dim2", ColumnType.STRING)
      .add("dim1", ColumnType.STRING)
      .add("cnt", ColumnType.LONG)
      .add("m1", ColumnType.DOUBLE)
      .add("extra2", ColumnType.LONG)
      .add("extra3", ColumnType.STRING)
      .add("m2", ColumnType.DOUBLE)
      .build();

  private static CatalogStorage storage;

  @Override
  public CatalogResolver createCatalogResolver()
  {
    CatalogTests.DbFixture dbFixture = new CatalogTests.DbFixture(DERBY_CONNECTION_RULE);
    storage = dbFixture.storage;
    MetadataCatalog catalog = new CachedMetadataCatalog(
        storage,
        storage.schemaRegistry(),
        storage.jsonMapper()
    );
    return new LiveCatalogResolver(catalog);
  }

  @Override
  public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
  {
    super.finalizeTestFramework(sqlTestFramework);
    buildTargetDatasources();
    buildFooDatasource();
    buildFooNonSealedDatasource();
  }

  private void buildTargetDatasources()
  {
    TableMetadata spec = TableBuilder.datasource("hourDs", "PT1H")
        .build();
    createTableMetadata(spec);
  }

  public void buildFooDatasource()
  {
    TableMetadata spec = TableBuilder.datasource("foo", "ALL")
        .timeColumn()
        .column("extra1", null)
        .column("dim2", null)
        .column("dim1", null)
        .column("cnt", null)
        .column("m1", Columns.DOUBLE)
        .column("extra2", Columns.LONG)
        .column("extra3", Columns.STRING)
        .hiddenColumns(Arrays.asList("dim3", "unique_dim1"))
        .sealed(true)
        .build();
    createTableMetadata(spec);
  }

  public void buildFooNonSealedDatasource()
  {
    TableMetadata spec = TableBuilder.datasource("foo_non_sealed", "ALL")
        .timeColumn()
        .column("extra1", null)
        .column("dim2", null)
        .column("dim1", null)
        .column("cnt", null)
        .column("m1", Columns.DOUBLE)
        .column("extra2", Columns.LONG)
        .column("extra3", Columns.STRING)
        .hiddenColumns(Arrays.asList("dim3", "unique_dim1"))
        .sealed(false)
        .build();
    createTableMetadata(spec);
  }

  private void createTableMetadata(TableMetadata table)
  {
    try {
      storage.tables().create(table);
    }
    catch (CatalogException e) {
      fail(e.getMessage());
    }
  }

  /**
   * If the segment grain is given in the catalog and absent in the PARTITIONED BY clause in the query, then use the
   * value from the catalog.
   */
  @Test
  public void testInsertHourGrainPartitonedByFromCatalog()
  {
    testIngestionQuery()
        .sql("INSERT INTO hourDs\n" +
             "SELECT * FROM foo")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("hourDs", FOO_SIGNATURE)
        .expectResources(dataSourceWrite("hourDs"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "extra1", "extra2", "extra3", "m1", "m2")
                .context(queryContextWithGranularity(Granularities.HOUR))
                .build()
        )
        .verify();
  }

  /**
   * If the segment grain is given in the catalog, and also by PARTITIONED BY, then
   * the query value is used.
   */
  @Test
  public void testInsertHourGrainWithDayPartitonedByFromQuery()
  {
    testIngestionQuery()
        .sql("INSERT INTO hourDs\n" +
             "SELECT * FROM foo\n" +
             "PARTITIONED BY day")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("hourDs", FOO_SIGNATURE)
        .expectResources(dataSourceWrite("hourDs"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "extra1", "extra2", "extra3", "m1", "m2")
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .verify();
  }

  /**
   * If the segment grain is given in the catalog and absent in the PARTITIONED BY clause in the query, then use the
   * value from the catalog.
   */
  @Test
  public void testReplaceHourGrainPartitonedByFromCatalog()
  {
    testIngestionQuery()
        .sql("REPLACE INTO hourDs OVERWRITE ALL\n" +
             "SELECT * FROM foo")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("hourDs", FOO_SIGNATURE)
        .expectResources(dataSourceWrite("hourDs"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "extra1", "extra2", "extra3", "m1", "m2")
                .context(queryContextWithGranularity(Granularities.HOUR))
                .build()
        )
        .verify();
  }

  /**
   * If the segment grain is given in the catalog, and also by PARTITIONED BY, then
   * the query value is used.
   */
  @Test
  public void testReplaceHourGrainWithDayPartitonedByFromQuery()
  {
    testIngestionQuery()
        .sql("REPLACE INTO hourDs OVERWRITE ALL\n" +
             "SELECT * FROM foo\n" +
             "PARTITIONED BY day")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("hourDs", FOO_SIGNATURE)
        .expectResources(dataSourceWrite("hourDs"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "extra1", "extra2", "extra3", "m1", "m2")
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .verify();
  }

  /**
   * Attempt to verify that types specified in the catalog are pushed down to
   * MSQ. At present, Druid does not have the tools needed to do a full push-down.
   * We have to accept a good-enough push-down: that the produced type is at least
   * compatible with the desired type.
   */
  @Test
  public void testInsertIntoCatalogTable()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .add("m1", ColumnType.DOUBLE)
        .add("extra2", ColumnType.LONG)
        .add("extra3", ColumnType.STRING)
        .build();
    testIngestionQuery()
        .sql("INSERT INTO foo\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m1,\n" +
             "  CAST(d AS BIGINT) AS extra2,\n" +
             "  e AS extra3\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo", signature)
        .expectResources(dataSourceWrite("foo"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG),
                    expressionVirtualColumn("v2", "CAST(\"c\", 'DOUBLE')", ColumnType.DOUBLE),
                    expressionVirtualColumn("v3", "CAST(\"d\", 'LONG')", ColumnType.LONG)
                )
                // Scan query lists columns in alphabetical order independent of the
                // SQL project list or the defined schema. Here we just check that the
                // set of columns is correct, but not their order.
                .columns("b", "e", "v0", "v1", "v2", "v3")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * Attempt to verify that types specified in the catalog are pushed down to
   * MSQ. At present, Druid does not have the tools needed to do a full push-down.
   * We have to accept a good-enough push-down: that the produced type is at least
   * compatible with the desired type.
   */
  @Test
  public void testWithInsertIntoCatalogTable()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .add("m1", ColumnType.DOUBLE)
        .add("extra2", ColumnType.LONG)
        .add("extra3", ColumnType.STRING)
        .build();
    testIngestionQuery()
        .sql("INSERT INTO \"foo\"\n" +
             "WITH \"ext\" AS (\n" +
             "  SELECT *\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             ")\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m1,\n" +
             "  CAST(d AS BIGINT) AS extra2,\n" +
             "  e AS extra3\n" +
             "FROM \"ext\"\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo", signature)
        .expectResources(dataSourceWrite("foo"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG),
                    expressionVirtualColumn("v2", "CAST(\"c\", 'DOUBLE')", ColumnType.DOUBLE),
                    expressionVirtualColumn("v3", "CAST(\"d\", 'LONG')", ColumnType.LONG)
                )
                // Scan query lists columns in alphabetical order independent of the
                // SQL project list or the defined schema. Here we just check that the
                // set of columns is correct, but not their order.
                .columns("b", "e", "v0", "v1", "v2", "v3")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }


  /**
   * Attempt to verify that types specified in the catalog are pushed down to
   * MSQ. At present, Druid does not have the tools needed to do a full push-down.
   * We have to accept a good-enough push-down: that the produced type is at least
   * compatible with the desired type.
   */
  @Test
  public void testInsertAddNonDefinedColumnIntoSealedCatalogTable()
  {
    testIngestionQuery()
        .sql("INSERT INTO foo\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m1,\n" +
             "  CAST(d AS BIGINT) AS extra2,\n" +
             "  e AS extra3,\n" +
             "  e AS non_defined_column\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidException.class,
            "Column [non_defined_column] is not defined in the target table [druid.foo] strict schema"
        )
        .verify();
  }

  @Test
  public void testInsertAddNonDefinedColumnIntoNonSealedCatalogTable()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .add("m1", ColumnType.DOUBLE)
        .add("extra2", ColumnType.LONG)
        .add("extra3", ColumnType.STRING)
        .add("non_defined_column", ColumnType.STRING)
        .build();
    testIngestionQuery()
        .sql("INSERT INTO foo_non_sealed\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m1,\n" +
             "  CAST(d AS BIGINT) AS extra2,\n" +
             "  e AS extra3,\n" +
             "  e AS non_defined_column\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo_non_sealed", signature)
        .expectResources(dataSourceWrite("foo_non_sealed"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG),
                    expressionVirtualColumn("v2", "CAST(\"c\", 'DOUBLE')", ColumnType.DOUBLE),
                    expressionVirtualColumn("v3", "CAST(\"d\", 'LONG')", ColumnType.LONG)
                )
                // Scan query lists columns in alphabetical order independent of the
                // SQL project list or the defined schema. Here we just check that the
                // set of columns is correct, but not their order.
                .columns("b", "e", "v0", "v1", "v2", "v3")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }
}
