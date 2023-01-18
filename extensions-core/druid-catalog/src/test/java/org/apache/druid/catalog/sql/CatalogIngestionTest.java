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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.BaseExternTableTest;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.CachedMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.CalciteIngestionDmlTest;
import org.apache.druid.sql.calcite.CalciteInsertDmlTest;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.Builder;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * Test the use of catalog specs to drive MSQ ingestion.
 */
public class CatalogIngestionTest extends CalciteIngestionDmlTest
{
  // Copied here from MSQE since that extension is not visible here.
  public static final String CTX_ROWS_PER_SEGMENT = "msqRowsPerSegment";

  @ClassRule
  public static final TestDerbyConnector.DerbyConnectorRule DERBY_CONNECTION_RULE =
      new TestDerbyConnector.DerbyConnectorRule();

  private static CatalogStorage storage;

  @Override
  protected void configureBuilder(Builder builder)
  {
    super.configureBuilder(builder);
    CatalogTests.DbFixture dbFixture = new CatalogTests.DbFixture(DERBY_CONNECTION_RULE);
    storage = dbFixture.storage;
    MetadataCatalog catalog = new CachedMetadataCatalog(
        storage,
        storage.schemaRegistry(),
        storage.jsonMapper()
    );
    builder.catalogResolver(new LiveCatalogResolver(catalog));
    builder.extraSchema(new ExternalSchema(catalog, storage.jsonMapper()));
  }

  /**
   * Test an inline table defined in the catalog. The structure is identical to the
   * inline tests in CatalogIngestionTest, only here the information comes from the
   * catalog.
   */
  @Test
  public void testInlineTable()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM ext.inline\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.externalRead("inline"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  /**
   * Test an inline table defined in the catalog. The structure is identical to the
   * inline tests in CatalogIngestionTest, only here the information comes from the
   * catalog.
   */
  @Test
  public void testInlineTableFn()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst\n" +
             "SELECT *\n" +
             "FROM TABLE(ext.inline())\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.externalRead("inline"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

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
             "SELECT TIME_PARSE(a) AS __time, b AS dim1, 1 AS cnt,\n" +
            "        CAST(c AS DOUBLE) AS m1, CAST(d AS BIGINT) AS extra2, e AS extra3\n" +
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
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .verify();
  }

  @Test
  public void testInsertIntoCatalogTableTypeConflict1()
  {
    testIngestionQuery()
        .sql("INSERT INTO foo\n" +
             "SELECT TIME_PARSE(a) AS __time, b AS dim1, 1 AS cnt,\n" +
            "        c AS m1, CAST(d AS INTEGER) AS extra2, e AS extra3\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            SqlPlanningException.class,
            "Type BIGINT of column m1 does not match the defined type of DOUBLE: add a CAST"
         )
        .verify();
  }

  @Test
  public void testInsertIntoCatalogTableTypeConflict2()
  {
    testIngestionQuery()
        .sql("INSERT INTO foo\n" +
             "SELECT TIME_PARSE(a) AS __time, b AS dim1, 1 AS cnt,\n" +
            "        CAST(c AS DOUBLE) AS m1, CAST(d AS INTEGER) AS extra2, e AS extra3\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            SqlPlanningException.class,
            "Type INTEGER of column extra2 does not match the defined type of BIGINT: add a CAST"
         )
        .verify();
  }

  /**
   * Insert from a table with a schema defined in the catalog.
   *
   * @see {@link org.apache.druid.sql.calcite.CalciteInsertDmlTest#testInsertFromTable}
   * for the non-catalog version
   */
  @Test
  public void testInsertFromTable()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst\n" +
             "SELECT * FROM foo\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", FOO_SIGNATURE)
        .expectResources(dataSourceWrite("dst"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                // Scan query lists columns in alphabetical order independent of the
                // SQL project list or the defined schema. Here we just check that the
                // set of columns is correct, but not their order.
                .columns("__time", "cnt", "dim1", "dim2", "extra1", "extra2", "extra3", "m1", "m2")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .expectLogicalPlanFrom("insertFromTable")
        .verify();
  }

  /**
   * Similar, but segment granularity read from catalog.
   */
  @Test
  public void testInsertAllTime()
  {
    testIngestionQuery()
        .sql("INSERT INTO allDs\n" +
             "SELECT * FROM foo")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("allDs", FOO_SIGNATURE)
        .expectResources(dataSourceWrite("allDs"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "extra1", "extra2", "extra3", "m1", "m2")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
         )
        .verify();
  }

  /**
   * Similar, but with hour segment granularity read from catalog.
   */
  @Test
  public void testInsertHourGrain()
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
   * Partition grain comes from both the catalog and redundant query
   * PARITIONED BY. The two sources agree, which is odd, but legal.
   */
  @Test
  public void testInsertHourGrainRedundant()
  {
    testIngestionQuery()
        .sql("INSERT INTO hourDs\n" +
             "SELECT * FROM foo\n" +
             "PARTITIONED BY hour")
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
   * the query value must be the same as the catalog value.
   */
  @Test
  public void testInsertHourGrainConflict()
  {
    testIngestionQuery()
        .sql("INSERT INTO hourDs\n" +
             "SELECT * FROM foo\n" +
             "PARTITIONED BY day")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            SqlPlanningException.class,
            "PARTITIONED BY mismatch. Catalog: [PT1H], query: [P1D]"
         )
        .verify();
  }

  @Test
  public void testInsertHourGrainConflictAll()
  {
    testIngestionQuery()
        .sql("INSERT INTO hourDs\n" +
             "SELECT * FROM foo\n" +
             "PARTITIONED BY ALL")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            SqlPlanningException.class,
            "PARTITIONED BY mismatch. Catalog: [PT1H], query: [ALL TIME]"
         )
        .verify();
  }

  /**
   * Similar, but with 5-minute segment granularity, and a defined segment
   * size, read from catalog.
   */
  @Test
  public void testInsert5MinGrainAndSegmentSize()
  {
    Map<String, Object> context = new HashMap<>(queryContextWithGranularity(Granularities.FIVE_MINUTE));
    context.put(CTX_ROWS_PER_SEGMENT, 5432198);
    testIngestionQuery()
        .sql("INSERT INTO fiveMinDs\n" +
             "SELECT * FROM foo")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("fiveMinDs", FOO_SIGNATURE)
        .expectResources(dataSourceWrite("fiveMinDs"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "extra1", "extra2", "extra3", "m1", "m2")
                .context(context)
                .build()
         )
        .verify();
  }

  /**
   * Base test with manually-provided PARTIIONED BY and CLUSTERED BY
   *
   * @see {@link org.apache.druid.sql.calcite.CalciteInsertDmlTest#testInsertWithClusteredBy}
   * for the non-catalog version
   */
  @Test
  public void testInsertWithClusteredBy()
  {
    // Test correctness of the query when only CLUSTERED BY clause is present
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.DOUBLE)
                                                  .add("dim1", ColumnType.STRING)
                                                  .add("ceil_m2", ColumnType.DOUBLE)
                                                  .build();
    testIngestionQuery()
        .sql(
            "INSERT INTO dst\n"
            + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo\n"
            + "PARTITIONED BY FLOOR(__time TO DAY)\n"
            + "CLUSTERED BY floor_m1, dim1 DESC"
        )
        .expectTarget("dst", targetRowSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "v0", "v1")
                .virtualColumns(
                    // The type of m1 comes from the catalog
                    expressionVirtualColumn("v0", "floor(\"m1\")", ColumnType.DOUBLE),
                    expressionVirtualColumn("v1", "ceil(\"m2\")", ColumnType.DOUBLE)
                )
                .orderBy(
                    ImmutableList.of(
                        new ScanQuery.OrderBy("v0", ScanQuery.Order.ASCENDING),
                        new ScanQuery.OrderBy("dim1", ScanQuery.Order.DESCENDING)
                    )
                )
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
         )
        .expectLogicalPlanFrom("insertWithClusteredBy2")
        .verify();
  }

  /**
   * Test when clustering comes from the catalog.
   * For now, the catalog does not allow expressions in cluster keys,
   * only column names. (Since it is hard to parse an expression
   * in the validation phase.)
   *
   * @see {@link org.apache.druid.sql.calcite.CalciteInsertDmlTest#testInsertWithClusteredBy}
   * for the non-catalog version
   */
  @Test
  public void testInsertWithClusteredByFromCatalog()
  {
    // Test correctness of the query when only CLUSTERED BY clause is present
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.DOUBLE)
                                                  .add("dim1", ColumnType.STRING)
                                                  .add("ceil_m2", ColumnType.DOUBLE)
                                                  .build();
    testIngestionQuery()
        .sql(
            "INSERT INTO druid.clusterBy\n"
            + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo"
         )
        .expectTarget("clusterBy", targetRowSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("clusterBy"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "v0", "v1")
                .virtualColumns(
                    // The type of m1 comes from the catalog
                    expressionVirtualColumn("v0", "floor(\"m1\")", ColumnType.DOUBLE),
                    expressionVirtualColumn("v1", "ceil(\"m2\")", ColumnType.DOUBLE)
                )
                .orderBy(
                    ImmutableList.of(
                        new ScanQuery.OrderBy("v0", ScanQuery.Order.ASCENDING),
                        new ScanQuery.OrderBy("dim1", ScanQuery.Order.DESCENDING)
                    )
                )
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
         )
        // Note that the logical plan represents that which is in the SQL parse
        // tree: it does not include info from the catalog, unfortunately.
        .expectLogicalPlanFrom("insertWithCatalogClusteredBy")
        .verify();
  }

  /**
   * Ensure that the user can provide a redundant {@code PARTITION BY}
   * and {@code CLUSTER BY}, as long as they agree with the catalog.
   * <p>
   * This test shows another reason why {@code PARTITION BY} is optional: the
   * user might specify partitioning in the catalog, but leave clustering to be
   * per-query.
   */
  @Test
  public void testInsertWithClusteredByFromCatalogAndExplicit()
  {
    // Test correctness of the query when only CLUSTERED BY clause is present
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.DOUBLE)
                                                  .add("dim1", ColumnType.STRING)
                                                  .add("ceil_m2", ColumnType.DOUBLE)
                                                  .build();
    testIngestionQuery()
        .sql(
            "INSERT INTO druid.clusterBy\n"
            + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo\n"
            + "CLUSTERED BY floor_m1, dim1 DESC"
         )
        .expectTarget("clusterBy", targetRowSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("clusterBy"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "v0", "v1")
                .virtualColumns(
                    // The type of m1 comes from the catalog
                    expressionVirtualColumn("v0", "floor(\"m1\")", ColumnType.DOUBLE),
                    expressionVirtualColumn("v1", "ceil(\"m2\")", ColumnType.DOUBLE)
                )
                .orderBy(
                    ImmutableList.of(
                        new ScanQuery.OrderBy("v0", ScanQuery.Order.ASCENDING),
                        new ScanQuery.OrderBy("dim1", ScanQuery.Order.DESCENDING)
                    )
                )
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
         )
        .expectLogicalPlanFrom("insertWithCatalogClusteredBy2")
        .verify();
  }

  @Test
  public void testInsertWithClusteredByFromCatalogConflict()
  {
    testIngestionQuery()
        .sql(
            "INSERT INTO druid.clusterBy\n"
            + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo\n"
            + "PARTITIONED BY FLOOR(__time TO DAY)\n"
            + "CLUSTERED BY floor_m1 DESC, dim1"
          )
        .expectValidationError(
            SqlPlanningException.class,
            "CLUSTER BY mismatch. Catalog: [`floor_m1`, `dim1` DESC], query: [`floor_m1` DESC, `dim1`]"
         )
        .verify();
  }

  @Test
  public void testInsertWithPartitionedByFromCatalogConflict()
  {
    testIngestionQuery()
        .sql(
            "INSERT INTO druid.clusterBy\n"
            + "SELECT __time, FLOOR(m1) as floor_m1, dim1, CEIL(m2) as ceil_m2 FROM foo\n"
            + "PARTITIONED BY FLOOR(__time TO HOUR)\n"
            + "CLUSTERED BY floor_m1, dim1 DESC"
          )
        .expectValidationError(
            SqlPlanningException.class,
            "PARTITIONED BY mismatch. Catalog: [P1D], query: [PT1H]"
         )
        .verify();
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new DruidModule() {

      @Override
      public void configure(Binder binder)
      {
        // Bindings, if such were possible. But, it isn't yet for tests.
        // binder.bind(MetadataCatalog.CatalogSource.class).toInstance(storage);
        // binder.bind(SchemaRegistry.class).toInstance(storage.schemaRegistry());
        // binder.bind(MetadataCatalog.class).to(CachedMetadataCatalog.class).in(LazySingleton.class);
        // binder.bind(CatalogResolver.class).to(LiveCatalogResolver.class).in(LazySingleton.class);

        // Register the external schema
        // SqlBindings.addSchema(binder, ExternalSchema.class);
      }
    });
  }

  @Override
  public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
  {
    super.finalizeTestFramework(sqlTestFramework);
    try {
      buildInlineTable(sqlTestFramework.queryJsonMapper());
      buildTargetDatasources();
      buildFooDatasource();
    }
    catch (CatalogException e) {
      throw new ISE(e, e.getMessage());
    }
  }

  private void buildInlineTable(ObjectMapper jsonMapper) throws CatalogException
  {
    TableMetadata table = TableBuilder.external("inline")
        .inputSource(toMap(jsonMapper, new InlineInputSource("a,b,1\nc,d,2")))
        .inputFormat(BaseExternTableTest.CSV_FORMAT)
        .column("x", "VARCHAR")
        .column("y", "VARCHAR")
        .column("z", "BIGINT")
        .build();
    storage.tables().create(table);
  }

  private Map<String, Object> toMap(ObjectMapper jsonMapper, Object obj)
  {
    try {
      return jsonMapper.convertValue(obj, ExternalTableDefn.MAP_TYPE_REF);
    }
    catch (Exception e) {
      throw new ISE(e, "bad conversion");
    }
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

  private void buildTargetDatasources()
  {
    TableMetadata spec = TableBuilder.datasource("allDs", "ALL")
        .build();
    createTableMetadata(spec);

    spec = TableBuilder.datasource("hourDs", "PT1H")
        .build();
    createTableMetadata(spec);

    spec = TableBuilder.datasource("fiveMinDs", "PT5M")
        .property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 5_432_198) // Easy to spot in results
        .build();
    createTableMetadata(spec);

    spec = TableBuilder.datasource("clusterBy", "P1D")
        .property(DatasourceDefn.CLUSTER_KEYS_PROPERTY, Arrays.asList(
            new ClusterKeySpec("floor_m1", false),
            new ClusterKeySpec("dim1", true)
         ))
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
        .column("extra2", Columns.BIGINT)
        .column("extra3", Columns.VARCHAR)
        .hiddenColumns(Arrays.asList("dim3", "unique_dim1"))
        .sealed(true)
        .build();
    createTableMetadata(spec);
  }
}
