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

import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.CachedMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.CalciteIngestionDmlTest;
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
   * If the segment grain is given in the catalog then use this value is used.
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
   * If the segment grain is given in the catalog, and also by PARTITIONED BY, then
   * the query value is used.
   */
  @Test
  public void testInsertHourGrainWithDay()
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
}
