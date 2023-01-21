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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.sql.LiveCatalogResolver;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.CachedMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class MSQCatalogInsertTest extends MSQTestBase
{
  @ClassRule
  public static final TestDerbyConnector.DerbyConnectorRule DERBY_CONNECTION_RULE =
      new TestDerbyConnector.DerbyConnectorRule();

  private static CatalogStorage storage;
  private static MetadataCatalog catalog;

  @BeforeClass
  public static void setupCatalog()
  {
    CatalogTests.DbFixture dbFixture = new CatalogTests.DbFixture(DERBY_CONNECTION_RULE);
    storage = dbFixture.storage;
    catalog = new CachedMetadataCatalog(
        storage,
        storage.schemaRegistry(),
        storage.jsonMapper()
    );

    TableMetadata spec = TableBuilder.datasource("simpleTypes", "P1D")
        .timeColumn()
        .column("string_col", Columns.VARCHAR)
        .column("long_col", Columns.BIGINT)
        .column("float_col", Columns.FLOAT)
        .column("double_col", Columns.DOUBLE)
        .build();
    createTableMetadata(spec);
  }

  @Override
  protected CatalogResolver createMockCatalogResolver()
  {
    return new LiveCatalogResolver(catalog);
  }

  private static void createTableMetadata(TableMetadata table)
  {
    try {
      storage.tables().create(table);
    }
    catch (CatalogException e) {
      fail(e.getMessage());
    }
  }

  /**
   * Baseline test: insert into a datasource inferring column types from the SELECT
   * statement, which, in turn, infers the types from the external table, in this case,
   * and inline external table.
   */
  @Test
  public void testInsertWithoutCatalog()
  {
    // Expected segment signature in the controller task, derived from the
    // SELECT row type and used to create segments.
    // In this test, the types are the same as the SELECT.
    Map<String, String> expectedStorageTypes = ImmutableMap.of(
        "__time", "LONG",
        "string_col", "STRING",
        "long_col", "LONG",
        "float_col", "FLOAT",
        "double_col", "DOUBLE"
        );

    // Expected signature of actual segment rows
    // Should be the same as the storage types.
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("string_col", ColumnType.STRING)
                                            .add("long_col", ColumnType.LONG)
                                            .add("float_col", ColumnType.FLOAT)
                                            .add("double_col", ColumnType.DOUBLE)
                                            .build();

    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{1674223620000L, "foo", 10L, 20.5F, 30.5D}
    );

    testIngestQuery().setSql(
                         "INSERT INTO testTarget1\n" +
                         "SELECT time_parse(time_col) as __time, string_col, long_col, float_col, double_col\n" +
                         "FROM TABLE(inline(\n" +
                         "  data => ARRAY['2023-01-20T14:07:00,foo,10,20.5,30.5'],\n" +
                         "  format => 'csv'))\n" +
                         "  (time_col VARCHAR, string_col VARCHAR, long_col BIGINT, float_col FLOAT, double_col DOUBLE)\n" +
                         "PARTITIONED BY ALL"
                      )
                     .setExpectedDataSource("testTarget1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedStorageTypes(expectedStorageTypes)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();
  }

  /**
   * Simple test of the target column types retrieved from the catalog. Tests the simple
   * types. Input data is of a type other than the target type, so the SELECT row type
   * will reflect the input types. The validator will impose the desired target types
   * and the appenderator will convert the types returned from SELECT to the types
   * that the catalog has imposed.
   */
  @Test
  public void testInsertSimpleTypesWithCatalog()
  {
    // Desired schema of the columns produced in the output segment.
    // In this test, the types come from the catalog definition.
    Map<String, String> expectedStorageTypes = ImmutableMap.of(
        "__time", "LONG",
        "string_col", "STRING",
        "long_col", "LONG",
        "float_col", "FLOAT",
        "double_col", "DOUBLE"
        );

    // Signature of actual segment rows. Should match the imposed
    // schema above.
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("string_col", ColumnType.STRING)
                                            .add("long_col", ColumnType.LONG)
                                            .add("float_col", ColumnType.FLOAT)
                                            .add("double_col", ColumnType.DOUBLE)
                                            .build();

    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{1674223620000L, "foo", 10L, 20F, 30D}
    );

    testIngestQuery().setSql(
                         "INSERT INTO simpleTypes\n" +
                         "SELECT time_parse(time_col) as __time, string_col, long_col, float_col, double_col\n" +
                         "FROM TABLE(inline(\n" +
                         "  data => ARRAY['2023-01-20T14:07:00,foo,10.0,20,30'],\n" +
                         "  format => 'csv'))\n" +
                         "  (time_col VARCHAR, string_col VARCHAR, long_col FLOAT, float_col BIGINT, double_col BIGINT)"
                      )
                     .setExpectedDataSource("simpleTypes")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedStorageTypes(expectedStorageTypes)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();
  }
}
