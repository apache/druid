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
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.BaseExternTableTest;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.LocalMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.Builder;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * Test catalog integration with the information schema tables. The only
 * test fixture in this path is the direct catalog interface instead of the
 * cached version in a production server.
 */
public class CatalogSchemaTest extends BaseCalciteQueryTest
{
  @ClassRule
  public static final TestDerbyConnector.DerbyConnectorRule DERBY_CONNECTION_RULE =
      new TestDerbyConnector.DerbyConnectorRule();

  private static CatalogStorage storage;

  /**
   * Catalog tables should appear in the list of datasources.
   */
  @Test
  public void testTablesDatasources()
  {
    Object[][] expected = new Object[][] {
        new Object[] {"druid", "druid", "broadcast", "TABLE", "YES", "YES"},
        new Object[] {"druid", "druid", "foo", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "foo2", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "foo4", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "forbiddenDatasource", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "lotsocolumns", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "numfoo", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "some_datasource", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "somexdatasource", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "test_with_cols", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "test_without_cols", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "visits", "TABLE", "NO", "NO"},
        new Object[] {"druid", "druid", "wikipedia", "TABLE", "NO", "NO"}
    };
    testBuilder()
      .sql(
          "SELECT *\n" +
          "FROM INFORMATION_SCHEMA.TABLES\n" +
          "WHERE TABLE_SCHEMA = 'druid'\n" +
          "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME"
       )
      .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
      .expectedResults(Arrays.asList(expected))
      .run();
  }

  /**
   * The external schema ("ext") should appear in the list of schemas.
   */
  @Test
  public void testExternalSchema()
  {
    Object[][] expected = new Object[][] {
        new Object[] {"druid", "INFORMATION_SCHEMA", null, null, null, null, null},
        new Object[] {"druid", "druid", null, null, null, null, null},
        // Goal: that the ext (external) schema appears
        new Object[] {"druid", "ext", null, null, null, null, null},
        new Object[] {"druid", "lookup", null, null, null, null, null},
        new Object[] {"druid", "sys", null, null, null, null, null},
        new Object[] {"druid", "view", null, null, null, null, null}
    };
    testBuilder()
      .sql(
          "SELECT *\n" +
          "FROM INFORMATION_SCHEMA.SCHEMATA\n" +
          "ORDER BY CATALOG_NAME, SCHEMA_NAME"
       )
      .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
      .expectedResults(Arrays.asList(expected))
      .run();
  }

  /**
   * External tables should appear in the lists of tables. Those that take parameters
   * are actually functions, but Druid merges tables and functions into the TABLES
   * table.
   */
  @Test
  public void testTablesExternal()
  {
    Object[][] expected = new Object[][] {
        new Object[] {"druid", "ext", "test_inline", "EXTERNAL", "NO", "NO"}
    };
    testBuilder()
      .sql(
          "SELECT *\n" +
          "FROM INFORMATION_SCHEMA.TABLES\n" +
          "WHERE TABLE_SCHEMA = 'ext'\n" +
          "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME"
       )
      .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
      .expectedResults(Arrays.asList(expected))
      .run();
  }

  /**
   * Verify that external table columns appear in the COLUMNS table.
   */
  @Test
  public void testColumnsExternal()
  {
    Object[][] expected = new Object[][] {
        new Object[] {"druid", "ext", "test_inline", "w", 1L, "", "YES", "VARCHAR", null, null, null, null, null, null, "UTF-16LE", "UTF-16LE$en_US$primary", 12L},
        new Object[] {"druid", "ext", "test_inline", "x", 2L, "", "NO", "BIGINT", null, null, 19L, 10L, 0L, null, null, null, -5L},
        new Object[] {"druid", "ext", "test_inline", "y", 3L, "", "NO", "FLOAT", null, null, 15L, 10L, -2147483648L, null, null, null, 6L},
        new Object[] {"druid", "ext", "test_inline", "z", 4L, "", "NO", "DOUBLE", null, null, 15L, 10L, -2147483648L, null, null, null, 8L}
    };
    testBuilder()
      .sql(
          "SELECT *\n" +
          "FROM INFORMATION_SCHEMA.COLUMNS\n" +
          "WHERE TABLE_SCHEMA = 'ext'\n" +
          "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
       )
      .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
      .expectedResults(Arrays.asList(expected))
      .run();
  }

  /**
   * Verify that the columns of a "no-data", catalog-defined datasource appear in the
   * COLUMNS table. With the catalog, a column exists whether or not it has data.
   */
  @Test
  public void testColumnsNewDSWithCols()
  {
    Object[][] expected = new Object[][] {
        new Object[] {"druid", "druid", "test_with_cols", "__time", 1L, "", "NO", "TIMESTAMP", null, null, null, null, null, 3L, null, null, 93L},
        new Object[] {"druid", "druid", "test_with_cols", "a", 2L, "", "YES", "VARCHAR", null, null, null, null, null, null, "UTF-16LE", "UTF-16LE$en_US$primary", 12L},
        new Object[] {"druid", "druid", "test_with_cols", "b", 3L, "", "NO", "BIGINT", null, null, 19L, 10L, 0L, null, null, null, -5L},
        new Object[] {"druid", "druid", "test_with_cols", "c", 4L, "", "NO", "FLOAT", null, null, 15L, 10L, -2147483648L, null, null, null, 6L},
        new Object[] {"druid", "druid", "test_with_cols", "d", 5L, "", "NO", "DOUBLE", null, null, 15L, 10L, -2147483648L, null, null, null, 8L}
    };
    testBuilder()
      .sql(
          "SELECT *\n" +
          "FROM INFORMATION_SCHEMA.COLUMNS\n" +
          "WHERE TABLE_NAME = 'test_with_cols'\n" +
          "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
       )
      .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
      .expectedResults(Arrays.asList(expected))
      .run();
  }

  /**
   * Catalog-only datasource table with no columns.
   */
  @Test
  public void testColumnsNewDSWithoutCols()
  {
    // When defined in the catalog, a table at least has a
    // time column, even if not explicitly defined.
    Object[][] expected = new Object[][] {
        new Object[] {"druid", "druid", "test_without_cols", "__time", 1L, "", "NO", "TIMESTAMP", null, null, null, null, null, 3L, null, null, 93L}
    };
    testBuilder()
      .sql(
          "SELECT *\n" +
          "FROM INFORMATION_SCHEMA.COLUMNS\n" +
          "WHERE TABLE_NAME = 'test_without_cols'\n" +
          "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
       )
      .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
      .expectedResults(Arrays.asList(expected))
      .run();
  }

  /**
   * Test that the "blended" schema appears in the COLUMNS table.
   * Some columns here are defined only in the catalog, some only in
   * the segments. Catalog columns come first, then datasource columns,
   * except for those that are hidden.
   */
  @Test
  public void testColumnsExistingDS()
  {
    Object[][] expected = new Object[][] {
        new Object[] {"druid", "druid", "foo", "__time", 1L, "", "NO", "TIMESTAMP", null, null, null, null, null, 3L, null, null, 93L},
        new Object[] {"druid", "druid", "foo", "extra1", 2L, "", "YES", "VARCHAR", null, null, null, null, null, null, "UTF-16LE", "UTF-16LE$en_US$primary", 12L},
        new Object[] {"druid", "druid", "foo", "dim2", 3L, "", "YES", "VARCHAR", null, null, null, null, null, null, "UTF-16LE", "UTF-16LE$en_US$primary", 12L},
        new Object[] {"druid", "druid", "foo", "dim1", 4L, "", "YES", "VARCHAR", null, null, null, null, null, null, "UTF-16LE", "UTF-16LE$en_US$primary", 12L},
        new Object[] {"druid", "druid", "foo", "cnt", 5L, "", "NO", "BIGINT", null, null, 19L, 10L, 0L, null, null, null, -5L},
        new Object[] {"druid", "druid", "foo", "m1", 6L, "", "NO", "DOUBLE", null, null, 15L, 10L, -2147483648L, null, null, null, 8L},
        new Object[] {"druid", "druid", "foo", "extra2", 7L, "", "NO", "BIGINT", null, null, 19L, 10L, 0L, null, null, null, -5L},
        new Object[] {"druid", "druid", "foo", "extra3", 8L, "", "YES", "VARCHAR", null, null, null, null, null, null, "UTF-16LE", "UTF-16LE$en_US$primary", 12L},
        new Object[] {"druid", "druid", "foo", "m2", 9L, "", "NO", "DOUBLE", null, null, 15L, 10L, -2147483648L, null, null, null, 8L}
    };
    testBuilder()
      .sql(
          "SELECT *\n" +
          "FROM INFORMATION_SCHEMA.COLUMNS\n" +
          "WHERE TABLE_NAME = 'foo'\n" +
          "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
       )
      .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
      .expectedResults(Arrays.asList(expected))
      .run();
  }

  @Override
  protected void configureBuilder(Builder builder)
  {
    super.configureBuilder(builder);
    CatalogTests.DbFixture dbFixture = new CatalogTests.DbFixture(DERBY_CONNECTION_RULE);
    storage = dbFixture.storage;
    MetadataCatalog catalog = new LocalMetadataCatalog(
        storage,
        storage.schemaRegistry()
    );
    builder.catalogResolver(new LiveCatalogResolver(catalog));
    builder.extraSchema(new ExternalSchema(catalog, storage.jsonMapper()));
  }

  @Override
  public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
  {
    super.finalizeTestFramework(sqlTestFramework);
    try {
      buildExternalTable(sqlTestFramework.queryJsonMapper());
      buildNewDatasources();
      buildExistingDatasource();
    }
    catch (CatalogException e) {
      throw new ISE(e, e.getMessage());
    }
  }

  private void buildExternalTable(ObjectMapper jsonMapper) throws CatalogException
  {
    TableMetadata table = TableBuilder.external("test_inline")
        .inputSource(toMap(jsonMapper, new InlineInputSource("a,b,1\nc,d,2")))
        .inputFormat(BaseExternTableTest.CSV_FORMAT)
        .column("w", Columns.STRING)
        .column("x", Columns.LONG)
        .column("y", Columns.FLOAT)
        .column("z", Columns.DOUBLE)
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

  private void buildNewDatasources()
  {
    TableMetadata spec = TableBuilder.datasource("test_with_cols", "PT1H")
        .timeColumn()
        .column("a", Columns.STRING)
        .column("b", Columns.LONG)
        .column("c", Columns.FLOAT)
        .column("d", Columns.DOUBLE)
        .build();
    createTableMetadata(spec);

    spec = TableBuilder.datasource("test_without_cols", "PT5M")
        .build();
    createTableMetadata(spec);
  }

  public void buildExistingDatasource()
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
}
