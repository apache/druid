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

package org.apache.druid.testsEx.catalog;

import com.google.inject.Inject;
import org.apache.druid.catalog.http.TableEditRequest.DropColumns;
import org.apache.druid.catalog.http.TableEditRequest.HideColumns;
import org.apache.druid.catalog.http.TableEditRequest.MoveColumn;
import org.apache.druid.catalog.http.TableEditRequest.UnhideColumns;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.AbstractDatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testsEx.categories.Catalog;
import org.apache.druid.testsEx.cluster.CatalogClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Light sanity check of the Catalog REST API. Functional testing is
 * done via a unit test. Here we simply ensure that the Jersey plumbing
 * works as intended.
 */
@RunWith(DruidTestRunner.class)
@Category(Catalog.class)
public class ITCatalogRestTest
{
  @Inject
  private DruidClusterClient clusterClient;

  /**
   * Sample a few error cases to ensure the plumbing works.
   * Complete error testing appears in unit tests.
   */
  @Test
  public void testErrors()
  {
    CatalogClient client = new CatalogClient(clusterClient);

    // Bogus schema
    {
      final TableMetadata table = new TableBuilder()
          .id(TableId.of("bogus", "foo"))
          .build();

      assertThrows(
          Exception.class,
          () -> client.createTable(table, false)
      );
    }

    // Read-only schema
    {
      final TableMetadata table = new TableBuilder()
          .id(TableId.of(TableId.SYSTEM_SCHEMA, "foo"))
          .property(AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D")
          .build();
      assertThrows(
          Exception.class,
          () -> client.createTable(table, false)
      );
    }

    // Malformed table name
    {
      final TableMetadata table = TableBuilder.datasource(" foo ", "P1D")
          .build();
      assertThrows(
          Exception.class,
          () -> client.createTable(table, false)
      );
    }
  }

  /**
   * Run though a table lifecycle to sanity check each API. Thorough
   * testing of each API appears in unit tests.
   */
  @Test
  public void testLifecycle()
  {
    CatalogClient client = new CatalogClient(clusterClient);

    // Create a datasource
    TableMetadata table = TableBuilder.datasource("example", "P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();

    // Use force action so test is reentrant if it fails part way through
    // when debugging.
    long version = client.createTable(table, true);

    // Update the datasource
    TableSpec dsSpec2 = TableBuilder.copyOf(table)
        .property(AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 3_000_000)
        .column("d", "DOUBLE")
        .buildSpec();

    // First, optimistic locking, wrong version
    assertThrows(ISE.class, () -> client.updateTable(table.id(), dsSpec2, 1));

    // Optimistic locking, correct version
    long newVersion = client.updateTable(table.id(), dsSpec2, version);
    assertTrue(newVersion > version);

    // Verify the update
    TableMetadata read = client.readTable(table.id());
    assertEquals(dsSpec2, read.spec());

    // Move a column
    MoveColumn moveCmd = new MoveColumn("d", MoveColumn.Position.BEFORE, "a");
    client.editTable(table.id(), moveCmd);

    // Drop a column
    DropColumns dropCmd = new DropColumns(Collections.singletonList("b"));
    client.editTable(table.id(), dropCmd);
    read = client.readTable(table.id());
    assertEquals(Arrays.asList("d", "a", "c"), CatalogUtils.columnNames(read.spec().columns()));

    // Hide columns
    HideColumns hideCmd = new HideColumns(
        Arrays.asList("e", "f")
    );
    client.editTable(table.id(), hideCmd);
    read = client.readTable(table.id());
    assertEquals(
          Arrays.asList("e", "f"),
          read.spec().properties().get(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Unhide
    UnhideColumns unhideCmd = new UnhideColumns(
        Collections.singletonList("e")
    );
    client.editTable(table.id(), unhideCmd);
    read = client.readTable(table.id());
    assertEquals(
          Collections.singletonList("f"),
          read.spec().properties().get(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // List schemas
    List<String> schemaNames = client.listSchemas();
    assertTrue(schemaNames.contains(TableId.DRUID_SCHEMA));
    assertTrue(schemaNames.contains(TableId.EXTERNAL_SCHEMA));
    assertTrue(schemaNames.contains(TableId.SYSTEM_SCHEMA));
    assertTrue(schemaNames.contains(TableId.CATALOG_SCHEMA));

    // List table names in schema
    List<String> tableNames = client.listTableNamesInSchema(TableId.DRUID_SCHEMA);
    assertTrue(tableNames.contains(table.id().name()));

    // List tables
    List<TableId> tables = client.listTables();
    assertTrue(tables.contains(table.id()));

    // Drop the table
    client.dropTable(table.id());
    tableNames = client.listTableNamesInSchema(TableId.DRUID_SCHEMA);
    assertFalse(tableNames.contains(table.id().name()));
  }
}
