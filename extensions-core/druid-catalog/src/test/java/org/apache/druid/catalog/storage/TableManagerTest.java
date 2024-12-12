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

package org.apache.druid.catalog.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.CatalogException.DuplicateKeyException;
import org.apache.druid.catalog.CatalogException.NotFoundException;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.storage.sql.CatalogManager;
import org.apache.druid.catalog.storage.sql.SQLCatalogManager;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(CatalogTest.class)
public class TableManagerTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
          new TestDerbyConnector.DerbyConnectorRule();
  private CatalogManager manager;

  @Before
  public void setUp()
  {
    MetadataStorageManager metastoreMgr = new MetadataStorageManager(
        JSON_MAPPER,
        derbyConnectorRule.getConnector(),
        () -> derbyConnectorRule.getMetadataConnectorConfig(),
        derbyConnectorRule.metadataTablesConfigSupplier()
        );
    manager = new SQLCatalogManager(metastoreMgr);
    manager.start();
  }

  @After
  public void tearDown()
  {
    if (manager != null) {
      manager = null;
    }
  }

  @Test
  public void testCreate() throws DuplicateKeyException, NotFoundException
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(TableId.datasource("table1"), spec);

    // Table does not exist, read throws an exception.
    assertThrows(NotFoundException.class, () -> manager.read(table.id()));

    // Create the table
    long version = manager.create(table);
    TableMetadata created = table.fromInsert(version);

    // Read the record
    TableMetadata read = manager.read(table.id());
    assertEquals(created, read);

    // Try to create a second time
    assertThrows(DuplicateKeyException.class, () -> manager.create(table));
  }

  @Test
  public void testUpdate() throws DuplicateKeyException, NotFoundException
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(TableId.datasource("table1"), spec);
    final long version1 = manager.create(table);

    // Change the definition
    props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 2_000_000
    );
    TableSpec spec2 = spec.withProperties(props);
    TableMetadata table2 = table.withSpec(spec2);
    assertThrows(NotFoundException.class, () -> manager.update(table2, 3));

    assertEquals(version1, manager.read(table.id()).updateTime());
    final long version2 = manager.update(table2, version1);
    TableMetadata read = manager.read(table.id());
    assertEquals(spec2, read.spec());
    assertEquals(version2, read.updateTime());

    // Replace
    TableMetadata table3 = table.withSpec(spec2);
    final long version3 = manager.replace(table3);
    assertTrue(version3 > version2);
    read = manager.read(table.id());
    assertEquals(spec2, read.spec());
    assertEquals(version3, read.updateTime());

    // Changing the state requires no version check
    assertEquals(TableMetadata.TableState.ACTIVE, read.state());
    long version4 = manager.markDeleting(table.id());
    read = manager.read(table.id());
    assertEquals(TableMetadata.TableState.DELETING, read.state());
    assertEquals(version4, read.updateTime());

    // Can't update when deleting
    assertThrows(NotFoundException.class, () -> manager.update(table3, version4));

    // Can't replace when deleting
    assertThrows(NotFoundException.class, () -> manager.replace(table3));
  }

  @Test
  public void testUpdateProperties() throws CatalogException
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(TableId.datasource("table1"), spec);
    final long version1 = manager.create(table);

    // Transform properties by adding a new one
    final long version2 = manager.updateProperties(table.id(), t -> {
      TableSpec target = t.spec();
      Map<String, Object> updated = new HashMap<>(target.properties());
      updated.put("foo", "bar");
      return target.withProperties(updated);
    });
    assertTrue(version2 > version1);

    TableMetadata read = manager.read(table.id());
    assertEquals(version2, read.updateTime());
    Map<String, Object> expected = new HashMap<>(props);
    expected.put("foo", "bar");
    assertEquals(expected, read.spec().properties());

    // Not found
    assertThrows(
        NotFoundException.class,
        () -> manager.updateProperties(TableId.datasource("bogus"), t -> t.spec())
    );

    // No update
    final long version3 = manager.updateProperties(table.id(), t -> null);
    assertEquals(0, version3);

    // Update fails if table is in the Deleting state
    manager.markDeleting(table.id());
    assertThrows(
        NotFoundException.class,
        () -> manager.updateProperties(table.id(), t -> t.spec())
    );
  }

  @Test
  public void testUpdateColumns() throws CatalogException
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("a", Columns.STRING, null),
        new ColumnSpec("b", Columns.LONG, null)
    );
    ColumnSpec colC = new ColumnSpec("c", Columns.DOUBLE, null);

    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, cols);
    TableMetadata table = TableMetadata.newTable(TableId.datasource("table1"), spec);
    final long version1 = manager.create(table);

    // Transform columns by adding a new one
    final long version2 = manager.updateColumns(table.id(), t -> {
      TableSpec target = t.spec();
      List<ColumnSpec> updated = new ArrayList<>(target.columns());
      updated.add(colC);
      return target.withColumns(updated);
    });
    assertTrue(version2 > version1);

    TableMetadata read = manager.read(table.id());
    assertEquals(version2, read.updateTime());
    List<ColumnSpec> expected = new ArrayList<>(cols);
    expected.add(colC);
    assertEquals(expected, read.spec().columns());

    // Not found
    assertThrows(
        NotFoundException.class,
        () -> manager.updateColumns(TableId.datasource("bogus"), t -> t.spec())
    );

    // No update
    final long version3 = manager.updateColumns(table.id(), t -> null);
    assertEquals(0, version3);

    // Update fails if table is in the Deleting state
    manager.markDeleting(table.id());
    assertThrows(
        NotFoundException.class,
        () -> manager.updateColumns(table.id(), t -> t.spec())
    );
  }

  @Test
  public void testDelete() throws DuplicateKeyException, NotFoundException
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(TableId.datasource("table1"), spec);

    assertThrows(NotFoundException.class, () -> manager.delete(table.id()));
    manager.create(table);
    manager.delete(table.id());
    assertThrows(NotFoundException.class, () -> manager.delete(table.id()));
  }

  @Test
  public void testList() throws DuplicateKeyException
  {
    List<TableId> list = manager.allTablePaths();
    assertTrue(list.isEmpty());

    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H",
        DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);

    // Create tables in inverse order
    TableMetadata table2 = TableMetadata.newTable(TableId.datasource("table2"), spec);
    long version = manager.create(table2);
    table2 = table2.fromInsert(version);
    TableMetadata table1 = TableMetadata.newTable(TableId.datasource("table1"), spec);
    version = manager.create(table1);
    table1 = table1.fromInsert(version);

    list = manager.allTablePaths();
    assertEquals(2, list.size());
    TableId id = list.get(0);
    assertEquals(TableId.DRUID_SCHEMA, id.schema());
    assertEquals("table1", id.name());
    id = list.get(1);
    assertEquals(TableId.DRUID_SCHEMA, id.schema());
    assertEquals("table2", id.name());

    List<String> names = manager.tableNamesInSchema(TableId.DRUID_SCHEMA);
    assertEquals(2, names.size());

    names = manager.tableNamesInSchema(TableId.SYSTEM_SCHEMA);
    assertEquals(0, names.size());

    List<TableMetadata> details = manager.tablesInSchema(TableId.DRUID_SCHEMA);
    assertEquals(Arrays.asList(table1, table2), details);
  }
}
