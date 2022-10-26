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
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.AbstractDatasourceDefn;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.storage.sql.CatalogManager;
import org.apache.druid.catalog.storage.sql.CatalogManager.DuplicateKeyException;
import org.apache.druid.catalog.storage.sql.CatalogManager.NotFoundException;
import org.apache.druid.catalog.storage.sql.CatalogManager.OutOfDateException;
import org.apache.druid.catalog.storage.sql.SQLCatalogManager;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
  public void testCreate() throws DuplicateKeyException
  {
    Map<String, Object> props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(TableId.datasource("table1"), spec);

    // Table does not exist, read returns nothing.
    assertNull(manager.read(table.id()));

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
  public void testUpdate() throws DuplicateKeyException, OutOfDateException, NotFoundException
  {
    Map<String, Object> props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(TableId.datasource("table1"), spec);
    long version = manager.create(table);

    // Change the definition
    props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 2_000_000
    );
    TableSpec spec2 = spec.withProperties(props);
    TableMetadata table2 = table.withSpec(spec2);
    assertThrows(OutOfDateException.class, () -> manager.update(table2, 3));

    assertEquals(version, manager.read(table.id()).updateTime());
    long newVersion = manager.update(table2, version);
    TableMetadata table3 = manager.read(table.id());
    assertEquals(spec2, table3.spec());
    assertEquals(newVersion, table3.updateTime());

    // Changing the state requires no version check
    assertEquals(TableMetadata.TableState.ACTIVE, table3.state());
    newVersion = manager.markDeleting(table.id());
    TableMetadata table4 = manager.read(table.id());
    assertEquals(TableMetadata.TableState.DELETING, table4.state());
    assertEquals(newVersion, table4.updateTime());

    // Update: no version check)
    TableMetadata table5 = table.withSpec(spec2);
    long newerVersion = manager.update(table5, 0);
    assertTrue(newerVersion > newVersion);
  }

  @Test
  public void testDelete() throws DuplicateKeyException
  {
    Map<String, Object> props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D",
        AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(TableId.datasource("table1"), spec);

    assertFalse(manager.delete(table.id()));
    manager.create(table);
    assertTrue(manager.delete(table.id()));
    assertFalse(manager.delete(table.id()));
  }

  @Test
  public void testList() throws DuplicateKeyException
  {
    List<TableId> list = manager.list();
    assertTrue(list.isEmpty());

    Map<String, Object> props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H",
        AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);

    // Create tables in inverse order
    TableMetadata table2 = TableMetadata.newTable(TableId.datasource("table2"), spec);
    long version = manager.create(table2);
    table2 = table2.fromInsert(version);
    TableMetadata table1 = TableMetadata.newTable(TableId.datasource("table1"), spec);
    version = manager.create(table1);
    table1 = table1.fromInsert(version);

    list = manager.list();
    assertEquals(2, list.size());
    TableId id = list.get(0);
    assertEquals(TableId.DRUID_SCHEMA, id.schema());
    assertEquals("table1", id.name());
    id = list.get(1);
    assertEquals(TableId.DRUID_SCHEMA, id.schema());
    assertEquals("table2", id.name());

    List<String> names = manager.list(TableId.DRUID_SCHEMA);
    assertEquals(2, names.size());

    names = manager.list(TableId.SYSTEM_SCHEMA);
    assertEquals(0, names.size());

    List<TableMetadata> details = manager.listDetails(TableId.DRUID_SCHEMA);
    assertEquals(Arrays.asList(table1, table2), details);
  }
}
