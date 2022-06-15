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

package org.apache.druid.metadata.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.DatasourceDefn;
import org.apache.druid.catalog.MetastoreManager;
import org.apache.druid.catalog.MetastoreManagerImpl;
import org.apache.druid.catalog.TableId;
import org.apache.druid.catalog.TableSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.catalog.CatalogManager.DuplicateKeyException;
import org.apache.druid.metadata.catalog.CatalogManager.NotFoundException;
import org.apache.druid.metadata.catalog.CatalogManager.OutOfDateException;
import org.apache.druid.metadata.catalog.CatalogManager.TableState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    MetastoreManager metastoreMgr = new MetastoreManagerImpl(
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
      manager.stop();
      manager = null;
    }
  }

  @Test
  public void testCreate() throws DuplicateKeyException
  {
    TableDefnManager tableMgr = manager.tables();

    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .build();
    TableSpec table = TableSpec.newSegmentTable("table1", defn);

    // Table does not exist, read returns nothing.
    assertNull(tableMgr.read(table.id()));

    // Create the table
    long version = tableMgr.create(table);
    TableSpec created = table.fromInsert(table.dbSchema(), version);

    // Read the record
    TableSpec read = tableMgr.read(table.id());
    assertEquals(created, read);

    // Try to create a second time
    try {
      tableMgr.create(table);
      fail();
    }
    catch (DuplicateKeyException e) {
      // Expected
    }
  }

  @Test
  public void testUpdate() throws DuplicateKeyException, OutOfDateException, NotFoundException
  {
    TableDefnManager tableMgr = manager.tables();

    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .build();
    TableSpec table = TableSpec.newSegmentTable("table1", defn);
    long version = tableMgr.create(table);

    // Change the definition
    DatasourceDefn defn2 = DatasourceDefn.builder()
        .segmentGranularity("PT1D")
        .rollupGranularity("PT1H")
        .targetSegmentRows(2_000_000)
        .build();

    try {
      tableMgr.updateDefn(table.id(), defn2, 3);
      fail();
    }
    catch (OutOfDateException e) {
      // expected
    }

    assertEquals(version, tableMgr.read(table.id()).updateTime());
    long newVersion = tableMgr.updateDefn(table.id(), defn2, version);
    TableSpec table3 = tableMgr.read(table.id());
    assertEquals(defn2, table3.defn());
    assertEquals(newVersion, table3.updateTime());

    // Changing the state requires no version check
    assertEquals(TableState.ACTIVE, table3.state());
    newVersion = tableMgr.markDeleting(table.id());
    TableSpec table4 = tableMgr.read(table.id());
    assertEquals(TableState.DELETING, table4.state());
    assertEquals(newVersion, table4.updateTime());

    // Update: no version check)
    long newerVersion = tableMgr.updateDefn(table.id(), defn2);
    assertTrue(newerVersion > newVersion);
  }

  @Test
  public void testDelete() throws DuplicateKeyException
  {
    TableDefnManager tableMgr = manager.tables();

    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .build();
    TableSpec table = TableSpec.newSegmentTable("table1", defn);

    assertFalse(tableMgr.delete(table.id()));
    tableMgr.create(table);
    assertTrue(tableMgr.delete(table.id()));
    assertFalse(tableMgr.delete(table.id()));
  }

  @Test
  public void testList() throws DuplicateKeyException
  {
    TableDefnManager tableMgr = manager.tables();

    List<TableId> list = tableMgr.list();
    assertTrue(list.isEmpty());

    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .build();

    // Create tables in inverse order
    TableSpec table2 = TableSpec.newSegmentTable("table2", defn);
    long version = tableMgr.create(table2);
    table2 = table2.fromInsert(TableId.DRUID_SCHEMA, version);
    TableSpec table1 = TableSpec.newSegmentTable("table1", defn);
    version = tableMgr.create(table1);
    table1 = table1.fromInsert(TableId.DRUID_SCHEMA, version);

    list = tableMgr.list();
    assertEquals(2, list.size());
    TableId id = list.get(0);
    assertEquals(TableId.DRUID_SCHEMA, id.schema());
    assertEquals("table1", id.name());
    id = list.get(1);
    assertEquals(TableId.DRUID_SCHEMA, id.schema());
    assertEquals("table2", id.name());

    List<String> names = tableMgr.list(TableId.DRUID_SCHEMA);
    assertEquals(2, names.size());

    names = tableMgr.list(TableId.SYSTEM_SCHEMA);
    assertEquals(0, names.size());

    List<TableSpec> details = tableMgr.listDetails(TableId.DRUID_SCHEMA);
    assertEquals(Arrays.asList(table1, table2), details);
  }
}
