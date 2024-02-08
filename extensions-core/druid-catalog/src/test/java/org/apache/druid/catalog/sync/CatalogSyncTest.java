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

package org.apache.druid.catalog.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.CatalogException.DuplicateKeyException;
import org.apache.druid.catalog.CatalogException.NotFoundException;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.table.BaseExternTableTest;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test the catalog synchronization mechanisms: direct (reads from the DB),
 * cached (holds a copy of the DB, based on update events) and remote
 * (like cached, but receives events over HTTP.)
 */
public class CatalogSyncTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;
  private ObjectMapper jsonMapper;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    storage = dbFixture.storage;
    jsonMapper = new ObjectMapper();
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  /**
   * Checks validation via the storage API. Detailed error checks
   * are done elsewhere: here we just ensure that they are, in fact, done.
   */
  @Test
  public void testInputValidation()
  {
    // Valid definition
    {
      TableMetadata table = TableBuilder.external("externTable")
          .inputSource(toMap(new InlineInputSource("a\nc")))
          .inputFormat(BaseExternTableTest.CSV_FORMAT)
          .column("a", Columns.STRING)
          .build();
      storage.validate(table);
    }

    // No columns
    {
      TableMetadata table = TableBuilder.external("externTable")
          .inputSource(toMap(new InlineInputSource("a\nc")))
          .inputFormat(BaseExternTableTest.CSV_FORMAT)
          .build();
      assertThrows(IAE.class, () -> storage.validate(table));
    }

    // No format
    {
      TableMetadata table = TableBuilder.external("externTable")
          .inputSource(toMap(new InlineInputSource("a\nc")))
           .column("a", Columns.STRING)
          .build();
      assertThrows(IAE.class, () -> storage.validate(table));
    }
  }

  private Map<String, Object> toMap(Object obj)
  {
    try {
      return jsonMapper.convertValue(obj, ExternalTableDefn.MAP_TYPE_REF);
    }
    catch (Exception e) {
      throw new ISE(e, "bad conversion");
    }
  }

  @Test
  public void testDirect() throws DuplicateKeyException, NotFoundException
  {
    populateCatalog();
    MetadataCatalog catalog = new LocalMetadataCatalog(storage, storage.schemaRegistry());
    verifyInitial(catalog);
    alterCatalog();
    verifyAltered(catalog);
  }

  @Test
  public void testCached() throws CatalogException
  {
    populateCatalog();
    CachedMetadataCatalog catalog = new CachedMetadataCatalog(storage, storage.schemaRegistry(), jsonMapper);
    storage.register(catalog);
    verifyInitial(catalog);
    alterCatalog();
    verifyAltered(catalog);
    editCatalogTable();
    verifyEdited(catalog);

    // Also test the deletion case
    TableId table2 = TableId.datasource("table2");
    storage.tables().delete(table2);
    assertThrows(NotFoundException.class, () -> storage.tables().read(table2));

    List<TableMetadata> tables = catalog.tables(TableId.DRUID_SCHEMA);
    assertEquals(2, tables.size());
    assertEquals("table1", tables.get(0).id().name());
    assertEquals("table3", tables.get(1).id().name());
  }

  @Test
  public void testRemoteWithJson() throws CatalogException
  {
    populateCatalog();
    MockCatalogSync sync = new MockCatalogSync(storage, jsonMapper);
    MetadataCatalog catalog = sync.catalog();
    storage.register(sync);
    verifyInitial(catalog);
    alterCatalog();
    verifyAltered(catalog);
    editCatalogTable();
    verifyEdited(catalog);

    // Also test the deletion case
    TableId table2 = TableId.datasource("table2");
    storage.tables().delete(table2);
    assertThrows(NotFoundException.class, () -> storage.tables().read(table2));

    List<TableMetadata> tables = catalog.tables(TableId.DRUID_SCHEMA);
    assertEquals(2, tables.size());
    assertEquals("table1", tables.get(0).id().name());
    assertEquals("table3", tables.get(1).id().name());
  }

  /**
   * Populate the catalog with a few items using the REST resource.
   * @throws DuplicateKeyException
   */
  private void populateCatalog() throws DuplicateKeyException
  {
    TableMetadata table1 = TableBuilder.datasource("table1", "P1D")
        .timeColumn()
        .column("a", Columns.STRING)
        .build();
    storage.validate(table1);
    storage.tables().create(table1);

    TableMetadata table2 = TableBuilder.datasource("table2", "P1D")
        .timeColumn()
        .column("dim", Columns.STRING)
        .column("measure", Columns.LONG)
        .build();
    storage.validate(table2);
    storage.tables().create(table2);

    TableMetadata table3 = TableBuilder.external("table3")
        .inputFormat(BaseExternTableTest.CSV_FORMAT)
        .inputSource(toMap(new InlineInputSource("a\nc")))
        .column("a", Columns.STRING)
        .build();
    storage.validate(table3);
    storage.tables().create(table3);
  }

  private void verifyInitial(MetadataCatalog catalog)
  {
    {
      TableId id = TableId.datasource("table1");
      TableMetadata table = catalog.getTable(id);
      assertEquals(id, table.id());
      assertTrue(table.updateTime() > 0);

      TableSpec dsSpec = table.spec();
      assertEquals(DatasourceDefn.TABLE_TYPE, dsSpec.type());
      List<ColumnSpec> cols = dsSpec.columns();
      assertEquals(2, cols.size());
      assertEquals(Columns.TIME_COLUMN, cols.get(0).name());
      assertEquals(Columns.LONG, cols.get(0).dataType());
      assertEquals("a", cols.get(1).name());
      assertEquals(Columns.STRING, cols.get(1).dataType());

      DatasourceFacade ds = new DatasourceFacade(catalog.resolveTable(id));
      assertEquals("P1D", ds.segmentGranularityString());
    }
    {
      TableId id = TableId.datasource("table2");
      TableMetadata table = catalog.getTable(id);
      assertEquals(id, table.id());
      assertTrue(table.updateTime() > 0);

      TableSpec dsSpec = table.spec();
      assertEquals(DatasourceDefn.TABLE_TYPE, table.spec().type());
      List<ColumnSpec> cols = dsSpec.columns();
      assertEquals(3, cols.size());
      assertEquals("__time", cols.get(0).name());
      assertEquals(Columns.TIME_COLUMN, cols.get(0).name());
      assertEquals(Columns.LONG, cols.get(0).dataType());
      assertEquals("dim", cols.get(1).name());
      assertEquals(Columns.STRING, cols.get(1).dataType());
      assertEquals("measure", cols.get(2).name());
      assertEquals(Columns.LONG, cols.get(2).dataType());

      DatasourceFacade ds = new DatasourceFacade(catalog.resolveTable(id));
      assertEquals("P1D", ds.segmentGranularityString());
    }

    assertNull(catalog.getTable(TableId.datasource("table3")));
    assertNull(catalog.resolveTable(TableId.datasource("table3")));

    {
      TableId id = TableId.external("table3");
      TableMetadata table = catalog.getTable(id);
      assertEquals(id, table.id());
      assertTrue(table.updateTime() > 0);

      TableSpec inputSpec = table.spec();
      assertEquals(ExternalTableDefn.TABLE_TYPE, inputSpec.type());
      List<ColumnSpec> cols = inputSpec.columns();
      assertEquals(1, cols.size());
      assertEquals("a", cols.get(0).name());
      assertEquals(Columns.STRING, cols.get(0).dataType());

      assertNotNull(inputSpec.properties());
    }

    List<TableMetadata> tables = catalog.tables(TableId.DRUID_SCHEMA);
    assertEquals(2, tables.size());
    assertEquals("table1", tables.get(0).id().name());
    assertEquals("table2", tables.get(1).id().name());

    tables = catalog.tables(TableId.EXTERNAL_SCHEMA);
    assertEquals(1, tables.size());
    assertEquals("table3", tables.get(0).id().name());
  }

  private void alterCatalog() throws DuplicateKeyException, NotFoundException
  {
    // Add a column to table 1
    TableId id1 = TableId.datasource("table1");
    TableMetadata table1 = storage.tables().read(id1);
    assertNotNull(table1);

    TableSpec update1 = TableBuilder.copyOf(table1)
        .column("b", Columns.DOUBLE)
        .buildSpec();
    storage.tables().update(table1.withSpec(update1), table1.updateTime());

    // Create a table 3
    TableMetadata table3 = TableBuilder.datasource("table3", "P1D")
        .timeColumn()
        .column("x", Columns.FLOAT)
        .build();
    storage.tables().create(table3);
  }

  private void verifyAltered(MetadataCatalog catalog)
  {
    {
      TableId id = TableId.datasource("table1");
      TableMetadata table = catalog.getTable(id);

      TableSpec dsSpec = table.spec();
      List<ColumnSpec> cols = dsSpec.columns();
      assertEquals(3, cols.size());
      assertEquals(Columns.TIME_COLUMN, cols.get(0).name());
      assertEquals("a", cols.get(1).name());
      assertEquals("b", cols.get(2).name());
      assertEquals(Columns.DOUBLE, cols.get(2).dataType());
    }
    {
      TableId id = TableId.datasource("table3");
      TableMetadata table = catalog.getTable(id);

      TableSpec dsSpec = table.spec();
      List<ColumnSpec> cols = dsSpec.columns();
      assertEquals(2, cols.size());
      assertEquals(Columns.TIME_COLUMN, cols.get(0).name());
      assertEquals("x", cols.get(1).name());
    }

    List<TableMetadata> tables = catalog.tables(TableId.DRUID_SCHEMA);
    assertEquals(3, tables.size());
    assertEquals("table1", tables.get(0).id().name());
    assertEquals("table2", tables.get(1).id().name());
    assertEquals("table3", tables.get(2).id().name());
  }

  private void editCatalogTable() throws CatalogException
  {
    // Edit table1: add a property
    TableId id = TableId.datasource("table1");
    storage.tables().updateProperties(id, t -> {
      TableSpec target = t.spec();
      Map<String, Object> updated = new HashMap<>(target.properties());
      updated.put("foo", "bar");
      return target.withProperties(updated);
    });

    // Edit table3: add a column
    id = TableId.datasource("table3");
    storage.tables().updateColumns(id, t -> {
      TableSpec target = t.spec();
      List<ColumnSpec> updated = new ArrayList<>(target.columns());
      ColumnSpec colC = new ColumnSpec("c", Columns.DOUBLE, null);
      updated.add(colC);
      return target.withColumns(updated);
    });
  }

  private void verifyEdited(MetadataCatalog catalog)
  {
    {
      TableId id = TableId.datasource("table1");
      DatasourceFacade ds = new DatasourceFacade(catalog.resolveTable(id));
      assertEquals("P1D", ds.segmentGranularityString());
      assertEquals("bar", ds.stringProperty("foo"));
    }
    {
      TableId id = TableId.datasource("table3");
      ResolvedTable table = catalog.resolveTable(id);
      assertEquals(3, table.spec().columns().size());
      assertEquals("c", table.spec().columns().get(2).name());
    }
  }
}
