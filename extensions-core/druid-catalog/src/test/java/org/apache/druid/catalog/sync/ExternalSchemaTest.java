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

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.BaseExternTableTest;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.HttpInputSourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.sql.ExternalSchema;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.table.ExternalTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test of the external schema. Creates an external schema
 * directly and via a cached catalog, to ensure changes are propagated.
 * Then, ensures that parameterized tables work, at least at the mechanical
 * level.
 */
public class ExternalSchemaTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    storage = dbFixture.storage;
    storage.jsonMapper().setInjectableValues(new Std().addValue(HttpInputSourceConfig.class, new HttpInputSourceConfig(null)));
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  private ExternalSchema externalSchema(MetadataCatalog catalog)
  {
    return new ExternalSchema(catalog, storage.jsonMapper());
  }

  @Test
  public void testDirect() throws CatalogException
  {
    populateCatalog();
    MetadataCatalog catalog = new LocalMetadataCatalog(storage, storage.schemaRegistry());
    ExternalSchema schema = externalSchema(catalog);
    verifyInitial(schema);
    alterCatalog();
    verifyAltered(schema);
  }

  @Test
  public void testCached() throws CatalogException
  {
    populateCatalog();
    CachedMetadataCatalog catalog = new CachedMetadataCatalog(
        storage,
        storage.schemaRegistry(),
        storage.jsonMapper()
    );
    storage.register(catalog);
    ExternalSchema schema = externalSchema(catalog);
    verifyInitial(schema);
    alterCatalog();
    verifyAltered(schema);
  }

  private void populateCatalog() throws CatalogException
  {
    TableMetadata table = TableBuilder.external("input1")
        .inputSource(toMap(new InlineInputSource("a\nc")))
        .inputFormat(BaseExternTableTest.CSV_FORMAT)
        .column("a", "varchar")
        .build();
    storage.tables().create(table);
  }

  private Map<String, Object> toMap(Object obj)
  {
    try {
      return dbFixture.storage.jsonMapper().convertValue(obj, ExternalTableDefn.MAP_TYPE_REF);
    }
    catch (Exception e) {
      throw new ISE(e, "bad conversion");
    }
  }

  private void verifyInitial(ExternalSchema schema)
  {
    assertNull(schema.getTable("input2"));

    Table table = schema.getTable("input1");
    assertTrue(table instanceof ExternalTable);
    assertEquals(1, ((ExternalTable) table).getRowSignature().size());

    Set<String> names = schema.getTableNames();
    assertEquals(1, names.size());
    assertTrue(names.contains("input1"));
  }

  private void alterCatalog() throws CatalogException
  {
    // Add a column to table 1
    TableId id1 = TableId.external("input1");
    TableMetadata table1 = storage.tables().read(id1);
    assertNotNull(table1);

    TableMetadata defn = TableBuilder.copyOf(table1)
        .column("b", "DOUBLE")
        .build();
    storage.tables().update(defn, table1.updateTime());

    // Create a table 2
    TableMetadata table = TableBuilder.external("input2")
        .inputSource(toMap(new InlineInputSource("1\2c")))
        .inputFormat(BaseExternTableTest.CSV_FORMAT)
        .column("x", "bigint")
        .build();
    storage.tables().create(table);
  }

  private void verifyAltered(ExternalSchema schema)
  {
    Table table = schema.getTable("input1");
    assertTrue(table instanceof ExternalTable);
    assertEquals(2, ((ExternalTable) table).getRowSignature().size());

    table = schema.getTable("input2");
    assertTrue(table instanceof ExternalTable);
    assertEquals(1, ((ExternalTable) table).getRowSignature().size());

    Set<String> names = schema.getTableNames();
    assertEquals(2, names.size());
    assertTrue(names.contains("input1"));
    assertTrue(names.contains("input2"));
  }

  private void createParameterizedTable() throws CatalogException
  {
    TableMetadata table = TableBuilder.external("httpParam")
        .inputSource(ImmutableMap.of("type", HttpInputSource.TYPE_KEY))
        .inputFormat(BaseExternTableTest.CSV_FORMAT)
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "http://koalas.com/{}.csv")
        .column("a", "varchar")
        .build();
    storage.tables().create(table);
  }

  @Test
  public void testParameterizedFn() throws CatalogException
  {
    populateCatalog();
    createParameterizedTable();
    CachedMetadataCatalog catalog = new CachedMetadataCatalog(
        storage,
        storage.schemaRegistry(),
        storage.jsonMapper()
    );
    storage.register(catalog);
    ExternalSchema schema = externalSchema(catalog);

    // Non-parameterized table, so no arguments. Using the table function form
    // doesn't accomplish much, but no harm in doing so.
    //
    // This is not a full check of the external data source
    // conversion; just a sanity check that the values are passed along
    // as expected.
    {
      // There should be a dynamic function for our table
      Collection<Function> fns = schema.getFunctions("input1");
      assertEquals(1, fns.size());
      Function fn = Iterators.getOnlyElement(fns.iterator());
      TableMacro macro = (TableMacro) fn;

      // Convert the function (macro) to a Druid external table
      TranslatableTable table = macro.apply(Collections.emptyList());
      ExternalTable druidTable = (ExternalTable) table;

      // Verify that the external table contains what we expect.
      assertEquals(1, druidTable.getRowSignature().size());
      ExternalDataSource extds = (ExternalDataSource) druidTable.getDataSource();
      assertTrue(extds.getInputFormat() instanceof CsvInputFormat);
      assertEquals(ImmutableList.of("a"), ((CsvInputFormat) extds.getInputFormat()).getColumns());
      assertTrue(extds.getInputSource() instanceof InlineInputSource);
      assertEquals("a\nc\n", ((InlineInputSource) extds.getInputSource()).getData());

      Set<ResourceAction> actions = ((AuthorizableOperator) fn).computeResources(null);
      assertEquals(1, actions.size());
      assertEquals(Externals.externalRead("input1"), Iterators.getOnlyElement(actions.iterator()));
    }

    // Override properties
    {
      Collection<Function> fns = schema.getFunctions("httpParam");
      assertEquals(1, fns.size());
      Function fn = Iterators.getOnlyElement(fns.iterator());
      TableMacro macro = (TableMacro) fn;

      List<Object> args = Collections.singletonList("foo");
      TranslatableTable table = macro.apply(args);
      ExternalTable druidTable = (ExternalTable) table;
      assertEquals(1, druidTable.getRowSignature().size());
      ExternalDataSource extds = (ExternalDataSource) druidTable.getDataSource();
      assertTrue(extds.getInputFormat() instanceof CsvInputFormat);
      assertEquals(ImmutableList.of("a"), ((CsvInputFormat) extds.getInputFormat()).getColumns());
      assertTrue(extds.getInputSource() instanceof HttpInputSource);
      HttpInputSource httpSource = (HttpInputSource) extds.getInputSource();
      assertEquals("http://koalas.com/foo.csv", httpSource.getUris().get(0).toString());

      Set<ResourceAction> actions = ((AuthorizableOperator) fn).computeResources(null);
      assertEquals(1, actions.size());
      assertEquals(Externals.externalRead("httpParam"), Iterators.getOnlyElement(actions.iterator()));
    }
  }
}
