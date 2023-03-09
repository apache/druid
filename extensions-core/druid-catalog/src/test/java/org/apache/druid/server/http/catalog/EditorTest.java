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

package org.apache.druid.server.http.catalog;

import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.http.TableEditRequest;
import org.apache.druid.catalog.http.TableEditRequest.DropColumns;
import org.apache.druid.catalog.http.TableEditRequest.HideColumns;
import org.apache.druid.catalog.http.TableEditRequest.MoveColumn;
import org.apache.druid.catalog.http.TableEditRequest.UnhideColumns;
import org.apache.druid.catalog.http.TableEditRequest.UpdateColumns;
import org.apache.druid.catalog.http.TableEditRequest.UpdateProperties;
import org.apache.druid.catalog.http.TableEditor;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class EditorTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage catalog;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    catalog = dbFixture.storage;
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  private TableMetadata doEdit(String tableName, TableEditRequest cmd) throws CatalogException
  {
    final TableId id = TableId.datasource(tableName);
    new TableEditor(catalog, id, cmd).go();
    return catalog.tables().read(id);
  }

  @Test
  public void testMoveColumn() throws CatalogException
  {
    final String tableName = "table1";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();
    catalog.tables().create(table);

    // Move first
    MoveColumn cmd = new MoveColumn("c", MoveColumn.Position.FIRST, null);
    List<ColumnSpec> revised = doEdit(tableName, cmd).spec().columns();
    assertEquals(
        Arrays.asList("c", "a", "b"),
        CatalogUtils.columnNames(revised)
    );

    // Move last
    catalog.tables().replace(table);
    cmd = new MoveColumn("a", MoveColumn.Position.LAST, null);
    revised = doEdit(tableName, cmd).spec().columns();
    assertEquals(
        Arrays.asList("b", "c", "a"),
        CatalogUtils.columnNames(revised)
    );

    // Move before, earlier anchor
    catalog.tables().replace(table);
    cmd = new MoveColumn("c", MoveColumn.Position.BEFORE, "b");
    revised = doEdit(tableName, cmd).spec().columns();
    assertEquals(
        Arrays.asList("a", "c", "b"),
        CatalogUtils.columnNames(revised)
    );

    // Move before, later anchor
    catalog.tables().replace(table);
    cmd = new MoveColumn("a", MoveColumn.Position.BEFORE, "c");
    revised = doEdit(tableName, cmd).spec().columns();
    assertEquals(
        Arrays.asList("b", "a", "c"),
        CatalogUtils.columnNames(revised)
    );

    // Move after, earlier anchor
    catalog.tables().replace(table);
    cmd = new MoveColumn("c", MoveColumn.Position.AFTER, "a");
    revised = doEdit(tableName, cmd).spec().columns();
    assertEquals(
        Arrays.asList("a", "c", "b"),
        CatalogUtils.columnNames(revised)
    );

    // Move after, later anchor
    catalog.tables().replace(table);
    cmd = new MoveColumn("a", MoveColumn.Position.AFTER, "b");
    revised = doEdit(tableName, cmd).spec().columns();
    assertEquals(
        Arrays.asList("b", "a", "c"),
        CatalogUtils.columnNames(revised)
    );
  }

  @Test
  public void testHideColumns() throws CatalogException
  {
    final String tableName = "table2";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .build();
    catalog.tables().create(table);

    // Null list
    HideColumns cmd = new HideColumns(null);
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Empty list
    cmd = new HideColumns(Collections.emptyList());
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Hide starting from a null list.
    cmd = new HideColumns(Arrays.asList("a", "b"));
    assertEquals(
        Arrays.asList("a", "b"),
        doEdit(tableName, cmd).spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Hide starting from an empty list.
    Map<String, Object> props = new HashMap<>(table.spec().properties());
    props.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Collections.emptyList());
    catalog.tables().replace(table.withProperties(props));
    assertEquals(
        Arrays.asList("a", "b"),
        doEdit(tableName, cmd).spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Hide, but all are duplicates
    props = new HashMap<>(table.spec().properties());
    props.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Arrays.asList("a", "b", "c"));
    catalog.tables().replace(table.withProperties(props));
    cmd = new HideColumns(Arrays.asList("b", "c"));
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Hide with existing columns
    cmd = new HideColumns(Arrays.asList("b", "d"));
    assertEquals(
        Arrays.asList("a", "b", "c", "d"),
        doEdit(tableName, cmd).spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Hide with duplicates
    props = new HashMap<>(table.spec().properties());
    props.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Arrays.asList("a", "b"));
    catalog.tables().replace(table.withProperties(props));
    cmd = new HideColumns(Arrays.asList("b", "d", "b", "d"));
    assertEquals(
        Arrays.asList("a", "b", "d"),
        doEdit(tableName, cmd).spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );
  }

  @Test
  public void testUnhideColumns() throws CatalogException
  {
    final String tableName = "table3";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .build();
    catalog.tables().create(table);

    // Null unhide list
    UnhideColumns cmd = new UnhideColumns(null);
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Empty list
    cmd = new UnhideColumns(Collections.emptyList());
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Unhide starting from a null list.
    cmd = new UnhideColumns(Arrays.asList("a", "b"));
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Unhide starting from an empty list.
    Map<String, Object> props = new HashMap<>(table.spec().properties());
    props.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Collections.emptyList());
    catalog.tables().replace(table.withProperties(props));
    assertNull(
        doEdit(tableName, cmd).spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Unhide starting with an empty list with (non-existing) columns to unhide
    props = new HashMap<>(table.spec().properties());
    props.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Collections.emptyList());
    catalog.tables().replace(table.withProperties(props));
    cmd = new UnhideColumns(Collections.singletonList("a"));
    assertNull(
        doEdit(tableName, cmd).spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Unhide columns which are not actually hidden.
    props = new HashMap<>(table.spec().properties());
    props.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Arrays.asList("a", "b", "c"));
    catalog.tables().replace(table.withProperties(props));
    cmd = new UnhideColumns(Arrays.asList("d", "e"));
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Unhide some hidden columns.
    cmd = new UnhideColumns(Arrays.asList("a", "c", "a", "d"));
    assertEquals(
        Collections.singletonList("b"),
        doEdit(tableName, cmd).spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Unhide all hidden columns
    props = new HashMap<>(table.spec().properties());
    props.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Arrays.asList("a", "b", "c"));
    catalog.tables().replace(table.withProperties(props));
    cmd = new UnhideColumns(Arrays.asList("a", "c", "b", "d"));
    assertNull(
        doEdit(tableName, cmd).spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );
  }

  @Test
  public void testDropColumns() throws CatalogException
  {
    final String tableName = "table4";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();
    catalog.tables().create(table);

    // Null drop list
    DropColumns cmd = new DropColumns(null);
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Empty list
    cmd = new DropColumns(Collections.emptyList());
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Drop non-existent columns
    cmd = new DropColumns(Arrays.asList("d", "e"));
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Drop some columns, including dups.
    cmd = new DropColumns(Arrays.asList("c", "a", "c", "d"));
    assertEquals(
        Collections.singletonList("b"),
        CatalogUtils.columnNames(doEdit(tableName, cmd).spec().columns())
    );

    // Drop all columns
    catalog.tables().replace(table);
    cmd = new DropColumns(Arrays.asList("c", "a", "c", "b"));
    assertEquals(
        Collections.emptyList(),
        doEdit(tableName, cmd).spec().columns()
    );

    // Drop from a null column list
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());
  }

  @Test
  public void testUpdateProperties() throws CatalogException
  {
    final String tableName = "table5";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .build();
    catalog.tables().create(table);

    // Null merge map
    UpdateProperties cmd = new UpdateProperties(null);
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Empty merge map
    cmd = new UpdateProperties(Collections.emptyMap());
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Can't test an empty property set: no table type allows empty
    // properties.

    // Remove a required property
    Map<String, Object> updates1 = new HashMap<>();
    updates1.put(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, null);
    assertThrows(
        CatalogException.class,
        () -> new TableEditor(
                  catalog,
                  table.id(),
                  new UpdateProperties(updates1)
              )
             .go()
    );

    // Add and update properties
    Map<String, Object> updates = new HashMap<>();
    updates.put(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H");
    updates.put("foo", "bar");
    cmd = new UpdateProperties(updates);
    Map<String, Object> expected = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H",
        "foo", "bar"
    );
    assertEquals(
        expected,
        doEdit(tableName, cmd).spec().properties()
    );

    // Update only
    updates = new HashMap<>();
    updates.put("foo", "mumble");
    cmd = new UpdateProperties(updates);
    expected = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H",
        "foo", "mumble"
    );
    assertEquals(
        expected,
        doEdit(tableName, cmd).spec().properties()
    );

    // Remove a property
    updates = new HashMap<>();
    updates.put("foo", null);
    cmd = new UpdateProperties(updates);
    expected = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H"
    );
    assertEquals(
        expected,
        doEdit(tableName, cmd).spec().properties()
    );
  }

  @Test
  public void testUpdateColumns() throws CatalogException
  {
    final String tableName = "table4";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();
    catalog.tables().create(table);

    // Null update list
    UpdateColumns cmd = new UpdateColumns(null);
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Empty list
    cmd = new UpdateColumns(Collections.emptyList());
    assertEquals(0, new TableEditor(catalog, table.id(), cmd).go());

    // Add a column
    cmd = new UpdateColumns(
        Collections.singletonList(
            new ColumnSpec("d", Columns.STRING, null)
         )
    );
    TableMetadata revised = doEdit(tableName, cmd);
    assertEquals(
        Arrays.asList("a", "b", "c", "d"),
        CatalogUtils.columnNames(revised.spec().columns())
    );
    ColumnSpec colD = revised.spec().columns().get(3);
    assertEquals("d", colD.name());
    assertEquals(Columns.STRING, colD.dataType());

    // Update a column
    cmd = new UpdateColumns(
        Collections.singletonList(
            new ColumnSpec(
                "a",
                Columns.LONG,
                ImmutableMap.of("foo", "bar")
            )
         )
    );
    revised = doEdit(tableName, cmd);
    assertEquals(
        Arrays.asList("a", "b", "c", "d"),
        CatalogUtils.columnNames(revised.spec().columns())
    );
    ColumnSpec colA = revised.spec().columns().get(0);
    assertEquals("a", colA.name());
    assertEquals(Columns.LONG, colA.dataType());
    assertEquals(ImmutableMap.of("foo", "bar"), colA.properties());

    // Duplicates
    UpdateColumns cmd2 = new UpdateColumns(
        Arrays.asList(
            new ColumnSpec("e", Columns.STRING, null),
            new ColumnSpec("e", null, null)
         )
    );
    assertThrows(CatalogException.class, () -> doEdit(tableName, cmd2));

    // Valid time column type
    cmd = new UpdateColumns(
        Collections.singletonList(
            new ColumnSpec(Columns.TIME_COLUMN, Columns.LONG, null)
         )
    );
    revised = doEdit(tableName, cmd);
    assertEquals(
        Arrays.asList("a", "b", "c", "d", "__time"),
        CatalogUtils.columnNames(revised.spec().columns())
    );
  }
}
