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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.http.TableEditRequest;
import org.apache.druid.catalog.http.TableEditRequest.AddProjection;
import org.apache.druid.catalog.http.TableEditRequest.DropColumns;
import org.apache.druid.catalog.http.TableEditRequest.DropProjection;
import org.apache.druid.catalog.http.TableEditRequest.HideColumns;
import org.apache.druid.catalog.http.TableEditRequest.MoveColumn;
import org.apache.druid.catalog.http.TableEditRequest.UnhideColumns;
import org.apache.druid.catalog.http.TableEditRequest.UpdateColumns;
import org.apache.druid.catalog.http.TableEditRequest.UpdateProperties;
import org.apache.druid.catalog.http.TableEditor;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ClusteredValueGroupsBaseTableMetadata;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.JUnit5TestDerbyConnector;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EditorTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  @RegisterExtension
  public static final JUnit5TestDerbyConnector DERBY_CONNECTOR_RULE = new JUnit5TestDerbyConnector();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage catalog;

  @BeforeEach
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(DERBY_CONNECTOR_RULE);
    catalog = dbFixture.storage;
  }

  @AfterEach
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

    Map<String, Object> updates = new HashMap<>();
    updates.put(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, null);
    cmd = new UpdateProperties(updates);
    Map<String, Object> expected = ImmutableMap.of();
    assertEquals(
        expected,
        doEdit(tableName, cmd).spec().properties()
    );

    // Add and update properties
    updates = new HashMap<>();
    updates.put(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H");
    updates.put("foo", "bar");
    cmd = new UpdateProperties(updates);
    expected = ImmutableMap.of(
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

    // Add a DESC cluster key - should fail
    Map<String, Object> updates1 = new HashMap<>();
    updates1.put(DatasourceDefn.CLUSTER_KEYS_PROPERTY, ImmutableList.of(new ClusterKeySpec("clusterKeyA", true)));

    assertThrows(
        CatalogException.class,
        () -> new TableEditor(
            catalog,
            table.id(),
            new UpdateProperties(updates1)
        ).go()
    );

    // Add a ASC cluster key - should succeed
    updates = new HashMap<>();
    updates.put(DatasourceDefn.CLUSTER_KEYS_PROPERTY, ImmutableList.of(new ClusterKeySpec("clusterKeyA", false)));
    cmd = new UpdateProperties(updates);
    expected = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H",
        DatasourceDefn.CLUSTER_KEYS_PROPERTY, ImmutableList.of(new ClusterKeySpec("clusterKeyA", false))
    );
    Map<String, Object> actual = doEdit(tableName, cmd).spec().properties();
    actual.put(
        DatasourceDefn.CLUSTER_KEYS_PROPERTY,
        MAPPER.convertValue(actual.get(DatasourceDefn.CLUSTER_KEYS_PROPERTY), ClusterKeySpec.CLUSTER_KEY_LIST_TYPE_REF)
    );
    assertEquals(
        expected,
        actual
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

  @Test
  public void testAddAndDropProjection() throws CatalogException
  {
    final String tableName = "projections";
    final TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .timeColumn()
        .column("dim", "VARCHAR")
        .column("met", "BIGINT")
        .build();
    catalog.tables().create(table);

    final DatasourceProjectionMetadata daily = new DatasourceProjectionMetadata(
        AggregateProjectionSpec.builder("daily")
                               .groupingColumns(new StringDimensionSchema("dim"))
                               .aggregators(new LongSumAggregatorFactory("sum_met", "met"))
                               .build()
    );

    assertTrue(new TableEditor(catalog, table.id(), new AddProjection(daily, false)).go() > 0);
    assertEquals(List.of(daily), projectionsOf(tableName));

    // Adding the same name again is an error, unless the caller said to leave it alone.
    assertThrows(
        CatalogException.class,
        () -> new TableEditor(catalog, table.id(), new AddProjection(daily, false)).go()
    );
    assertEquals(0, new TableEditor(catalog, table.id(), new AddProjection(daily, true)).go());
    assertEquals(List.of(daily), projectionsOf(tableName));

    // Dropping a projection that is not there is likewise an error unless tolerated.
    assertThrows(
        CatalogException.class,
        () -> new TableEditor(catalog, table.id(), new DropProjection("nope", false)).go()
    );
    assertEquals(0, new TableEditor(catalog, table.id(), new DropProjection("nope", true)).go());

    assertTrue(new TableEditor(catalog, table.id(), new DropProjection("daily", false)).go() > 0);
    assertNull(
        catalog.tables().read(TableId.datasource(tableName))
               .spec().properties().get(DatasourceDefn.PROJECTIONS_KEYS_PROPERTY)
    );
  }

  /**
   * {@code AddColumns} means add: a column that already exists is an error rather than a silent in-place update, which
   * is what plain {@code UpdateColumns} would do.
   */
  @Test
  public void testAddColumns() throws CatalogException
  {
    final String tableName = "addCols";
    final TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .timeColumn()
        .column("dim", "VARCHAR")
        .build();
    catalog.tables().create(table);

    final TableMetadata revised = doEdit(
        tableName,
        new TableEditRequest.AddColumns(Collections.singletonList(new ColumnSpec("met", "BIGINT", null)))
    );
    assertEquals(
        Arrays.asList(Columns.TIME_COLUMN, "dim", "met"),
        CatalogUtils.columnNames(revised.spec().columns())
    );

    final CatalogException e = assertThrows(
        CatalogException.class,
        () -> doEdit(
            tableName,
            new TableEditRequest.AddColumns(Collections.singletonList(new ColumnSpec("dim", "BIGINT", null)))
        )
    );
    assertTrue(e.getMessage().contains("Column [dim] already exists"), e.getMessage());
    // The rejected add did not change the existing column's type.
    assertEquals("VARCHAR", columnType(tableName, "dim"));
  }

  /**
   * {@code AlterColumns} means alter: a column that does not exist is an error rather than being appended, so a
   * misspelled target cannot quietly create a column.
   */
  @Test
  public void testAlterColumns() throws CatalogException
  {
    final String tableName = "alterCols";
    final TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .timeColumn()
        .column("dim", "VARCHAR")
        .build();
    catalog.tables().create(table);

    doEdit(
        tableName,
        new TableEditRequest.AlterColumns(Collections.singletonList(new ColumnSpec("dim", "BIGINT", null)))
    );
    assertEquals("BIGINT", columnType(tableName, "dim"));

    final CatalogException e = assertThrows(
        CatalogException.class,
        () -> doEdit(
            tableName,
            new TableEditRequest.AlterColumns(Collections.singletonList(new ColumnSpec("typo", "BIGINT", null)))
        )
    );
    assertTrue(e.getMessage().contains("Column [typo] does not exist"), e.getMessage());
    // The misspelled target was not created.
    assertEquals(
        Arrays.asList(Columns.TIME_COLUMN, "dim"),
        CatalogUtils.columnNames(catalog.tables().read(table.id()).spec().columns())
    );
  }

  private String columnType(String tableName, String columnName) throws CatalogException
  {
    return catalog.tables().read(TableId.datasource(tableName)).spec().columns().stream()
                  .filter(c -> columnName.equals(c.name()))
                  .findFirst()
                  .orElseThrow(() -> new AssertionError("No column [" + columnName + "]"))
                  .dataType();
  }

  /**
   * A property edit is validated against the parts of the spec it does not touch. Segment granularity and the declared
   * projections are only meaningful together, so coarsening the segments under a projection has to be caught here
   * rather than at ingest time.
   */
  @Test
  public void testUpdatePropertiesValidatedAgainstProjections() throws CatalogException
  {
    final String tableName = "granularity";
    final TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .timeColumn()
        .column("dim", "VARCHAR")
        .column("met", "BIGINT")
        .build();
    catalog.tables().create(table);

    final DatasourceProjectionMetadata daily = new DatasourceProjectionMetadata(
        AggregateProjectionSpec.builder("daily")
                               .virtualColumns(
                                   Granularities.toVirtualColumn(
                                       Granularities.DAY,
                                       Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                   )
                               )
                               .groupingColumns(
                                   new LongDimensionSchema(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME),
                                   new StringDimensionSchema("dim")
                               )
                               .aggregators(new LongSumAggregatorFactory("sum_met", "met"))
                               .build()
    );
    // A day-granularity projection is fine in day-granularity segments.
    assertTrue(new TableEditor(catalog, table.id(), new AddProjection(daily, false)).go() > 0);

    assertThrows(
        CatalogException.class,
        () -> doEdit(
            tableName,
            new UpdateProperties(ImmutableMap.of(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H"))
        )
    );
    // The rejected edit left the stored spec alone.
    assertEquals(
        "P1D",
        catalog.tables().read(table.id()).spec().properties().get(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY)
    );
  }

  /**
   * The base table layout lives in a property rather than in the projections list, but it follows the same rules as
   * {@code AddProjection} / {@code DropProjection}, decided inside the update transaction rather than by the caller.
   */
  @Test
  public void testSetAndDropBaseTable() throws CatalogException
  {
    final String tableName = "baseTable";
    final TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column("tenant", "VARCHAR")
        .timeColumn()
        .sealed(true)
        .build();
    catalog.tables().create(table);

    final ClusteredValueGroupsBaseTableMetadata layout =
        new ClusteredValueGroupsBaseTableMetadata(ImmutableList.of("tenant"), null, null);

    assertTrue(new TableEditor(catalog, table.id(), new TableEditRequest.SetBaseTable(layout, false)).go() > 0);
    assertEquals(
        layout,
        catalog.tableRegistry()
               .resolve(catalog.tables().read(table.id()).spec())
               .decodeProperty(DatasourceDefn.BASE_TABLE_PROPERTY)
    );

    // Defining a second layout is an error unless the caller said to leave the existing one alone.
    final CatalogException e = assertThrows(
        CatalogException.class,
        () -> doEdit(tableName, new TableEditRequest.SetBaseTable(layout, false))
    );
    assertTrue(e.getMessage().contains("already has a base table layout"), e.getMessage());
    assertEquals(0, new TableEditor(catalog, table.id(), new TableEditRequest.SetBaseTable(layout, true)).go());

    assertTrue(new TableEditor(catalog, table.id(), new TableEditRequest.DropBaseTable(false)).go() > 0);
    assertNull(
        catalog.tables().read(table.id()).spec().properties().get(DatasourceDefn.BASE_TABLE_PROPERTY)
    );

    // Dropping one that is not there is likewise an error unless tolerated.
    assertThrows(
        CatalogException.class,
        () -> doEdit(tableName, new TableEditRequest.DropBaseTable(false))
    );
    assertEquals(0, new TableEditor(catalog, table.id(), new TableEditRequest.DropBaseTable(true)).go());
  }

  /**
   * A projection records the type of each column it groups by, so retyping such a column would leave a projection the
   * table can no longer build. The whole-spec validation catches it before the edit is committed.
   */
  @Test
  public void testAlterColumnsValidatedAgainstProjections() throws CatalogException
  {
    final String tableName = "retype";
    final TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .timeColumn()
        .column("dim", "VARCHAR")
        .column("met", "BIGINT")
        .build();
    catalog.tables().create(table);

    final DatasourceProjectionMetadata byDim = new DatasourceProjectionMetadata(
        AggregateProjectionSpec.builder("by_dim")
                               .groupingColumns(new StringDimensionSchema("dim"))
                               .aggregators(new LongSumAggregatorFactory("sum_met", "met"))
                               .build()
    );
    assertTrue(new TableEditor(catalog, table.id(), new AddProjection(byDim, false)).go() > 0);

    final CatalogException e = assertThrows(
        CatalogException.class,
        () -> doEdit(
            tableName,
            new TableEditRequest.AlterColumns(Collections.singletonList(new ColumnSpec("dim", "BIGINT", null)))
        )
    );
    assertTrue(e.getMessage().contains("groups on column [dim]"), e.getMessage());
    // The rejected edit left the column as it was.
    assertEquals("VARCHAR", columnType(tableName, "dim"));
  }

  /**
   * The mirror case: a column edit is validated against the properties it does not touch. A base table layout names
   * the columns it clusters on, so dropping one leaves a layout that can no longer be derived.
   */
  @Test
  public void testDropColumnsValidatedAgainstBaseTable() throws CatalogException
  {
    final String tableName = "clustered";
    final TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        // Declared order is the segment order, so the clustering column leads.
        .column("tenant", "VARCHAR")
        .timeColumn()
        .column("met", "BIGINT")
        .sealed(true)
        .baseTable(new ClusteredValueGroupsBaseTableMetadata(ImmutableList.of("tenant"), null, null))
        .build();
    catalog.tables().create(table);

    assertThrows(
        CatalogException.class,
        () -> doEdit(tableName, new DropColumns(ImmutableList.of("tenant")))
    );
    assertEquals(
        Arrays.asList("tenant", Columns.TIME_COLUMN, "met"),
        CatalogUtils.columnNames(catalog.tables().read(table.id()).spec().columns())
    );

    // Dropping a column the layout does not name is still allowed.
    assertTrue(new TableEditor(catalog, table.id(), new DropColumns(ImmutableList.of("met"))).go() > 0);
  }

  private List<DatasourceProjectionMetadata> projectionsOf(String tableName) throws CatalogException
  {
    return catalog.tableRegistry()
                  .resolve(catalog.tables().read(TableId.datasource(tableName)).spec())
                  .decodeProperty(DatasourceDefn.PROJECTIONS_KEYS_PROPERTY);
  }

}
