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
import io.vavr.collection.Map;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.http.CatalogResource;
import org.apache.druid.catalog.http.MoveColumn;
import org.apache.druid.catalog.http.TableEditRequest;
import org.apache.druid.catalog.http.TableEditRequest.HideColumns;
import org.apache.druid.catalog.http.TableEditor;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.storage.sql.CatalogManager;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.druid.server.http.catalog.DummyRequest.getBy;
import static org.apache.druid.server.http.catalog.DummyRequest.postBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
  public void testHideColumns()
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

    Map<String, Object> props = new HashMap<>(table.spec().properties());
    props.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Collections.emptyList());
    catalog.tables().replace(table.withProperties(props));
    // Hide starting from an empty list.
    cmd = new HideColumns(Arrays.asList("a", "b"), Collections.emptyList());
    revised = cmd.perform(Collections.emptyList());
    assertEquals(Arrays.asList("a", "b"), revised);

    // Hide with existing columns
    cmd = new HideColumns(Arrays.asList("b", "d"), null);
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "b", "c", "d"), revised);

    // Hide with existing columns
    cmd = new HideColumns(Arrays.asList("b", "d"), null);
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "b", "c", "d"), revised);

    // Unhide existing columns
    cmd = new HideColumns(null, Arrays.asList("b", "d"));
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "c"), revised);

    // Both hide and unhide. Hide takes precedence.
    cmd = new HideColumns(Arrays.asList("b", "d", "e"), Arrays.asList("c", "d"));
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "b", "d", "e"), revised);

    // Duplicates
  }

  @Test
  public void testUnhideColumns()
  {
    final String tableName = "table2";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .build();
    catalog.tables().create(table);

    // Everything is null
    HideColumns cmd = new HideColumns(null, null);
    List<String> revised = cmd.perform(null);
    assertNull(revised);

    // Unhide from null list
    cmd = new HideColumns(null, Collections.singletonList("a"));
    revised = cmd.perform(null);
    assertNull(revised);

    // And from an empty list
    cmd = new HideColumns(null, Collections.singletonList("a"));
    revised = cmd.perform(Collections.emptyList());
    assertNull(revised);

    // Hide starting from a null list.
    cmd = new HideColumns(Arrays.asList("a", "b"), null);
    revised = cmd.perform(null);
    assertEquals(Arrays.asList("a", "b"), revised);

    // Hide starting from an empty list.
    cmd = new HideColumns(Arrays.asList("a", "b"), Collections.emptyList());
    revised = cmd.perform(Collections.emptyList());
    assertEquals(Arrays.asList("a", "b"), revised);

    // Hide with existing columns
    cmd = new HideColumns(Arrays.asList("b", "d"), null);
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "b", "c", "d"), revised);

    // Hide with existing columns
    cmd = new HideColumns(Arrays.asList("b", "d"), null);
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "b", "c", "d"), revised);

    // Unhide existing columns
    cmd = new HideColumns(null, Arrays.asList("b", "d"));
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "c"), revised);

    // Both hide and unhide. Hide takes precedence.
    cmd = new HideColumns(Arrays.asList("b", "d", "e"), Arrays.asList("c", "d"));
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "b", "d", "e"), revised);
  }
}
