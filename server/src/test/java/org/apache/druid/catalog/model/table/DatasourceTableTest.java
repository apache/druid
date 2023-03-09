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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.facade.DatasourceFacade.ColumnFacade;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test of validation and serialization of the catalog table definitions.
 */
@Category(CatalogTest.class)
public class DatasourceTableTest
{
  private static final Logger LOG = new Logger(DatasourceTableTest.class);

  private final ObjectMapper mapper = DefaultObjectMapper.INSTANCE;
  private final TableDefnRegistry registry = new TableDefnRegistry(mapper);

  @Test
  public void testMinimalSpec()
  {
    // Minimum possible definition
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
    );

    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    ResolvedTable table = registry.resolve(spec);
    assertNotNull(table);
    assertTrue(table.defn() instanceof DatasourceDefn);
    table.validate();
    DatasourceFacade facade = new DatasourceFacade(registry.resolve(table.spec()));
    assertEquals("P1D", facade.segmentGranularityString());
    assertNull(facade.targetSegmentRows());
    assertTrue(facade.hiddenColumns().isEmpty());
    assertFalse(facade.isSealed());
  }

  private void expectValidationFails(final ResolvedTable table)
  {
    assertThrows(IAE.class, () -> table.validate());
  }

  private void expectValidationFails(final TableSpec spec)
  {
    ResolvedTable table = registry.resolve(spec);
    expectValidationFails(table);
  }

  private void expectValidationSucceeds(final TableSpec spec)
  {
    ResolvedTable table = registry.resolve(spec);
    table.validate();
  }

  @Test
  public void testEmptySpec()
  {
    {
      TableSpec spec = new TableSpec(null, ImmutableMap.of(), null);
      assertThrows(IAE.class, () -> registry.resolve(spec));
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, ImmutableMap.of(), null);
      ResolvedTable table = registry.resolve(spec);
      expectValidationFails(table);
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, ImmutableMap.of(), null);
      expectValidationFails(spec);
    }
  }

  @Test
  public void testAllProperties()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "My table")
        .put(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D")
        .put(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000)
        .put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Arrays.asList("foo", "bar"))
        .put(DatasourceDefn.SEALED_PROPERTY, true)
        .build();

    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    DatasourceFacade facade = new DatasourceFacade(registry.resolve(spec));
    assertEquals("P1D", facade.segmentGranularityString());
    assertEquals(1_000_000, (int) facade.targetSegmentRows());
    assertEquals(Arrays.asList("foo", "bar"), facade.hiddenColumns());
    assertTrue(facade.isSealed());
  }

  @Test
  public void testWrongTypes()
  {
    {
      TableSpec spec = new TableSpec("bogus", ImmutableMap.of(), null);
      assertThrows(IAE.class, () -> registry.resolve(spec));
    }

    // Segment granularity
    {
      TableSpec spec = TableBuilder.datasource("foo", "bogus").buildSpec();
      expectValidationFails(spec);
    }

    {
      TableSpec spec = TableBuilder.datasource("foo", "bogus").buildSpec();
      expectValidationFails(spec);
    }

    // Target segment rows
    {
      TableSpec spec = TableBuilder.datasource("foo", "P1D")
          .property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, "bogus")
          .buildSpec();
      expectValidationFails(spec);
    }

    // Hidden columns
    {
      TableSpec spec = TableBuilder.datasource("foo", "P1D")
          .property(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, "bogus")
          .buildSpec();
      expectValidationFails(spec);
    }
    {
      TableSpec spec = TableBuilder.datasource("foo", "P1D")
          .hiddenColumns("a", Columns.TIME_COLUMN)
          .buildSpec();
      expectValidationFails(spec);
    }

    // Sealed
    {
      TableSpec spec = TableBuilder.datasource("foo", "P1D")
          .property(DatasourceDefn.SEALED_PROPERTY, "bogus")
          .buildSpec();
      expectValidationFails(spec);
    }
  }

  @Test
  public void testExtendedProperties()
  {
    TableSpec spec = TableBuilder.datasource("foo", "P1D")
        .property("foo", 10)
        .property("bar", "mumble")
        .buildSpec();
    expectValidationSucceeds(spec);
  }

  @Test
  public void testColumnSpec()
  {
    // Name is required
    {
      ColumnSpec spec = new ColumnSpec(null, null, null);
      assertThrows(IAE.class, () -> spec.validate());
    }
    {
      ColumnSpec spec = new ColumnSpec("foo", null, null);
      spec.validate();
    }

    // Type is optional
    {
      ColumnSpec spec = new ColumnSpec("foo", "VARCHAR", null);
      spec.validate();
    }
  }

  @Test
  public void testColumns()
  {
    TableBuilder builder = TableBuilder.datasource("foo", "P1D");

    // OK to have no columns
    {
      TableSpec spec = builder.copy()
          .buildSpec();
      ResolvedTable table = registry.resolve(spec);
      table.validate();
      DatasourceFacade facade = new DatasourceFacade(registry.resolve(table.spec()));
      assertTrue(facade.columnFacades().isEmpty());
    }

    // OK to have no column type
    {
      TableSpec spec = builder.copy()
          .column("foo", null)
          .buildSpec();
      ResolvedTable table = registry.resolve(spec);
      table.validate();

      DatasourceFacade facade = new DatasourceFacade(registry.resolve(table.spec()));
      assertEquals(1, facade.columnFacades().size());
      ColumnFacade col = facade.columnFacades().get(0);
      assertSame(spec.columns().get(0), col.spec());
      assertFalse(col.isTime());
      assertFalse(col.hasType());
      assertNull(col.druidType());
    }

    // Can have a legal scalar type
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.STRING)
          .buildSpec();
      ResolvedTable table = registry.resolve(spec);
      table.validate();
      DatasourceFacade facade = new DatasourceFacade(registry.resolve(table.spec()));
      assertEquals(1, facade.columnFacades().size());
      ColumnFacade col = facade.columnFacades().get(0);
      assertSame(spec.columns().get(0), col.spec());
      assertFalse(col.isTime());
      assertTrue(col.hasType());
      assertSame(ColumnType.STRING, col.druidType());
    }

    // Reject duplicate columns
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.STRING)
          .column("bar", Columns.LONG)
          .buildSpec();
      expectValidationSucceeds(spec);
    }
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.STRING)
          .column("foo", Columns.LONG)
          .buildSpec();
      expectValidationFails(spec);
    }
  }

  @Test
  public void testTimeColumn()
  {
    TableBuilder builder = TableBuilder.datasource("foo", "P1D");

    // Time column can have no type
    {
      TableSpec spec = builder.copy()
          .column(Columns.TIME_COLUMN, null)
          .buildSpec();
      ResolvedTable table = registry.resolve(spec);
      table.validate();

      DatasourceFacade facade = new DatasourceFacade(registry.resolve(table.spec()));
      assertEquals(1, facade.columnFacades().size());
      ColumnFacade col = facade.columnFacades().get(0);
      assertSame(spec.columns().get(0), col.spec());
      assertTrue(col.isTime());
      assertTrue(col.hasType());
      assertSame(ColumnType.LONG, col.druidType());
    }

    // Time column can only have TIMESTAMP type
    {
      TableSpec spec = builder.copy()
          .timeColumn()
          .buildSpec();
      ResolvedTable table = registry.resolve(spec);
      table.validate();
      DatasourceFacade facade = new DatasourceFacade(registry.resolve(table.spec()));
      assertEquals(1, facade.columnFacades().size());
      ColumnFacade col = facade.columnFacades().get(0);
      assertSame(spec.columns().get(0), col.spec());
      assertTrue(col.isTime());
      assertTrue(col.hasType());
      assertSame(ColumnType.LONG, col.druidType());
    }

    {
      TableSpec spec = builder.copy()
          .column(Columns.TIME_COLUMN, Columns.STRING)
          .buildSpec();
      expectValidationFails(spec);
    }
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ColumnSpec.class)
                  .usingGetClass()
                  .verify();
    EqualsVerifier.forClass(TableSpec.class)
                  .usingGetClass()
                  .verify();
  }

  private TableSpec exampleSpec()
  {
    Map<String, Object> colProps = ImmutableMap.<String, Object>builder()
        .put("colProp1", "value 1")
        .put("colProp2", "value 2")
        .build();
    TableSpec spec = TableBuilder.datasource("foo", "PT1H")
        .description("My table")
        .property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000)
        .hiddenColumns("foo", "bar")
        .property("tag1", "some value")
        .property("tag2", "second value")
        .column(new ColumnSpec("a", null, colProps))
        .column("b", Columns.STRING)
        .buildSpec();

    // Sanity check
    expectValidationSucceeds(spec);
    return spec;
  }

  private TableSpec mergeTables(TableSpec spec, TableSpec update)
  {
    ResolvedTable table = registry.resolve(spec);
    assertNotNull(table);
    return table.merge(update).spec();
  }

  @Test
  public void testMergeEmpty()
  {
    TableSpec spec = exampleSpec();
    TableSpec update = new TableSpec(null, null, null);

    TableSpec merged = mergeTables(spec, update);
    assertEquals(spec, merged);
  }

  private void assertMergeFails(TableSpec spec, TableSpec update)
  {
    assertThrows(IAE.class, () -> mergeTables(spec, update));
  }

  @Test
  public void testMergeTableType()
  {
    TableSpec spec = exampleSpec();

    // Null type test is above.
    // Wrong type
    TableSpec update = new TableSpec("bogus", null, null);
    assertMergeFails(spec, update);

    // Same type
    update = new TableSpec(spec.type(), null, null);
    TableSpec merged = mergeTables(spec, update);
    assertEquals(spec, merged);
  }

  @Test
  public void testMergeProperties()
  {
    TableSpec spec = exampleSpec();

    // Use a regular map, not an immutable one, because immutable maps,
    // in their infinite wisdom, don't allow null values. But, we need
    // such values to indicate which properties to remove.
    Map<String, Object> updatedProps = new HashMap<>();
    // Update a property
    updatedProps.put(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D");
    // Remove a property
    updatedProps.put("tag1", null);
    // Add a property
    updatedProps.put("tag3", "third value");

    TableSpec update = new TableSpec(null, updatedProps, null);
    TableSpec merged = mergeTables(spec, update);
    expectValidationSucceeds(merged);

    // We know that an empty map will leave the spec unchanged
    // due to testMergeEmpty. Here we verify those that we
    // changed.
    assertNotEquals(spec, merged);
    assertEquals(
        updatedProps.get(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY),
        merged.properties().get(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY)
    );
    assertFalse(merged.properties().containsKey("tag1"));
    assertEquals(
        updatedProps.get("tag3"),
        merged.properties().get("tag3")
    );
  }

  @Test
  public void testMergeHiddenCols()
  {
    TableSpec spec = exampleSpec();

    // Remove all hidden columns
    Map<String, Object> updatedProps = new HashMap<>();
    updatedProps.put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, null);
    TableSpec update = new TableSpec(null, updatedProps, null);
    TableSpec merged = mergeTables(spec, update);
    expectValidationSucceeds(merged);
    assertFalse(
        merged.properties().containsKey(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Wrong type
    updatedProps = ImmutableMap.of(
        DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, "mumble"
    );
    update = new TableSpec(null, updatedProps, null);
    assertMergeFails(spec, update);

    // Merge
    updatedProps = ImmutableMap.of(
        DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Collections.singletonList("mumble")
    );
    update = new TableSpec(null, updatedProps, null);
    merged = mergeTables(spec, update);
    expectValidationSucceeds(merged);

    assertEquals(
        Arrays.asList("foo", "bar", "mumble"),
        merged.properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );
  }

  @Test
  public void testMergeColsWithEmptyList()
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);

    List<ColumnSpec> colUpdates = Collections.singletonList(
        new ColumnSpec(
            "a",
            Columns.LONG,
            null
        )
    );
    TableSpec update = new TableSpec(null, null, colUpdates);
    TableSpec merged = mergeTables(spec, update);
    List<ColumnSpec> columns = merged.columns();
    assertEquals(1, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals(Columns.LONG, columns.get(0).dataType());
  }

  @Test
  public void testMergeCols()
  {
    TableSpec spec = exampleSpec();

    Map<String, Object> updatedProps = new HashMap<>();
    // Update a property
    updatedProps.put("colProp1", "new value");
    // Remove a property
    updatedProps.put("colProp2", null);
    // Add a property
    updatedProps.put("tag3", "third value");

    List<ColumnSpec> colUpdates = Arrays.asList(
        new ColumnSpec(
            "a",
            Columns.LONG,
            updatedProps
        ),
        new ColumnSpec(
            "c",
            Columns.STRING,
            null
        )
    );
    TableSpec update = new TableSpec(null, null, colUpdates);
    TableSpec merged = mergeTables(spec, update);

    assertNotEquals(spec, merged);
    List<ColumnSpec> columns = merged.columns();
    assertEquals(3, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals(Columns.LONG, columns.get(0).dataType());
    Map<String, Object> colProps = columns.get(0).properties();
    assertEquals(2, colProps.size());
    assertEquals("new value", colProps.get("colProp1"));
    assertEquals("third value", colProps.get("tag3"));

    assertEquals("c", columns.get(2).name());
    assertEquals(Columns.STRING, columns.get(2).dataType());
  }

  /**
   * Test case for multiple of the {@code datasource.md} examples. To use this, enable the
   * test, run it, then copy the JSON from the console. The examples pull out bits
   * and pieces in multiple places.
   */
  @Test
  @Ignore
  public void docExample()
  {
    TableSpec spec = TableBuilder.datasource("foo", "PT1H")
        .description("Web server performance metrics")
        .property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000)
        .hiddenColumns("foo", "bar")
        .column("__time", Columns.LONG)
        .column("host", Columns.STRING, ImmutableMap.of(TableDefn.DESCRIPTION_PROPERTY, "The web server host"))
        .column("bytesSent", Columns.LONG, ImmutableMap.of(TableDefn.DESCRIPTION_PROPERTY, "Number of response bytes sent"))
        .clusterColumns(new ClusterKeySpec("a", false), new ClusterKeySpec("b", true))
        .sealed(true)
        .buildSpec();
    LOG.info(spec.toString());
  }
}
