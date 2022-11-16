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
import org.apache.druid.catalog.model.table.DatasourceDefn.DatasourceColumnDefn;
import org.apache.druid.java.util.common.IAE;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test of validation and serialization of the catalog table definitions.
 */
@Category(CatalogTest.class)
public class DatasourceTableTest
{
  private static final String SUM_BIGINT = "SUM(BIGINT)";

  private final ObjectMapper mapper = new ObjectMapper();
  private final TableDefnRegistry registry = new TableDefnRegistry(mapper);

  @Test
  public void testMinimalSpec()
  {
    // Minimum possible definition
    Map<String, Object> props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
    );
    {
      TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
      ResolvedTable table = registry.resolve(spec);
      assertNotNull(table);
      assertTrue(table.defn() instanceof DatasourceDefn);
      table.validate();
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
      ResolvedTable table = registry.resolve(spec);
      assertNotNull(table);
      assertTrue(table.defn() instanceof DatasourceDefn);
      table.validate();
    }
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
        .put(AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D")
        .put(AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000)
        .put(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Arrays.asList("foo", "bar"))
        .build();

    {
      TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
      expectValidationSucceeds(spec);
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
      expectValidationSucceeds(spec);
    }
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
          .property(AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, "bogus")
          .buildSpec();
      expectValidationFails(spec);
    }

    // Hidden columns
    {
      TableSpec spec = TableBuilder.datasource("foo", "P1D")
          .property(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY, "bogus")
          .buildSpec();
      expectValidationFails(spec);
    }
    {
      TableSpec spec = TableBuilder.datasource("foo", "P1D")
          .hiddenColumns("a", Columns.TIME_COLUMN)
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
    // Type is required
    {
      ColumnSpec spec = new ColumnSpec(null, null, null, null);
      assertThrows(IAE.class, () -> spec.validate());
    }

    // Name is required
    {
      ColumnSpec spec = new ColumnSpec(DatasourceColumnDefn.COLUMN_TYPE, null, null, null);
      assertThrows(IAE.class, () -> spec.validate());
    }
    {
      ColumnSpec spec = new ColumnSpec(DatasourceColumnDefn.COLUMN_TYPE, "foo", null, null);
      spec.validate();
    }

    // Type is optional
    {
      ColumnSpec spec = new ColumnSpec(DatasourceColumnDefn.COLUMN_TYPE, "foo", "VARCHAR", null);
      spec.validate();
    }
  }

  @Test
  public void testDetailTableColumns()
  {
    TableBuilder builder = TableBuilder.datasource("foo", "P1D");

    // OK to have no columns
    {
      TableSpec spec = builder.copy()
          .buildSpec();
      expectValidationSucceeds(spec);
    }

    // OK to have no column type
    {
      TableSpec spec = builder.copy()
          .column("foo", null)
          .buildSpec();
      expectValidationSucceeds(spec);
    }

    // Time column can have no type
    {
      TableSpec spec = builder.copy()
          .column(Columns.TIME_COLUMN, null)
          .buildSpec();
      expectValidationSucceeds(spec);
    }

    // Time column can only have TIMESTAMP type
    {
      TableSpec spec = builder.copy()
          .timeColumn()
          .buildSpec();
      expectValidationSucceeds(spec);
    }
    {
      TableSpec spec = builder.copy()
          .column(Columns.TIME_COLUMN, Columns.VARCHAR)
          .buildSpec();
      expectValidationFails(spec);
    }

    // Can have a legal scalar type
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.VARCHAR)
          .buildSpec();
      expectValidationSucceeds(spec);
    }

    // Reject an unknown SQL type
    {
      TableSpec spec = builder.copy()
          .column("foo", "BOGUS")
          .buildSpec();
      expectValidationFails(spec);
    }

    // Cannot use a measure type
    {
      TableSpec spec = builder.copy()
          .column("foo", SUM_BIGINT)
          .buildSpec();
      expectValidationFails(spec);
    }

    // Reject duplicate columns
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.VARCHAR)
          .column("bar", Columns.BIGINT)
          .buildSpec();
      expectValidationSucceeds(spec);
    }
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.VARCHAR)
          .column("foo", Columns.BIGINT)
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
        .property(AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000)
        .hiddenColumns("foo", "bar")
        .property("tag1", "some value")
        .property("tag2", "second value")
        .column(new ColumnSpec(DatasourceColumnDefn.COLUMN_TYPE, "a", null, colProps))
        .column("b", Columns.VARCHAR)
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
    updatedProps.put(AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D");
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
        updatedProps.get(AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY),
        merged.properties().get(AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY)
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
    updatedProps.put(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY, null);
    TableSpec update = new TableSpec(null, updatedProps, null);
    TableSpec merged = mergeTables(spec, update);
    expectValidationSucceeds(merged);
    assertFalse(
        merged.properties().containsKey(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Wrong type
    updatedProps = ImmutableMap.of(
        AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY, "mumble"
    );
    update = new TableSpec(null, updatedProps, null);
    assertMergeFails(spec, update);

    // Merge
    updatedProps = ImmutableMap.of(
        AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Collections.singletonList("mumble")
    );
    update = new TableSpec(null, updatedProps, null);
    merged = mergeTables(spec, update);
    expectValidationSucceeds(merged);

    assertEquals(
        Arrays.asList("foo", "bar", "mumble"),
        merged.properties().get(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );
  }

  @Test
  public void testMergeColsWithEmptyList()
  {
    Map<String, Object> props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);

    List<ColumnSpec> colUpdates = Collections.singletonList(
        new ColumnSpec(
            DatasourceColumnDefn.COLUMN_TYPE,
            "a",
            Columns.BIGINT,
            null
        )
    );
    TableSpec update = new TableSpec(null, null, colUpdates);
    TableSpec merged = mergeTables(spec, update);
    List<ColumnSpec> columns = merged.columns();
    assertEquals(1, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals(Columns.BIGINT, columns.get(0).sqlType());
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
            DatasourceColumnDefn.COLUMN_TYPE,
            "a",
            Columns.BIGINT,
            updatedProps
        ),
        new ColumnSpec(
            DatasourceColumnDefn.COLUMN_TYPE,
            "c",
            Columns.VARCHAR,
            null
        )
    );
    TableSpec update = new TableSpec(null, null, colUpdates);
    TableSpec merged = mergeTables(spec, update);

    assertNotEquals(spec, merged);
    List<ColumnSpec> columns = merged.columns();
    assertEquals(3, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals(Columns.BIGINT, columns.get(0).sqlType());
    Map<String, Object> colProps = columns.get(0).properties();
    assertEquals(2, colProps.size());
    assertEquals("new value", colProps.get("colProp1"));
    assertEquals("third value", colProps.get("tag3"));

    assertEquals("c", columns.get(2).name());
    assertEquals(Columns.VARCHAR, columns.get(2).sqlType());
  }
}
