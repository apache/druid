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
import org.apache.druid.catalog.model.table.DatasourceDefn.DetailDatasourceDefn;
import org.apache.druid.catalog.model.table.DatasourceDefn.RollupDatasourceDefn;
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
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
    );
    {
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, null);
      ResolvedTable table = registry.resolve(spec);
      assertNotNull(table);
      assertTrue(table.defn() instanceof DetailDatasourceDefn);
      table.validate();
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      ResolvedTable table = registry.resolve(spec);
      assertNotNull(table);
      assertTrue(table.defn() instanceof RollupDatasourceDefn);
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
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, ImmutableMap.of(), null);
      ResolvedTable table = registry.resolve(spec);
      expectValidationFails(table);
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, ImmutableMap.of(), null);
      expectValidationFails(spec);
    }
  }

  @Test
  public void testAllProperties()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "My table")
        .put(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D")
        .put(DatasourceDefn.ROLLUP_GRANULARITY_PROPERTY, "PT1M")
        .put(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000)
        .put(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, Arrays.asList("foo", "bar"))
        .build();

    {
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, null);
      expectValidationSucceeds(spec);

      // Check serialization
      byte[] bytes = spec.toBytes(mapper);
      assertEquals(spec, TableSpec.fromBytes(mapper, bytes));
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationSucceeds(spec);

      // Check serialization
      byte[] bytes = spec.toBytes(mapper);
      assertEquals(spec, TableSpec.fromBytes(mapper, bytes));
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
      TableSpec spec = TableBuilder.detailTable("foo", "bogus").buildSpec();
      expectValidationFails(spec);
    }

    {
      TableSpec spec = TableBuilder.rollupTable("foo", "bogus").buildSpec();
      expectValidationFails(spec);
    }

    // Rollup granularity
    {
      TableSpec spec = TableBuilder.rollupTable("foo", "P1D")
          .rollupGranularity("bogus")
          .buildSpec();
      expectValidationFails(spec);
    }

    {
      TableSpec spec = TableBuilder.rollupTable("foo", "P1D")
          .property(DatasourceDefn.ROLLUP_GRANULARITY_PROPERTY, 10)
          .buildSpec();
      expectValidationFails(spec);
    }

    // Target segment rows
    {
      TableSpec spec = TableBuilder.detailTable("foo", "P1D")
          .property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, "bogus")
          .buildSpec();
      expectValidationFails(spec);
    }

    // Hidden columns
    {
      TableSpec spec = TableBuilder.detailTable("foo", "P1D")
          .property(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, "bogus")
          .buildSpec();
      expectValidationFails(spec);
    }
    {
      TableSpec spec = TableBuilder.detailTable("foo", "P1D")
          .hiddenColumns("a", Columns.TIME_COLUMN)
          .buildSpec();
      expectValidationFails(spec);
    }
  }

  @Test
  public void testExtendedProperties()
  {
    TableSpec spec = TableBuilder.detailTable("foo", "P1D")
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
      ColumnSpec spec = new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, null, null, null);
      assertThrows(IAE.class, () -> spec.validate());
    }
    {
      ColumnSpec spec = new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", null, null);
      spec.validate();
    }

    // Type is optional
    {
      ColumnSpec spec = new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", "VARCHAR", null);
      spec.validate();
    }
  }

  @Test
  public void testDetailTableColumns()
  {
    TableBuilder builder = TableBuilder.detailTable("foo", "P1D");

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

    // Cannot use a measure
    {
      TableSpec spec = builder.copy()
          .measure("foo", SUM_BIGINT)
          .buildSpec();
      expectValidationFails(spec);
    }

    // Cannot use a dimension for a detail table
    {
      TableSpec spec = builder.copy()
          .column(new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "foo", Columns.VARCHAR, null))
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
  public void testRollupTableColumns()
  {
    TableBuilder builder = TableBuilder.rollupTable("foo", "P1D")
        .rollupGranularity("PT1M");

    // OK to have no columns
    {
      TableSpec spec = builder.buildSpec();
      expectValidationSucceeds(spec);
    }

    // OK for a dimension to have no type
    {
      TableSpec spec = builder.copy()
          .column("foo", null)
          .buildSpec();
      expectValidationSucceeds(spec);
    }

    // Dimensions must have a scalar type, if the type is non-null
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.VARCHAR)
          .buildSpec();
      expectValidationSucceeds(spec);
    }
    {
      TableSpec spec = builder.copy()
          .column("foo", "BOGUS")
          .buildSpec();
      expectValidationFails(spec);
    }
    {
      TableSpec spec = builder.copy()
          .column("foo", SUM_BIGINT)
          .buildSpec();
      expectValidationFails(spec);
    }

    // Time column can be a dimension and can only have TIMESTAMP type
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
    {
      TableSpec spec = builder.copy()
          .column(Columns.TIME_COLUMN, SUM_BIGINT)
          .buildSpec();
      expectValidationFails(spec);
    }

    // Measures must have an aggregate type
    {
      TableSpec spec = builder.copy()
          .measure("foo", null)
          .buildSpec();
      expectValidationFails(spec);
    }
    {
      TableSpec spec = builder.copy()
          .measure("foo", Columns.VARCHAR)
          .buildSpec();
      expectValidationFails(spec);
    }
    {
      TableSpec spec = builder.copy()
          .measure("foo", SUM_BIGINT)
          .buildSpec();
      expectValidationSucceeds(spec);
    }

    // Cannot use a detail column
    {
      TableSpec spec = builder.copy()
          .column(new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", null, null))
          .buildSpec();
      expectValidationFails(spec);
    }

    // Reject duplicate columns
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.VARCHAR)
          .measure("bar", SUM_BIGINT)
          .buildSpec();
      expectValidationSucceeds(spec);
    }
    {
      TableSpec spec = builder.copy()
          .column("foo", Columns.VARCHAR)
          .measure("foo", SUM_BIGINT)
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
    TableSpec spec = TableBuilder.rollupTable("foo", "PT1H")
        .description("My table")
        .rollupGranularity("PT1M")
        .property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1_000_000)
        .hiddenColumns("foo", "bar")
        .property("tag1", "some value")
        .property("tag2", "second value")
        .column(new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "a", null, colProps))
        .column("b", Columns.VARCHAR)
        .buildSpec();

    // Sanity check
    expectValidationSucceeds(spec);
    return spec;
  }

  @Test
  public void testSerialization()
  {
    TableSpec spec = exampleSpec();

    // Round-trip
    TableSpec spec2 = TableSpec.fromBytes(mapper, spec.toBytes(mapper));
    assertEquals(spec, spec2);

    // Sanity check of toString, which uses JSON
    assertNotNull(spec.toString());
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
    TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, null);

    List<ColumnSpec> colUpdates = Collections.singletonList(
        new ColumnSpec(
            DatasourceDefn.DETAIL_COLUMN_TYPE,
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
            DatasourceDefn.DIMENSION_TYPE,
            "a",
            Columns.BIGINT,
            updatedProps
        ),
        new ColumnSpec(
            DatasourceDefn.DIMENSION_TYPE,
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
