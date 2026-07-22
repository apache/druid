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

package org.apache.druid.catalog.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusteredValueGroupsBaseTableMetadataTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper().setInjectableValues(
      new InjectableValues.Std().addValue(ExprMacroTable.class, ExprMacroTable.nil())
  );

  // Declared order is the physical segment order: clustering columns lead, __time is an explicit positional column.
  private static final List<ColumnSpec> COLUMNS = Arrays.asList(
      new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
      new ColumnSpec(Columns.TIME_COLUMN, Columns.SQL_TIMESTAMP, null),
      new ColumnSpec("region", null, null),
      new ColumnSpec("delta", Columns.SQL_BIGINT, null),
      new ColumnSpec("value", Columns.SQL_DOUBLE, null)
  );

  @Test
  public void testSerde() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        VirtualColumns.create(
            new ExpressionVirtualColumn("tenant", "lower(\"raw_tenant\")", ColumnType.STRING, ExprMacroTable.nil())
        )
    );
    final String json = mapper.writeValueAsString(metadata);
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assert.assertEquals(metadata, fromJson);
  }

  @Test
  public void testSerdeNoVirtualColumns() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Arrays.asList("tenant", "region"),
        null
    );
    final String json = mapper.writeValueAsString(metadata);
    Assert.assertFalse(json.contains("virtualColumns"));
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assert.assertEquals(metadata, fromJson);
  }

  @Test
  public void testSerdeAsUntypedMapValue() throws Exception
  {
    // Catalog property values are serialized from a Map<String, Object>, where Jackson serializes by runtime type;
    // the type discriminator must survive that path (it is an EXISTING_PROPERTY for this reason).
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null
    );
    final String json = mapper.writeValueAsString(Collections.singletonMap("baseTable", metadata));
    Assert.assertTrue(json.contains("\"type\":\"clusteredValueGroups\""));
    final Map<String, Object> untyped = mapper.readValue(json, new TypeReference<>() {});
    Assert.assertEquals(
        metadata,
        mapper.convertValue(untyped.get("baseTable"), DatasourceBaseTableMetadata.class)
    );
  }

  @Test
  public void testCreateSpec()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null
    );
    // The declared column order is the physical segment order, used verbatim; types map through Columns.druidType
    // with untyped -> STRING.
    Assert.assertEquals(
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema(Columns.TIME_COLUMN),
                                                       new StringDimensionSchema("region"),
                                                       new LongDimensionSchema("delta"),
                                                       new DoubleDimensionSchema("value")
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build(),
        metadata.createSpec(COLUMNS)
    );
  }

  @Test
  public void testCreateSpecWithVirtualColumn()
  {
    // A clustering column computed at ingest time: the virtual column feeds the stored, declared 'tenant' column.
    final VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("tenant", "lower(\"raw_tenant\")", ColumnType.STRING, ExprMacroTable.nil())
    );
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        virtualColumns
    );
    Assert.assertEquals(
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .virtualColumns(virtualColumns)
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema(Columns.TIME_COLUMN),
                                                       new StringDimensionSchema("region"),
                                                       new LongDimensionSchema("delta"),
                                                       new DoubleDimensionSchema("value")
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build(),
        metadata.createSpec(COLUMNS)
    );
  }

  @Test
  public void testCreateSpecMultipleClusteringColumns()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Arrays.asList("region", "tenant"),
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("region", Columns.SQL_VARCHAR, null),
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("delta", Columns.SQL_BIGINT, null)
    );
    Assert.assertEquals(
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("region"),
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema(Columns.TIME_COLUMN),
                                                       new LongDimensionSchema("delta")
                                                   )
                                                   .clusteringColumns("region", "tenant")
                                                   .build(),
        metadata.createSpec(columns)
    );
  }

  @Test
  public void testCreateSpecRetainsDeclaredArrayAndNestedTypes()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("tags", Columns.SQL_VARCHAR_ARRAY, null),
        new ColumnSpec("vals", Columns.SQL_BIGINT_ARRAY, null),
        new ColumnSpec("ratios", Columns.SQL_FLOAT_ARRAY, null),
        new ColumnSpec("attrs", ColumnType.NESTED_DATA.asTypeString(), null)
    );
    // Declared types are retained in the ingestion schema rather than left to inference: arrays cast an auto column
    // to the declared type (an all-null batch has no values to infer from; FLOAT ARRAY is stored as DOUBLE ARRAY by
    // the auto schema), and COMPLEX<json> uses the dedicated nested column schema.
    Assert.assertEquals(
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema(Columns.TIME_COLUMN),
                                                       new AutoTypeColumnSchema("tags", ColumnType.STRING_ARRAY, null),
                                                       new AutoTypeColumnSchema("vals", ColumnType.LONG_ARRAY, null),
                                                       new AutoTypeColumnSchema("ratios", ColumnType.DOUBLE_ARRAY, null),
                                                       new NestedDataColumnSchema("attrs", 5)
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build(),
        metadata.createSpec(columns)
    );
  }

  @Test
  public void testCreateSpecUnsupportedComplexTypeFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("unique_things", "COMPLEX<hyperUnique>", null)
    );
    final DruidException e = Assert.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
    Assert.assertTrue(e.getMessage().contains("column [unique_things] has unsupported type [COMPLEX<hyperUnique>]"));
  }

  @Test
  public void testCreateSpecClusteringColumnsNotLeadingPrefixFails()
  {
    // 'region' is declared, but not as part of the leading prefix of the column list; the declared order is the
    // physical segment order, so this is an error rather than a silent reorder.
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("region"),
        null
    );
    final DruidException e = Assert.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assert.assertTrue(e.getMessage().contains("clusteringColumns must be the leading prefix of columns"));
  }

  @Test
  public void testCreateSpecClusteringColumnsWrongOrderFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Arrays.asList("tenant", "region"),
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("region", Columns.SQL_VARCHAR, null),
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null)
    );
    final DruidException e = Assert.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
    Assert.assertTrue(e.getMessage().contains("clusteringColumns must be the leading prefix of columns"));
  }

  @Test
  public void testCreateSpecUndeclaredClusteringColumnFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("no_such_column"),
        null
    );
    final DruidException e = Assert.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assert.assertTrue(e.getMessage().contains("clustering column [no_such_column] is not a declared column"));
  }

  @Test
  public void testCreateSpecMissingTimeColumnFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec("region", Columns.SQL_VARCHAR, null)
    );
    final DruidException e = Assert.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
    Assert.assertTrue(e.getMessage().contains("must include [__time]"));
  }

  @Test
  public void testCreateSpecDisallowedClusteringTypeFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tags"),
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tags", Columns.SQL_VARCHAR_ARRAY, null),
        new ColumnSpec(Columns.TIME_COLUMN, Columns.SQL_TIMESTAMP, null)
    );
    final DruidException e = Assert.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
    Assert.assertTrue(e.getMessage().contains("unsupported type"));
  }

  @Test
  public void testCreateSpecEmptyClusteringColumnsFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(null, null);
    final DruidException e = Assert.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assert.assertTrue(e.getMessage().contains("clusteringColumns must be non-empty"));
  }

  @Test
  public void testCreateSpecNoDeclaredColumnsFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null
    );
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> metadata.createSpec(Collections.emptyList())
    );
    Assert.assertTrue(e.getMessage().contains("without declared columns"));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ClusteredValueGroupsBaseTableMetadata.class)
                  .usingGetClass()
                  .verify();
  }
}
