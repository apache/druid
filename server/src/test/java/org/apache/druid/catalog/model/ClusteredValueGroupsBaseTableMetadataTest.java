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
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DoubleDimensionHandler;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusteredValueGroupsBaseTableMetadataTest extends InitializedNullHandlingTest
{
  static {
    BuiltInTypesModule.registerHandlersAndSerde();
  }

  private final ObjectMapper mapper = new DefaultObjectMapper().setInjectableValues(
      new InjectableValues.Std()
          .addValue(ExprMacroTable.class, ExprMacroTable.nil())
          .addValue(DefaultColumnFormatConfig.class, new DefaultColumnFormatConfig(null, null, null, null))
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
        Collections.singletonList("tenant_lower"),
        VirtualColumns.create(
            new ExpressionVirtualColumn("tenant_lower", "lower(\"tenant\")", ColumnType.STRING, ExprMacroTable.nil())
        ), null
    );
    final String json = mapper.writeValueAsString(metadata);
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assertions.assertEquals(metadata, fromJson);
  }

  @Test
  public void testSerdeNoVirtualColumns() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Arrays.asList("tenant", "region"),
        null,
        null
    );
    final String json = mapper.writeValueAsString(metadata);
    Assertions.assertFalse(json.contains("virtualColumns"));
    Assertions.assertFalse(json.contains("columnSchemas"));
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assertions.assertEquals(metadata, fromJson);
  }

  @Test
  public void testSerdeWithColumnSchemas() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Arrays.asList(
            new StringDimensionSchema("region", DimensionSchema.MultiValueHandling.ARRAY, false),
            new AutoTypeColumnSchema("value", ColumnType.DOUBLE, null),
            new NestedDataColumnSchema("attrs", NestedDataColumnSchema.DEFAULT_FORMAT_VERSION)
        )
    );
    final String json = mapper.writeValueAsString(metadata);
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assertions.assertEquals(metadata, fromJson);
  }

  @Test
  public void testSerdeAsUntypedMapValue() throws Exception
  {
    // Catalog property values are serialized from a Map<String, Object>, where Jackson serializes by runtime type;
    // the type discriminator must survive that path (it is an EXISTING_PROPERTY for this reason).
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        null
    );
    final String json = mapper.writeValueAsString(Collections.singletonMap("baseTable", metadata));
    Assertions.assertTrue(json.contains("\"type\":\"clusteredValueGroups\""));
    final Map<String, Object> untyped = mapper.readValue(json, new TypeReference<>() {});
    Assertions.assertEquals(
        metadata,
        mapper.convertValue(untyped.get("baseTable"), DatasourceBaseTableMetadata.class)
    );
  }

  @Test
  public void testCreateSpec()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        null
    );
    // The declared column order is the physical segment order, used verbatim; types map through Columns.druidType
    // with untyped -> STRING.
    Assertions.assertEquals(
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
    // A clustering column computed at ingest time: the virtual column materializes the stored, declared
    // 'tenant_lower' column, reading the stored 'tenant' column (virtual column inputs must themselves be stored
    // columns or other virtual columns).
    final VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("tenant_lower", "lower(\"tenant\")", ColumnType.STRING, ExprMacroTable.nil())
    );
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant_lower"),
        virtualColumns,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant_lower", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec("region", Columns.SQL_VARCHAR, null)
    );
    Assertions.assertEquals(
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .virtualColumns(virtualColumns)
                                                   .columns(
                                                       new StringDimensionSchema("tenant_lower"),
                                                       new LongDimensionSchema(Columns.TIME_COLUMN),
                                                       new StringDimensionSchema("tenant"),
                                                       new StringDimensionSchema("region")
                                                   )
                                                   .clusteringColumns("tenant_lower")
                                                   .build(),
        metadata.createSpec(columns)
    );
  }

  @Test
  public void testCreateSpecWithColumnSchemas()
  {
    // Column schemas customize the exact DimensionSchema used at segment creation without defining columns: the
    // declared column list remains the logical schema. 'region' keeps its declared STRING type but customizes
    // multi-value handling and disables the bitmap index; 'value' is stored as an auto column cast to its declared
    // DOUBLE type.
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Arrays.asList(
            new StringDimensionSchema("region", DimensionSchema.MultiValueHandling.ARRAY, false),
            new AutoTypeColumnSchema("value", ColumnType.DOUBLE, null)
        )
    );
    Assertions.assertEquals(
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema(Columns.TIME_COLUMN),
                                                       new StringDimensionSchema("region", DimensionSchema.MultiValueHandling.ARRAY, false),
                                                       new LongDimensionSchema("delta"),
                                                       new AutoTypeColumnSchema("value", ColumnType.DOUBLE, null)
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build(),
        metadata.createSpec(COLUMNS)
    );
  }

  @Test
  public void testCreateSpecAutoColumnSchemaCoercions()
  {
    // A declared FLOAT column may be customized with an auto schema cast to FLOAT: the auto schema itself stores
    // FLOAT as DOUBLE (the same coercion the default derivation relies on for FLOAT arrays). A column declared
    // COMPLEX<json> may use an uncast auto schema, since arbitrary shapes are that type's contract.
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Arrays.asList(
            new AutoTypeColumnSchema("ratio", ColumnType.FLOAT, null),
            AutoTypeColumnSchema.of("attrs")
        )
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("ratio", Columns.SQL_FLOAT, null),
        new ColumnSpec("attrs", ColumnType.NESTED_DATA.asTypeString(), null)
    );
    Assertions.assertEquals(
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema(Columns.TIME_COLUMN),
                                                       new AutoTypeColumnSchema("ratio", ColumnType.FLOAT, null),
                                                       AutoTypeColumnSchema.of("attrs")
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build(),
        metadata.createSpec(columns)
    );
  }

  @Test
  public void testCreateSpecUncastAutoColumnSchemaFails()
  {
    // An uncast auto column stores values as ingested rather than coercing to the declared type, so it is only
    // legal for columns declared COMPLEX<json>.
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Collections.singletonList(AutoTypeColumnSchema.of("value"))
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(
        e.getMessage().contains("columnSchemas entry [value] is an auto column schema without a castToType")
    );
  }

  @Test
  public void testCreateSpecAutoColumnSchemaCastMismatchFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Collections.singletonList(new AutoTypeColumnSchema("region", ColumnType.LONG, null))
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(
        e.getMessage().contains("columnSchemas entry [region] of type [LONG] does not match the column's declared type [STRING]")
    );
  }

  @Test
  public void testCreateSpecJsonColumnSchemaForNonJsonColumnFails()
  {
    // A json schema always stores COMPLEX<json>, so it may only customize columns declared as COMPLEX<json>.
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Collections.singletonList(new NestedDataColumnSchema("region", NestedDataColumnSchema.DEFAULT_FORMAT_VERSION))
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(
        e.getMessage().contains(
            "columnSchemas entry [region] of type [COMPLEX<json>] does not match the column's declared type [STRING]"
        )
    );
  }

  @Test
  public void testCreateSpecColumnSchemaForUndeclaredColumnFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Collections.singletonList(new StringDimensionSchema("no_such_column"))
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(
        e.getMessage().contains("columnSchemas entry [no_such_column] does not customize a declared column")
    );
  }

  @Test
  public void testCreateSpecColumnSchemaForClusteringColumnFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Collections.singletonList(new StringDimensionSchema("tenant", DimensionSchema.MultiValueHandling.ARRAY, false))
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(e.getMessage().contains("columnSchemas cannot customize clustering column [tenant]"));
  }

  @Test
  public void testCreateSpecColumnSchemaForTimeColumnFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Collections.singletonList(new LongDimensionSchema(Columns.TIME_COLUMN))
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(e.getMessage().contains("columnSchemas cannot customize [__time]"));
  }

  @Test
  public void testCreateSpecColumnSchemaTypeMismatchFails()
  {
    // 'region' is declared as STRING; a LONG dimension schema would contradict the logical schema SQL writes are
    // validated against.
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Collections.singletonList(new LongDimensionSchema("region"))
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(
        e.getMessage().contains("columnSchemas entry [region] of type [LONG] does not match the column's declared type [STRING]")
    );
  }

  @Test
  public void testCreateSpecNullColumnSchemaEntryFails() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = mapper.readValue(
        "{\"type\":\"clusteredValueGroups\",\"clusteringColumns\":[\"tenant\"],\"columnSchemas\":[null]}",
        DatasourceBaseTableMetadata.class
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(e.getMessage().contains("columnSchemas must not contain null entries"));
  }

  @Test
  public void testCreateSpecDuplicateColumnSchemasFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        Arrays.asList(
            new StringDimensionSchema("region"),
            new StringDimensionSchema("region", DimensionSchema.MultiValueHandling.ARRAY, false)
        )
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(e.getMessage().contains("columnSchemas contains duplicate entries for column [region]"));
  }

  @Test
  public void testCreateSpecMultipleClusteringColumns()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Arrays.asList("region", "tenant"),
        null,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("region", Columns.SQL_VARCHAR, null),
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("delta", Columns.SQL_BIGINT, null)
    );
    Assertions.assertEquals(
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
        null,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("tags", Columns.SQL_VARCHAR_ARRAY, null),
        new ColumnSpec("vals", Columns.SQL_BIGINT_ARRAY, null),
        new ColumnSpec("ratios", Columns.SQL_FLOAT_ARRAY, null),
        new ColumnSpec("attrs", ColumnType.NESTED_DATA.asTypeString(), null),
        // type prefixes match case-insensitively; these must not silently fall back to STRING
        new ColumnSpec("attrs2", "complex<json>", null),
        new ColumnSpec("vals2", "array<long>", null)
    );
    // Declared types are retained in the ingestion schema rather than left to inference: arrays cast an auto column
    // to the declared type (an all-null batch has no values to infer from; FLOAT ARRAY is stored as DOUBLE ARRAY by
    // the auto schema). COMPLEX<json> resolves through its dimension handler to an uncast auto column, which is how
    // json columns are stored everywhere else.
    Assertions.assertEquals(
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema(Columns.TIME_COLUMN),
                                                       new AutoTypeColumnSchema("tags", ColumnType.STRING_ARRAY, null),
                                                       new AutoTypeColumnSchema("vals", ColumnType.LONG_ARRAY, null),
                                                       new AutoTypeColumnSchema("ratios", ColumnType.DOUBLE_ARRAY, null),
                                                       AutoTypeColumnSchema.of("attrs"),
                                                       AutoTypeColumnSchema.of("attrs2"),
                                                       new AutoTypeColumnSchema("vals2", ColumnType.LONG_ARRAY, null)
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build(),
        metadata.createSpec(columns)
    );
  }

  /**
   * A complex type with no registered dimension handler cannot be stored, and the handler lookup reports it rather
   * than the type being rejected as unsupported in general: the handler may simply belong to an extension that is not
   * loaded on the service validating the spec.
   */
  @Test
  public void testCreateSpecComplexTypeWithoutHandlerFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("unique_things", "COMPLEX<hyperUnique>", null)
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
    Assertions.assertEquals(
        "Complex type[hyperUnique] for dimension[unique_things] is not a valid type",
        e.getMessage()
    );
  }

  /**
   * A complex type that does have a registered handler resolves through it, which is how types contributed by
   * extensions become declarable.
   */
  @Test
  public void testCreateSpecComplexTypeWithRegisteredHandler()
  {
    final String typeName = "clusteredBaseTableTestType";
    // Only getDimensionSchema is exercised; the handler's storage behavior is irrelevant to building a spec.
    DimensionHandlerUtils.registerDimensionHandlerProvider(
        typeName,
        name -> new DoubleDimensionHandler(name)
        {
          @Override
          public DimensionSchema getDimensionSchema(ColumnCapabilities capabilities)
          {
            return new TestComplexDimensionSchema(name, typeName);
          }
        }
    );

    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("sketch", StringUtils.format("COMPLEX<%s>", typeName), null)
    );

    final List<DimensionSchema> specColumns = metadata.createSpec(columns).getDimensionsSpec().getDimensions();
    final DimensionSchema stored = specColumns.get(specColumns.size() - 1);
    Assertions.assertEquals("sketch", stored.getName());
    Assertions.assertEquals(ColumnType.ofComplex(typeName), stored.getColumnType());
  }

  @Test
  public void testCreateSpecNestedTypeUsesRegisteredHandler()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null),
        new ColumnSpec("payload", ColumnType.NESTED_DATA.asTypeString(), null)
    );

    final List<DimensionSchema> specColumns = metadata.createSpec(columns).getDimensionsSpec().getDimensions();
    final DimensionSchema stored = specColumns.get(specColumns.size() - 1);
    Assertions.assertEquals(AutoTypeColumnSchema.of("payload"), stored);
    Assertions.assertEquals(ColumnType.NESTED_DATA, stored.getColumnType());
    // The schema a json column used to get here, retained for backwards compatibility, selects the same handler
    // (DimensionHandler has no equals, so compare the class and the dimension spec it hands out, which carries the
    // type the handler stores).
    final DimensionHandler<?, ?, ?> legacyHandler =
        new NestedDataColumnSchema("payload", NestedDataColumnSchema.DEFAULT_FORMAT_VERSION).getDimensionHandler();
    Assertions.assertEquals(legacyHandler.getClass(), stored.getDimensionHandler().getClass());
    Assertions.assertEquals(legacyHandler.getDimensionSpec(), stored.getDimensionHandler().getDimensionSpec());
  }

  @Test
  public void testCreateSpecUnparseableTypeFails()
  {
    // A column declared WITHOUT a type defaults to STRING, but a declared type that does not parse must be rejected
    // rather than silently defaulted: the declared type is the physical segment schema here. (A malformed
    // parameterized type such as a missing closing bracket parses to null.)
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        null
    );
    for (String badType : new String[]{"COMPLEX<json", "ARRAY<LONG", "ARRAY<FOO>", "FOO"}) {
      final List<ColumnSpec> columns = Arrays.asList(
          new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
          new ColumnSpec(Columns.TIME_COLUMN, null, null),
          new ColumnSpec("busted", badType, null)
      );
      final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
      Assertions.assertTrue(
          e.getMessage().contains("column [busted] has an unrecognized type [" + badType + "]"),
          "expected unrecognized-type error for [" + badType + "] but got: " + e.getMessage()
      );
    }
  }

  @Test
  public void testCreateSpecClusteringColumnsNotLeadingPrefixFails()
  {
    // 'region' is declared, but not as part of the leading prefix of the column list; the declared order is the
    // physical segment order, so this is an error rather than a silent reorder.
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("region"),
        null,
        null
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(e.getMessage().contains("clusteringColumns must be the leading prefix of columns"));
  }

  @Test
  public void testCreateSpecClusteringColumnsWrongOrderFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Arrays.asList("tenant", "region"),
        null,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("region", Columns.SQL_VARCHAR, null),
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec(Columns.TIME_COLUMN, null, null)
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
    Assertions.assertTrue(e.getMessage().contains("clusteringColumns must be the leading prefix of columns"));
  }

  @Test
  public void testCreateSpecUndeclaredClusteringColumnFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("no_such_column"),
        null,
        null
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(e.getMessage().contains("clustering column [no_such_column] is not a declared column"));
  }

  @Test
  public void testCreateSpecMissingTimeColumnFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
        new ColumnSpec("region", Columns.SQL_VARCHAR, null)
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
    Assertions.assertTrue(e.getMessage().contains("must include [__time]"));
  }

  @Test
  public void testCreateSpecDisallowedClusteringTypeFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tags"),
        null,
        null
    );
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("tags", Columns.SQL_VARCHAR_ARRAY, null),
        new ColumnSpec(Columns.TIME_COLUMN, Columns.SQL_TIMESTAMP, null)
    );
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(columns));
    Assertions.assertTrue(e.getMessage().contains("unsupported type"));
  }

  @Test
  public void testCreateSpecEmptyClusteringColumnsFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(null, null, null);
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> metadata.createSpec(COLUMNS));
    Assertions.assertTrue(e.getMessage().contains("clusteringColumns must be non-empty"));
  }

  @Test
  public void testCreateSpecNoDeclaredColumnsFails()
  {
    final DatasourceBaseTableMetadata metadata = new ClusteredValueGroupsBaseTableMetadata(
        Collections.singletonList("tenant"),
        null,
        null
    );
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> metadata.createSpec(Collections.emptyList())
    );
    Assertions.assertTrue(e.getMessage().contains("without declared columns"));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ClusteredValueGroupsBaseTableMetadata.class)
                  .usingGetClass()
                  .verify();
  }

  /**
   * Minimal complex {@link DimensionSchema}, the shape an honest handler for a complex type returns: the column type
   * it reports is the type it was registered for.
   */
  private static class TestComplexDimensionSchema extends DimensionSchema
  {
    private final String typeName;

    TestComplexDimensionSchema(String name, String typeName)
    {
      super(name, null, false);
      this.typeName = typeName;
    }

    @Override
    public String getTypeName()
    {
      return typeName;
    }

    @Override
    public ColumnType getColumnType()
    {
      return ColumnType.ofComplex(typeName);
    }
  }
}
