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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TableProjectionSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The column-derivation rules shared with the clustered layout (type retention, complex handler resolution,
 * columnSchemas customization validation) are covered exhaustively in {@link ClusteredValueGroupsBaseTableMetadataTest}
 * and route through the same {@code BaseTableColumns} helper; this covers what is specific to the plain layout.
 */
public class TableBaseTableMetadataTest extends InitializedNullHandlingTest
{
  static {
    BuiltInTypesModule.registerHandlersAndSerde();
  }

  // The granularity macro table (not nil) so the carrier's timestamp_floor expression can be parsed back.
  private final ObjectMapper mapper = new DefaultObjectMapper().setInjectableValues(
      new InjectableValues.Std()
          .addValue(ExprMacroTable.class, ExprMacroTable.granularity())
          .addValue(DefaultColumnFormatConfig.class, new DefaultColumnFormatConfig(null, null, null, null))
  );

  private static final List<ColumnSpec> COLUMNS = Arrays.asList(
      new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
      new ColumnSpec(Columns.TIME_COLUMN, Columns.SQL_TIMESTAMP, null),
      new ColumnSpec("delta", Columns.SQL_BIGINT, null)
  );

  private static VirtualColumns hourCarrier()
  {
    return VirtualColumns.create(
        Granularities.toVirtualColumn(Granularities.HOUR, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = new TableBaseTableMetadata(hourCarrier(), null);
    final String json = mapper.writeValueAsString(metadata);
    Assertions.assertTrue(json.contains("\"type\":\"table\""), json);
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assertions.assertEquals(metadata, fromJson);
  }

  @Test
  public void testSerdeEmpty() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = new TableBaseTableMetadata(null, null);
    final String json = mapper.writeValueAsString(metadata);
    Assertions.assertFalse(json.contains("virtualColumns"));
    Assertions.assertFalse(json.contains("columnSchemas"));
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assertions.assertEquals(metadata, fromJson);
  }

  @Test
  public void testCreateSpec()
  {
    final TableProjectionSpec spec = new TableBaseTableMetadata(hourCarrier(), null).createSpec(COLUMNS);

    // Declared catalog order is both storage and sort order, and the carrier rides through as the query granularity.
    Assertions.assertEquals(
        List.of("tenant", Columns.TIME_COLUMN, "delta"),
        spec.getColumns().stream().map(DimensionSchema::getName).collect(Collectors.toList())
    );
    Assertions.assertEquals(
        List.of(
            OrderBy.ascending("tenant"),
            OrderBy.ascending(Columns.TIME_COLUMN),
            OrderBy.ascending("delta")
        ),
        spec.getOrdering()
    );
    Assertions.assertEquals(Granularities.HOUR, spec.getQueryGranularity());
    Assertions.assertEquals(0, spec.getMetrics().length);
  }

  @Test
  public void testCreateSpecWithColumnSchemas()
  {
    final TableProjectionSpec spec = new TableBaseTableMetadata(
        null,
        Collections.singletonList(
            new StringDimensionSchema("tenant", DimensionSchema.MultiValueHandling.ARRAY, false)
        )
    ).createSpec(COLUMNS);

    Assertions.assertEquals(
        new StringDimensionSchema("tenant", DimensionSchema.MultiValueHandling.ARRAY, false),
        spec.getColumns().get(0)
    );
  }

  @Test
  public void testCreateSpecColumnSchemaForUndeclaredColumnFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new TableBaseTableMetadata(
            null,
            Collections.singletonList(new StringDimensionSchema("nope"))
        ).createSpec(COLUMNS)
    );
    Assertions.assertTrue(e.getMessage().contains("columnSchemas entry [nope] does not customize a declared column"));
  }

  /**
   * The plain spec accepts only the granularity carrier, so any other virtual column in the metadata surfaces as the
   * spec's rejection when the layout is validated.
   */
  @Test
  public void testCreateSpecNonCarrierVirtualColumnFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new TableBaseTableMetadata(
            VirtualColumns.create(
                Granularities.toVirtualColumn(Granularities.HOUR, "not_the_carrier")
            ),
            null
        ).createSpec(COLUMNS)
    );
    Assertions.assertTrue(e.getMessage().contains("virtual column [not_the_carrier] is not supported"), e.getMessage());
  }

  @Test
  public void testCreateSpecMissingTimeColumnFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new TableBaseTableMetadata(null, null).createSpec(
            Collections.singletonList(new ColumnSpec("tenant", Columns.SQL_VARCHAR, null))
        )
    );
    Assertions.assertTrue(e.getMessage().contains(Columns.TIME_COLUMN), e.getMessage());
  }

  @Test
  public void testCreateSpecNoDeclaredColumnsFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new TableBaseTableMetadata(null, null).createSpec(null)
    );
    Assertions.assertTrue(
        e.getMessage().contains("Cannot define a [table] base table without declared columns"),
        e.getMessage()
    );
  }
}
