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
import org.apache.druid.data.input.impl.RollupTableProjectionSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
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
 * The column-derivation rules shared with the other layouts are covered exhaustively in
 * {@link ClusteredValueGroupsBaseTableMetadataTest} and route through the same {@code BaseTableColumns} helper; this
 * covers what the rollup layout adds — splitting the declared columns into grouping and metric columns by aggregator
 * name.
 */
public class RollupTableBaseTableMetadataTest extends InitializedNullHandlingTest
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
      new ColumnSpec("total", Columns.SQL_BIGINT, null)
  );

  private static VirtualColumns hourCarrier()
  {
    return VirtualColumns.create(
        Granularities.toVirtualColumn(Granularities.HOUR, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
    );
  }

  private static AggregatorFactory[] totalSum()
  {
    return new AggregatorFactory[]{new LongSumAggregatorFactory("total", "total")};
  }

  @Test
  public void testSerde() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = new RollupTableBaseTableMetadata(hourCarrier(), totalSum(), null);
    final String json = mapper.writeValueAsString(metadata);
    Assertions.assertTrue(json.contains("\"type\":\"rollupTable\""), json);
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assertions.assertEquals(metadata, fromJson);
  }

  @Test
  public void testSerdeEmpty() throws Exception
  {
    final DatasourceBaseTableMetadata metadata = new RollupTableBaseTableMetadata(null, null, null);
    final String json = mapper.writeValueAsString(metadata);
    Assertions.assertFalse(json.contains("virtualColumns"));
    Assertions.assertFalse(json.contains("aggregators"));
    Assertions.assertFalse(json.contains("columnSchemas"));
    final DatasourceBaseTableMetadata fromJson = mapper.readValue(json, DatasourceBaseTableMetadata.class);
    Assertions.assertEquals(metadata, fromJson);
  }

  @Test
  public void testCreateSpec()
  {
    final RollupTableProjectionSpec spec =
        new RollupTableBaseTableMetadata(hourCarrier(), totalSum(), null).createSpec(COLUMNS);

    // Declared columns split by aggregator name: everything else is a grouping column, in declared order; the carrier
    // rides through as the query granularity, and the layout advertises rollup.
    Assertions.assertEquals(
        List.of("tenant", Columns.TIME_COLUMN),
        spec.getGroupingColumns().stream().map(DimensionSchema::getName).collect(Collectors.toList())
    );
    Assertions.assertArrayEquals(totalSum(), spec.getMetrics());
    Assertions.assertEquals(Granularities.HOUR, spec.getQueryGranularity());
    Assertions.assertTrue(spec.isRollup());
  }

  @Test
  public void testCreateSpecWithoutAggregatorsCollapsesDuplicates()
  {
    // No aggregators is a dedup table: every declared column is a grouping column.
    final RollupTableProjectionSpec spec = new RollupTableBaseTableMetadata(null, null, null).createSpec(
        Arrays.asList(
            new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
            new ColumnSpec(Columns.TIME_COLUMN, Columns.SQL_TIMESTAMP, null)
        )
    );
    Assertions.assertEquals(0, spec.getMetrics().length);
    Assertions.assertTrue(spec.isRollup());
  }

  @Test
  public void testCreateSpecGroupingColumnAfterMetricColumnFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new RollupTableBaseTableMetadata(null, totalSum(), null).createSpec(
            Arrays.asList(
                new ColumnSpec(Columns.TIME_COLUMN, Columns.SQL_TIMESTAMP, null),
                new ColumnSpec("total", Columns.SQL_BIGINT, null),
                new ColumnSpec("tenant", Columns.SQL_VARCHAR, null)
            )
        )
    );
    Assertions.assertTrue(
        e.getMessage().contains("grouping column [tenant] is declared after metric column [total]"),
        e.getMessage()
    );
  }

  /**
   * A declared metric column named for the query-granularity carrier slips past the grouping-column carrier-name
   * check (it is matched to its aggregator, not treated as a grouping column), so the spec's aggregator validation is
   * what rejects it: the carrier virtual column would shadow the metric once a query granularity is attached.
   */
  @Test
  public void testCreateSpecAggregatorNamedForGranularityCarrierFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new RollupTableBaseTableMetadata(
            null,
            new AggregatorFactory[]{
                new LongSumAggregatorFactory(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME, "cnt")
            },
            null
        ).createSpec(
            Arrays.asList(
                new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
                new ColumnSpec(Columns.TIME_COLUMN, Columns.SQL_TIMESTAMP, null),
                new ColumnSpec(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME, Columns.SQL_BIGINT, null)
            )
        )
    );
    Assertions.assertTrue(
        e.getMessage().contains(
            "aggregator cannot be named [" + Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME + "]"
        ),
        e.getMessage()
    );
  }

  @Test
  public void testCreateSpecAggregatorWithoutDeclaredColumnFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new RollupTableBaseTableMetadata(
            null,
            new AggregatorFactory[]{new LongSumAggregatorFactory("nope", "nope")},
            null
        ).createSpec(COLUMNS)
    );
    Assertions.assertTrue(
        e.getMessage().contains("aggregator [nope] does not fill a declared column"),
        e.getMessage()
    );
  }

  @Test
  public void testCreateSpecMetricTypeMismatchFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new RollupTableBaseTableMetadata(null, totalSum(), null).createSpec(
            Arrays.asList(
                new ColumnSpec("tenant", Columns.SQL_VARCHAR, null),
                new ColumnSpec(Columns.TIME_COLUMN, Columns.SQL_TIMESTAMP, null),
                new ColumnSpec("total", Columns.SQL_DOUBLE, null)
            )
        )
    );
    Assertions.assertTrue(
        e.getMessage().contains("metric column [total] is declared as type [DOUBLE], but its aggregator produces [LONG]"),
        e.getMessage()
    );
  }

  @Test
  public void testCreateSpecColumnSchemaForMetricColumnFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new RollupTableBaseTableMetadata(
            null,
            totalSum(),
            Collections.singletonList(new StringDimensionSchema("total"))
        ).createSpec(COLUMNS)
    );
    Assertions.assertTrue(
        e.getMessage().contains("columnSchemas cannot customize metric column [total]"),
        e.getMessage()
    );
  }

  @Test
  public void testCreateSpecColumnSchemaForGroupingColumnApplied()
  {
    final RollupTableProjectionSpec spec = new RollupTableBaseTableMetadata(
        null,
        totalSum(),
        Collections.singletonList(
            new StringDimensionSchema("tenant", DimensionSchema.MultiValueHandling.ARRAY, false)
        )
    ).createSpec(COLUMNS);
    Assertions.assertEquals(
        new StringDimensionSchema("tenant", DimensionSchema.MultiValueHandling.ARRAY, false),
        spec.getGroupingColumns().get(0)
    );
  }

  @Test
  public void testCreateSpecNoDeclaredColumnsFails()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new RollupTableBaseTableMetadata(null, totalSum(), null).createSpec(null)
    );
    Assertions.assertTrue(
        e.getMessage().contains("Cannot define a [rollupTable] base table without declared columns"),
        e.getMessage()
    );
  }
}
