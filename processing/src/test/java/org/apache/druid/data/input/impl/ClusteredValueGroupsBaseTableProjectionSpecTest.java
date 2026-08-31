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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class ClusteredValueGroupsBaseTableProjectionSpecTest extends InitializedNullHandlingTest
{
  private static ClusteredValueGroupsBaseTableProjectionSpec tenantSpec()
  {
    return ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(new StringDimensionSchema("tenant"), new StringDimensionSchema("region"), new LongDimensionSchema("__time"))
        .clusteringColumns("tenant")
        .build();
  }

  @Test
  void testWithQueryGranularityAddsVirtualGranularityColumn()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = tenantSpec().withQueryGranularity(Granularities.HOUR);

    final VirtualColumn vc = spec.getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertNotNull(vc);
    Assertions.assertEquals(Granularities.HOUR, Granularities.fromVirtualColumn(vc));

    // The rest of the spec is unchanged.
    Assertions.assertEquals(tenantSpec().getClusteringColumns(), spec.getClusteringColumns());
    Assertions.assertEquals(tenantSpec().getColumns(), spec.getColumns());
    Assertions.assertEquals(tenantSpec().getOrdering(), spec.getOrdering());
    Assertions.assertArrayEquals(tenantSpec().getMetrics(), spec.getMetrics());
  }

  @Test
  void testWithQueryGranularityNullAndNoneAreNoOps()
  {
    // Absent __virtualGranularity virtual column already means NONE, so null/NONE add nothing and return the same spec.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = tenantSpec();
    Assertions.assertSame(spec, spec.withQueryGranularity(null));
    Assertions.assertSame(spec, spec.withQueryGranularity(Granularities.NONE));
  }

  @Test
  void testWithQueryGranularityAllIsRejected()
  {
    // ALL would floor __time to a single constant (the interval start) for the whole segment, which clustered base
    // tables do not yet support, so it is rejected rather than silently ignored.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = tenantSpec();
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> spec.withQueryGranularity(Granularities.ALL)
    );
    Assertions.assertTrue(e.getMessage().contains("ALL"));
  }

  @Test
  void testGetQueryGranularityRoundTrips()
  {
    Assertions.assertEquals(
        Granularities.HOUR,
        tenantSpec().withQueryGranularity(Granularities.HOUR).getQueryGranularity()
    );
  }

  @Test
  void testGetQueryGranularityIsNoneWhenNoVirtualColumn()
  {
    Assertions.assertEquals(Granularities.NONE, tenantSpec().getQueryGranularity());
  }

  @Test
  void testHasEqualCompactionStateIgnoresQueryGranularity()
  {
    // Two specs that differ only in their query granularity are equivalent for compaction (query granularity is
    // compared by its own check), regardless of which granularity each carries.
    Assertions.assertTrue(tenantSpec().withQueryGranularity(Granularities.HOUR).hasEqualCompactionState(tenantSpec()));
    Assertions.assertTrue(tenantSpec().hasEqualCompactionState(tenantSpec().withQueryGranularity(Granularities.HOUR)));
    Assertions.assertTrue(
        tenantSpec().withQueryGranularity(Granularities.HOUR)
                    .hasEqualCompactionState(tenantSpec().withQueryGranularity(Granularities.DAY))
    );
  }

  @Test
  void testHasEqualCompactionStateComparesSchema()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec differentClustering =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(new StringDimensionSchema("tenant"), new StringDimensionSchema("region"), new LongDimensionSchema("__time"))
            .clusteringColumns("tenant", "region")
            .build();

    Assertions.assertFalse(tenantSpec().hasEqualCompactionState(differentClustering));
    Assertions.assertTrue(tenantSpec().hasEqualCompactionState(tenantSpec()));
  }

  @Test
  void testWithQueryGranularityIsIdempotentWhenAlreadyPresent()
  {
    // Once a query-granularity virtual column is present it is authoritative; a second call is a no-op and does not
    // double-add or change it (the compaction path attaches it up front, then MSQ generation calls this again).
    final ClusteredValueGroupsBaseTableProjectionSpec withGranularity =
        tenantSpec().withQueryGranularity(Granularities.HOUR);
    final ClusteredValueGroupsBaseTableProjectionSpec reapplied = withGranularity.withQueryGranularity(Granularities.DAY);

    Assertions.assertSame(withGranularity, reapplied);
    Assertions.assertEquals(
        Granularities.HOUR,
        Granularities.fromVirtualColumn(
            reapplied.getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
        )
    );
  }

  @Test
  void testVirtualColumnMaterializedFromStoredInputIsValid()
  {
    // region_upper := upper(region): the input region is stored and the output region_upper is stored -> valid.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("region_upper", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new StringDimensionSchema("region_upper"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build();
    Assertions.assertNotNull(spec.getVirtualColumns().getVirtualColumn("region_upper"));
  }

  @Test
  void testVirtualColumnChainWithUnstoredIntermediateIsValid()
  {
    // Chain: clustering column tenant_key := upper(v0) where v0 := lower(tenant). The physical leaf tenant is stored
    // and the clustering output tenant_key is stored; the intermediary v0 is NOT stored but feeds tenant_key, so it is
    // exempt from the output rule -> valid.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE),
            new ExpressionVirtualColumn("tenant_key", "upper(v0)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .columns(
            new StringDimensionSchema("tenant_key"),
            new StringDimensionSchema("tenant"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant_key")
        .build();
    Assertions.assertNotNull(spec.getVirtualColumns().getVirtualColumn("tenant_key"));
    Assertions.assertNotNull(spec.getVirtualColumns().getVirtualColumn("v0"));
  }

  @Test
  void testVirtualColumnWithUnretainedInputIsRejected()
  {
    // tenant_lower := lower(tenant) is materialized (the clustering column), but the raw tenant input is NOT stored, so
    // it can't be recomputed from stored columns -> rejected.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .virtualColumns(VirtualColumns.create(
                new ExpressionVirtualColumn("tenant_lower", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
            ))
            .columns(
                new StringDimensionSchema("tenant_lower"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant_lower")
            .build()
    );
    Assertions.assertTrue(e.getMessage().contains("[tenant]"));
  }

  @Test
  void testDanglingVirtualColumnIsRejected()
  {
    // region_upper := upper(region) reads a stored column, but its output is neither stored nor an input to another
    // virtual column -> dead metadata, rejected.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .virtualColumns(VirtualColumns.create(
                new ExpressionVirtualColumn("region_upper", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
            ))
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build()
    );
    Assertions.assertTrue(e.getMessage().contains("[region_upper]"));
  }

  @Test
  void testVirtualColumnOutputTypeMismatchIsRejected()
  {
    // region_upper := upper(region) produces STRING, but the column it materializes is declared LONG -> rejected.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .virtualColumns(VirtualColumns.create(
                new ExpressionVirtualColumn("region_upper", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
            ))
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new LongDimensionSchema("region_upper"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build()
    );
    Assertions.assertTrue(e.getMessage().contains("[region_upper]"));
    Assertions.assertTrue(e.getMessage().contains("STRING"));
    Assertions.assertTrue(e.getMessage().contains("LONG"));
  }

  @Test
  void testNumericVirtualColumnMatchingDeclaredTypeIsValid()
  {
    // region_len := strlen(region) produces LONG and is declared LONG -> accepted (proves numeric output-type inference
    // works, not just STRING).
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("region_len", "strlen(region)", ColumnType.LONG, TestExprMacroTable.INSTANCE)
        ))
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new LongDimensionSchema("region_len"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build();
    Assertions.assertNotNull(spec.getVirtualColumns().getVirtualColumn("region_len"));
  }

  @Test
  void testDotNotationVirtualColumnIsRejected()
  {
    // Dot-notation virtual columns are referenced as "name.subfield" and have no fixed output identity to materialize
    // or cluster on, so they are not supported in clustered base table specs.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .virtualColumns(VirtualColumns.create(new DotNotationVirtualColumn("dotty")))
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build()
    );
    Assertions.assertTrue(e.getMessage().contains("dot notation"));
    Assertions.assertTrue(e.getMessage().contains("[dotty]"));
  }

  @Test
  void testWithAdditionalColumnsAppendsAfterDeclaredColumns()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = tenantSpec().withAdditionalColumns(
        ImmutableList.of(new LongDimensionSchema("cnt"), new StringDimensionSchema("city"))
    );

    Assertions.assertEquals(
        ImmutableList.of("tenant", "region", "__time", "cnt", "city"),
        spec.getColumns().stream().map(DimensionSchema::getName).collect(Collectors.toList())
    );
    // The clustering prefix is untouched: the appended columns are stored and sorted by, but not clustered on.
    Assertions.assertEquals(ImmutableList.of("tenant"), spec.getClusteringColumnNames());
    Assertions.assertEquals(
        ImmutableList.of("tenant"),
        spec.getClusteringColumns().stream().map(DimensionSchema::getName).collect(Collectors.toList())
    );
    Assertions.assertEquals(
        ImmutableList.of("region", "__time", "cnt", "city"),
        spec.getNonClusteringColumns().stream().map(DimensionSchema::getName).collect(Collectors.toList())
    );
    // Rows are physically sorted by every column present, so the appended columns join the ordering at the end.
    Assertions.assertEquals(
        ImmutableList.of(
            OrderBy.ascending("tenant"),
            OrderBy.ascending("region"),
            OrderBy.ascending("__time"),
            OrderBy.ascending("cnt"),
            OrderBy.ascending("city")
        ),
        spec.getOrdering()
    );
    Assertions.assertEquals(spec.getColumns(), spec.getDimensionsSpec().getDimensions());
  }

  @Test
  void testWithAdditionalColumnsNullAndEmptyAreNoOps()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = tenantSpec();
    Assertions.assertSame(spec, spec.withAdditionalColumns(null));
    Assertions.assertSame(spec, spec.withAdditionalColumns(Collections.emptyList()));
  }

  @Test
  void testWithAdditionalColumnsKeepsVirtualColumnsAndQueryGranularity()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("region_upper", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new StringDimensionSchema("region_upper"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build()
        .withQueryGranularity(Granularities.HOUR)
        .withAdditionalColumns(ImmutableList.of(new LongDimensionSchema("cnt")));

    Assertions.assertNotNull(spec.getVirtualColumns().getVirtualColumn("region_upper"));
    Assertions.assertEquals(Granularities.HOUR, spec.getQueryGranularity());
    Assertions.assertEquals("cnt", spec.getColumns().get(spec.getColumns().size() - 1).getName());
  }

  @Test
  void testWithAdditionalColumnsRejectsTimeColumn()
  {
    // __time marks a position in the column list, so it can never arrive as an appended extra.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> tenantSpec().withAdditionalColumns(ImmutableList.of(new LongDimensionSchema("__time")))
    );
    Assertions.assertTrue(e.getMessage().contains("[__time]"));
  }

  @Test
  void testWithAdditionalColumnsRejectsDuplicateOfDeclaredColumn()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> tenantSpec().withAdditionalColumns(ImmutableList.of(new StringDimensionSchema("region")))
    );
    Assertions.assertTrue(e.getMessage().contains("duplicate name [region]"));
  }

  @Test
  void testWithAdditionalColumnsRejectsDuplicateOfVirtualColumnOutput()
  {
    // region_upper is materialized by a virtual column, so an incoming column of the same name would be computed by
    // the virtual column rather than read; the arriving values would be ignored.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .virtualColumns(VirtualColumns.create(
                new ExpressionVirtualColumn("region_upper", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
            ))
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new StringDimensionSchema("region_upper"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build()
            .withAdditionalColumns(ImmutableList.of(new StringDimensionSchema("region_upper")))
    );
    Assertions.assertTrue(e.getMessage().contains("[region_upper]"));
    Assertions.assertTrue(e.getMessage().contains("computed by a virtual column"));
  }

  @Test
  void testWithAdditionalColumnsRejectsUnmaterializedVirtualColumnName()
  {
    // Chain: tenant_key := upper(v0), v0 := lower(tenant); the intermediate v0 is not a stored column. Appending a
    // column named v0 would pass the constructor's rules (its output would simply become stored), but ingest reads
    // virtual columns first, so the arriving v0 values would be silently discarded.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE),
            new ExpressionVirtualColumn("tenant_key", "upper(v0)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .columns(
            new StringDimensionSchema("tenant_key"),
            new StringDimensionSchema("tenant"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant_key")
        .build();
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> spec.withAdditionalColumns(ImmutableList.of(new StringDimensionSchema("v0")))
    );
    Assertions.assertTrue(e.getMessage().contains("[v0]"));
    Assertions.assertTrue(e.getMessage().contains("computed by a virtual column"));
  }

  /**
   * Minimal test-only virtual column whose only meaningful behavior is {@link #usesDotNotation()} returning true; the
   * selector/capability methods are never reached by spec validation. (No core virtual column uses dot notation.)
   */
  private static final class DotNotationVirtualColumn implements VirtualColumn
  {
    private final String name;

    private DotNotationVirtualColumn(String name)
    {
      this.name = name;
    }

    @Override
    public String getOutputName()
    {
      return name;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnCapabilities capabilities(String columnName)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> requiredColumns()
    {
      return Collections.emptyList();
    }

    @Override
    public boolean usesDotNotation()
    {
      return true;
    }

    @Override
    public byte[] getCacheKey()
    {
      throw new UnsupportedOperationException();
    }
  }
}
