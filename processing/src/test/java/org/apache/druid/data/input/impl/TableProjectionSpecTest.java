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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.stream.Collectors;

class TableProjectionSpecTest extends InitializedNullHandlingTest
{
  private static TableProjectionSpec pagesSpec()
  {
    return TableProjectionSpec.builder()
        .columns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"), new LongDimensionSchema("cnt"))
        .build();
  }

  @Test
  void testOrderingAndDimensionsSpecFollowDeclaredOrder()
  {
    final TableProjectionSpec spec = pagesSpec();

    // Declared order is both storage and sort order: every column ascending, in list order, with __time at its
    // declared position rather than forced first.
    Assertions.assertEquals(
        ImmutableList.of(OrderBy.ascending("page"), OrderBy.ascending("__time"), OrderBy.ascending("cnt")),
        spec.getOrdering()
    );
    Assertions.assertEquals(spec.getColumns(), spec.getDimensionsSpec().getDimensions());
    Assertions.assertFalse(spec.getDimensionsSpec().isForceSegmentSortByTime());
    Assertions.assertEquals(0, spec.getMetrics().length);
  }

  @Test
  void testWithQueryGranularityAddsVirtualGranularityColumn()
  {
    final TableProjectionSpec spec = pagesSpec().withQueryGranularity(Granularities.HOUR);

    final VirtualColumn vc = spec.getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertNotNull(vc);
    Assertions.assertEquals(Granularities.HOUR, Granularities.fromVirtualColumn(vc));

    // The rest of the spec is unchanged.
    Assertions.assertEquals(pagesSpec().getColumns(), spec.getColumns());
    Assertions.assertEquals(pagesSpec().getOrdering(), spec.getOrdering());
    Assertions.assertArrayEquals(pagesSpec().getMetrics(), spec.getMetrics());
  }

  @Test
  void testWithQueryGranularityNullAndNoneAreNoOps()
  {
    // Absent __virtualGranularity virtual column already means NONE, so null/NONE add nothing and return the same spec.
    final TableProjectionSpec spec = pagesSpec();
    Assertions.assertSame(spec, spec.withQueryGranularity(null));
    Assertions.assertSame(spec, spec.withQueryGranularity(Granularities.NONE));
  }

  @Test
  void testWithQueryGranularityAllIsRejected()
  {
    // ALL has no granularity virtual column representation, so accepting it would silently degrade to NONE when the
    // spec is read back; it is rejected rather than silently ignored.
    final TableProjectionSpec spec = pagesSpec();
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
        pagesSpec().withQueryGranularity(Granularities.HOUR).getQueryGranularity()
    );
  }

  @Test
  void testGetQueryGranularityIsNoneWhenNoVirtualColumn()
  {
    Assertions.assertEquals(Granularities.NONE, pagesSpec().getQueryGranularity());
  }

  @Test
  void testWithQueryGranularityIsIdempotentWhenAlreadyPresent()
  {
    // Once a query-granularity virtual column is present it is authoritative; a second call is a no-op and does not
    // double-add or change it (the compaction path attaches it up front, then MSQ generation calls this again).
    final TableProjectionSpec withGranularity = pagesSpec().withQueryGranularity(Granularities.HOUR);
    final TableProjectionSpec reapplied = withGranularity.withQueryGranularity(Granularities.DAY);

    Assertions.assertSame(withGranularity, reapplied);
    Assertions.assertEquals(
        Granularities.HOUR,
        Granularities.fromVirtualColumn(
            reapplied.getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
        )
    );
  }

  @Test
  void testHasEqualCompactionStateIgnoresQueryGranularity()
  {
    // Two specs that differ only in their query granularity are equivalent for compaction (query granularity is
    // compared by its own check), regardless of which granularity each carries.
    Assertions.assertTrue(pagesSpec().withQueryGranularity(Granularities.HOUR).hasEqualCompactionState(pagesSpec()));
    Assertions.assertTrue(pagesSpec().hasEqualCompactionState(pagesSpec().withQueryGranularity(Granularities.HOUR)));
    Assertions.assertTrue(
        pagesSpec().withQueryGranularity(Granularities.HOUR)
                   .hasEqualCompactionState(pagesSpec().withQueryGranularity(Granularities.DAY))
    );
  }

  @Test
  void testHasEqualCompactionStateComparesColumns()
  {
    final TableProjectionSpec differentColumns = TableProjectionSpec.builder()
        .columns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
        .build();

    Assertions.assertFalse(pagesSpec().hasEqualCompactionState(differentColumns));
    Assertions.assertTrue(pagesSpec().hasEqualCompactionState(pagesSpec()));
  }

  @Test
  void testHasEqualCompactionStateRejectsDifferentSpecType()
  {
    // A clustered spec over the same columns is a different physical layout, never equivalent to a plain table.
    final ClusteredValueGroupsBaseTableProjectionSpec clustered = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"), new LongDimensionSchema("cnt"))
        .clusteringColumns("page")
        .build();

    Assertions.assertFalse(pagesSpec().hasEqualCompactionState(clustered));
    Assertions.assertFalse(clustered.hasEqualCompactionState(pagesSpec()));
  }

  @Test
  void testWithAdditionalColumnsAppendsAfterDeclaredColumns()
  {
    final TableProjectionSpec spec = pagesSpec().withAdditionalColumns(
        ImmutableList.of(new LongDimensionSchema("clicks"), new StringDimensionSchema("city"))
    );

    Assertions.assertEquals(
        ImmutableList.of("page", "__time", "cnt", "clicks", "city"),
        spec.getColumns().stream().map(DimensionSchema::getName).collect(Collectors.toList())
    );
    // Rows are physically sorted by every column present, so the appended columns join the ordering at the end.
    Assertions.assertEquals(
        ImmutableList.of(
            OrderBy.ascending("page"),
            OrderBy.ascending("__time"),
            OrderBy.ascending("cnt"),
            OrderBy.ascending("clicks"),
            OrderBy.ascending("city")
        ),
        spec.getOrdering()
    );
    Assertions.assertEquals(spec.getColumns(), spec.getDimensionsSpec().getDimensions());
  }

  @Test
  void testWithAdditionalColumnsNullAndEmptyAreNoOps()
  {
    final TableProjectionSpec spec = pagesSpec();
    Assertions.assertSame(spec, spec.withAdditionalColumns(null));
    Assertions.assertSame(spec, spec.withAdditionalColumns(Collections.emptyList()));
  }

  @Test
  void testWithAdditionalColumnsKeepsQueryGranularity()
  {
    final TableProjectionSpec spec = pagesSpec()
        .withQueryGranularity(Granularities.HOUR)
        .withAdditionalColumns(ImmutableList.of(new LongDimensionSchema("clicks")));

    Assertions.assertEquals(Granularities.HOUR, spec.getQueryGranularity());
    Assertions.assertEquals("clicks", spec.getColumns().get(spec.getColumns().size() - 1).getName());
  }

  @Test
  void testWithAdditionalColumnsRejectsTimeColumn()
  {
    // __time marks a position in the column list, so it can never arrive as an appended extra.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> pagesSpec().withAdditionalColumns(ImmutableList.of(new LongDimensionSchema("__time")))
    );
    Assertions.assertTrue(e.getMessage().contains("[__time]"));
  }

  @Test
  void testWithAdditionalColumnsRejectsDuplicateOfDeclaredColumn()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> pagesSpec().withAdditionalColumns(ImmutableList.of(new StringDimensionSchema("page")))
    );
    Assertions.assertTrue(e.getMessage().contains("duplicate name [page]"));
  }

  @Test
  void testEmptyColumnsRejected()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> TableProjectionSpec.builder().build()
    );
    Assertions.assertTrue(e.getMessage().contains("non-empty"));
  }

  @Test
  void testDuplicateColumnRejected()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> TableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("page"),
                new StringDimensionSchema("page"),
                new LongDimensionSchema("__time")
            )
            .build()
    );
    Assertions.assertTrue(e.getMessage().contains("duplicate name [page]"));
  }

  @Test
  void testMissingTimeColumnRejected()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> TableProjectionSpec.builder().columns(new StringDimensionSchema("page")).build()
    );
    Assertions.assertTrue(e.getMessage().contains("[__time]"));
  }

  @Test
  void testGranularityCarrierInColumnsRejected()
  {
    // The query-granularity virtual column is a carrier in virtualColumns, never a stored column.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> TableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("page"),
                new LongDimensionSchema(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
            )
            .build()
    );
    Assertions.assertTrue(e.getMessage().contains(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME));
  }

  @Test
  void testNonCarrierVirtualColumnRejected()
  {
    // The standard segment-generation path does not evaluate spec virtual columns, so any virtual column other than
    // the query-granularity carrier would never be materialized; computed columns belong in a transformSpec.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> TableProjectionSpec.builder()
            .virtualColumns(VirtualColumns.create(
                new ExpressionVirtualColumn("page_upper", "upper(page)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
            ))
            .columns(
                new StringDimensionSchema("page"),
                new StringDimensionSchema("page_upper"),
                new LongDimensionSchema("__time")
            )
            .build()
    );
    Assertions.assertTrue(e.getMessage().contains("[page_upper]"));
    Assertions.assertTrue(e.getMessage().contains("transformSpec"));
  }

  @Test
  void testSerdeRoundTripsThroughInterface() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    final BaseTableProjectionSpec bare = pagesSpec();
    final String bareJson = mapper.writeValueAsString(bare);
    Assertions.assertTrue(bareJson.contains("\"type\":\"table\""), bareJson);
    Assertions.assertEquals(bare, mapper.readValue(bareJson, BaseTableProjectionSpec.class));

    final BaseTableProjectionSpec withGranularity = pagesSpec().withQueryGranularity(Granularities.HOUR);
    Assertions.assertEquals(
        withGranularity,
        mapper.readValue(mapper.writeValueAsString(withGranularity), BaseTableProjectionSpec.class)
    );
  }
}
