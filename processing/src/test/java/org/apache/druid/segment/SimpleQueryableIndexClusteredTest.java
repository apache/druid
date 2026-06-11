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

package org.apache.druid.segment;

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.projections.ClusterGroupSchemaTestHelpers;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Coverage for {@link SimpleQueryableIndex}'s clustered base table awareness: the constructor accepts an empty
 * columns map for clustered segments, and the cluster-aware helpers surface the summary and per-group schemas that
 * {@link org.apache.druid.segment.IndexIO}'s V10 loader collects from a clustered segment.
 */
class SimpleQueryableIndexClusteredTest
{
  /**
   * Build a one-column STRING-clustered summary plus its specs from typed tenant values. Returns the helper's
   * {@link ClusterGroupSchemaTestHelpers.Built} bundle plus the constructed schema (whose {@code clusterGroups} are
   * the same spec instances as {@code built.specs()}, wired via setSummary by the schema constructor).
   */
  private static ClusterGroupSchemaTestHelpers.Built summary(String... tenants)
  {
    final RowSignature clustering = RowSignature.builder().add("tenant", ColumnType.STRING).build();
    final List<List<?>> tuples = new java.util.ArrayList<>(tenants.length);
    for (String t : tenants) {
      tuples.add(java.util.Arrays.asList(t));
    }
    final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
        clustering,
        tuples
    );
    new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of("tenant", ColumnHolder.TIME_COLUMN_NAME, "region", "metric"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        List.of(
            OrderBy.ascending("tenant"),
            OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME),
            OrderBy.ascending("region")
        ),
        clustering,
        null,
        built.dictionaries(),
        built.specs()
    );
    return built;
  }

  private static ClusteredValueGroupsBaseTableSchema summarySchema(String... tenants)
  {
    return summary(tenants).specs().get(0).getSummary();
  }

  private static SimpleQueryableIndex buildClusteredIndex(
      ClusteredValueGroupsBaseTableSchema summary,
      List<Map<String, Supplier<BaseColumnHolder>>> clusterGroupColumns,
      Map<String, Map<String, Supplier<BaseColumnHolder>>> projectionColumns
  )
  {
    final Metadata reconstructed = summary.asMetadata(null);
    return new SimpleQueryableIndex(
        Intervals.of("2025-01-01/2025-01-02"),
        new ListIndexed<>(List.of()),
        new RoaringBitmapFactory(),
        Map.of(),                       // clustered summary has no top-level columns
        null,                           // no SegmentFileMapper for in-memory test
        reconstructed,
        projectionColumns,
        summary,
        clusterGroupColumns
    )
    {
      @Override
      public Metadata getMetadata()
      {
        return reconstructed;
      }

      @Override
      public int getNumRows()
      {
        return 0;
      }
    };
  }

  @Test
  void testConstructorAcceptsEmptyColumnsForClusteredSegment()
  {
    // Pre-Phase-2 the constructor would NPE here because __time isn't in the columns map.
    Assertions.assertDoesNotThrow(
        () -> buildClusteredIndex(summarySchema("acme"), List.of(Map.of()), Map.of())
    );
  }

  @Test
  void testGetClusteredBaseSummaryReturnsSummary()
  {
    final ClusteredValueGroupsBaseTableSchema s = summarySchema("acme");
    SimpleQueryableIndex index = buildClusteredIndex(s, List.of(Map.of()), Map.of());
    Assertions.assertSame(s, index.getClusteredBaseSummary());
  }

  @Test
  void testGetClusterGroupSchemasReturnsGroupsInOrder()
  {
    final ClusterGroupSchemaTestHelpers.Built built = summary("acme", "globex");
    final ClusteredValueGroupsBaseTableSchema sum = built.specs().get(0).getSummary();
    SimpleQueryableIndex index = buildClusteredIndex(sum, List.of(Map.of(), Map.of()), Map.of());
    List<TableClusterGroupSpec> result = index.getClusterGroupSchemas();
    Assertions.assertEquals(2, result.size());
    Assertions.assertSame(built.specs().get(0), result.get(0));
    Assertions.assertSame(built.specs().get(1), result.get(1));
  }

  @Test
  void testNonClusteredIndexHasNullSummaryAndEmptyGroups()
  {
    // Use the no-cluster-args constructor variant directly via the overload that takes no cluster params.
    SimpleQueryableIndex index = new SimpleQueryableIndex(
        Intervals.of("2025-01-01/2025-01-02"),
        new ListIndexed<>(List.of()),
        new RoaringBitmapFactory(),
        Map.of(),
        null,
        null,
        null
    )
    {
      @Override
      public Metadata getMetadata()
      {
        return null;
      }

      @Override
      public int getNumRows()
      {
        return 0;
      }
    };

    Assertions.assertNull(index.getClusteredBaseSummary());
    Assertions.assertTrue(index.getClusterGroupSchemas().isEmpty());
  }

  @Test
  void testGetProjectionQueryableIndexResolvesAggregatesNotClusterGroups()
  {
    // Sanity: cluster groups don't pollute the aggregate-projection map. getProjection(spec) is the
    // aggregate-only path; cluster-group dispatch is QueryableIndexCursorFactory's job.
    SimpleQueryableIndex index = buildClusteredIndex(
        summarySchema("acme"),
        List.of(Map.of()),
        Map.of()
    );
    // No aggregate projections were added, so no aggregate by name "tenant=acme" exists either.
    Assertions.assertThrows(
        NullPointerException.class,
        () -> index.getProjectionQueryableIndex("does-not-exist")
    );
  }
}
