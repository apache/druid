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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.projections.ClusterGroupSchemaTestHelpers;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class QueryableIndexCursorFactoryClusteredTest
{
  /**
   * Build a clustered summary from typed tenant values. The helper derives dictionaries and dictionary-IDs for each
   * group; the resulting specs are nested in the summary and wired via setSummary inside the schema constructor.
   */
  private static ClusteredValueGroupsBaseTableSchema summary(String... tenants)
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
    return new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of("tenant", ColumnHolder.TIME_COLUMN_NAME, "metric"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        List.of(OrderBy.ascending("tenant"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        clustering,
        null,
        built.dictionaries(),
        built.specs()
    );
  }

  /**
   * Bare-bones {@link QueryableIndex} that exposes the clustered cluster summary + groups via the interface
   * defaults, plus a stubbed {@link #getClusterGroupQueryableIndex} that returns a fake group sub-index whose
   * column capabilities are programmable. Used to drive {@link QueryableIndexCursorFactory#getRowSignature} and
   * {@link QueryableIndexCursorFactory#getColumnCapabilities} for clustered segments without standing up a real
   * V10 segment file.
   */
  private static class FakeClusteredIndex implements QueryableIndex
  {
    private final ClusteredValueGroupsBaseTableSchema summary;
    private final Function<String, ColumnCapabilities> dataColumnCaps;

    FakeClusteredIndex(
        ClusteredValueGroupsBaseTableSchema summary,
        Function<String, ColumnCapabilities> dataColumnCaps
    )
    {
      this.summary = summary;
      this.dataColumnCaps = dataColumnCaps;
    }

    @Override
    public org.joda.time.Interval getDataInterval()
    {
      return Intervals.of("2025-01-01/2025-01-02");
    }

    @Override
    public int getNumRows()
    {
      return 0;
    }

    @Override
    public Indexed<String> getAvailableDimensions()
    {
      return new ListIndexed<>(List.of());
    }

    @Override
    public BitmapFactory getBitmapFactoryForDimensions()
    {
      return new RoaringBitmapFactory();
    }

    @Nullable
    @Override
    public Metadata getMetadata()
    {
      return null;
    }

    @Override
    public Map<String, DimensionHandler> getDimensionHandlers()
    {
      return Collections.emptyMap();
    }

    @Override
    public List<String> getColumnNames()
    {
      return Collections.emptyList();
    }

    @Nullable
    @Override
    public BaseColumnHolder getColumnHolder(String columnName)
    {
      return null;
    }

    @Override
    public List<OrderBy> getOrdering()
    {
      return summary.getOrdering();
    }

    @Override
    public void close()
    {
    }

    @Nullable
    @Override
    public ClusteredValueGroupsBaseTableSchema getClusteredBaseSummary()
    {
      return summary;
    }

    @Override
    public List<TableClusterGroupSpec> getClusterGroupSchemas()
    {
      return summary.getClusterGroups();
    }

    @Nullable
    @Override
    public QueryableIndex getClusterGroupQueryableIndex(TableClusterGroupSpec groupSpec)
    {
      // Return a fake sub-index that answers getColumnCapabilities from the supplied function. Other methods
      // throw if reached so we'd notice if the cursor factory's clustered paths start needing more.
      return new QueryableIndex()
      {
        @Override
        public org.joda.time.Interval getDataInterval()
        {
          return Intervals.of("2025-01-01/2025-01-02");
        }

        @Override
        public int getNumRows()
        {
          return 0;
        }

        @Override
        public Indexed<String> getAvailableDimensions()
        {
          return new ListIndexed<>(List.of());
        }

        @Override
        public BitmapFactory getBitmapFactoryForDimensions()
        {
          return new RoaringBitmapFactory();
        }

        @Nullable
        @Override
        public Metadata getMetadata()
        {
          return null;
        }

        @Override
        public Map<String, DimensionHandler> getDimensionHandlers()
        {
          return Collections.emptyMap();
        }

        @Override
        public List<String> getColumnNames()
        {
          return Collections.emptyList();
        }

        @Nullable
        @Override
        public BaseColumnHolder getColumnHolder(String columnName)
        {
          return null;
        }

        @Nullable
        @Override
        public ColumnCapabilities getColumnCapabilities(String column)
        {
          return dataColumnCaps.apply(column);
        }

        @Override
        public List<OrderBy> getOrdering()
        {
          return Collections.emptyList();
        }

        @Override
        public void close()
        {
        }
      };
    }
  }

  private static ColumnCapabilities longCaps()
  {
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
  }

  private static ColumnCapabilities timeCaps()
  {
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
  }

  @Test
  void testGetRowSignatureSourcesClusteringColumnFromSummaryAndDataColumnFromGroup()
  {
    final FakeClusteredIndex index = new FakeClusteredIndex(
        summary("acme", "globex"),
        col -> {
          if (ColumnHolder.TIME_COLUMN_NAME.equals(col)) {
            return timeCaps();
          }
          if ("metric".equals(col)) {
            return longCaps();
          }
          return null;
        }
    );

    QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index)
    );

    RowSignature sig = factory.getRowSignature();
    Assertions.assertEquals(ColumnType.STRING, sig.getColumnType("tenant").orElseThrow());
    Assertions.assertEquals(ColumnType.LONG, sig.getColumnType(ColumnHolder.TIME_COLUMN_NAME).orElseThrow());
    Assertions.assertEquals(ColumnType.LONG, sig.getColumnType("metric").orElseThrow());
  }

  @Test
  void testGetColumnCapabilitiesForClusteringColumnFromSummary()
  {
    final FakeClusteredIndex index = new FakeClusteredIndex(
        summary("acme"),
        col -> null
    );

    QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index)
    );

    ColumnCapabilities tenantCaps = factory.getColumnCapabilities("tenant");
    Assertions.assertNotNull(tenantCaps);
    Assertions.assertTrue(tenantCaps.is(ValueType.STRING));
  }

  @Test
  void testGetColumnCapabilitiesForDataColumnFromFirstGroup()
  {
    final FakeClusteredIndex index = new FakeClusteredIndex(
        summary("acme", "globex"),
        col -> "metric".equals(col) ? longCaps() : null
    );

    QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index)
    );

    ColumnCapabilities metricCaps = factory.getColumnCapabilities("metric");
    Assertions.assertNotNull(metricCaps);
    Assertions.assertTrue(metricCaps.is(ValueType.LONG));
  }

  @Test
  void testGetColumnCapabilitiesForUnknownColumnIsNull()
  {
    final FakeClusteredIndex index = new FakeClusteredIndex(
        summary("acme"),
        col -> null
    );

    QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index)
    );

    Assertions.assertNull(factory.getColumnCapabilities("unknown"));
  }
}
