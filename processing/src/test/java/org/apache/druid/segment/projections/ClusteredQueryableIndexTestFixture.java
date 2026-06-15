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

package org.apache.druid.segment.projections;

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test fixture that wires real per-group {@link QueryableIndex} instances (built via {@link IndexBuilder}) behind a
 * clustered {@link SimpleQueryableIndex}, so cursor-factory tests can exercise the full per-group
 * {@link ClusteringColumnSelectorFactory} wrapping {@link org.apache.druid.segment.ConcatenatingCursor} pipeline
 * against actual indexed data without a real writer.
 * <p>
 * Each group's index carries only the non-clustering dimensions/metrics; clustering columns are constants captured
 * on the spec, never on the per-group index. The fixture's {@link #segmentIndex()} overrides
 * {@link QueryableIndex#getClusterGroupQueryableIndex(TableClusterGroupSpec)} to return the corresponding real
 * per-group index, so {@link org.apache.druid.segment.QueryableIndexCursorFactory}'s clustered dispatch sees the
 * same shape it will once the writer side lands. Designed to be swapped out for real clustered segments at that
 * point without test-shape changes.
 * <p>
 * Holds {@link AutoCloseable} state (per-group mmap'd indexes + a temp directory), so callers should manage it via
 * {@link Closeable#close()} or a {@code try-with-resources}.
 */
public final class ClusteredQueryableIndexTestFixture implements Closeable
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final QueryableIndex segmentIndex;
  private final ClusteredValueGroupsBaseTableSchema summary;
  private final Closer closer;

  private ClusteredQueryableIndexTestFixture(
      QueryableIndex segmentIndex,
      ClusteredValueGroupsBaseTableSchema summary,
      Closer closer
  )
  {
    this.segmentIndex = segmentIndex;
    this.summary = summary;
    this.closer = closer;
  }

  /**
   * The clustered "segment" {@link QueryableIndex}; hand this to
   * {@link org.apache.druid.segment.QueryableIndexCursorFactory}.
   */
  public QueryableIndex segmentIndex()
  {
    return segmentIndex;
  }

  public ClusteredValueGroupsBaseTableSchema summary()
  {
    return summary;
  }

  public List<TableClusterGroupSpec> specs()
  {
    return summary.getClusterGroups();
  }

  @Override
  public void close()
  {
    try {
      closer.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static final class Builder
  {
    private Interval interval;
    private RowSignature clusteringColumns;
    private DimensionsSpec dimensionsSpec = DimensionsSpec.EMPTY;
    private AggregatorFactory[] metrics = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    private boolean rollup = false;
    private final List<GroupDescriptor> groups = new ArrayList<>();

    private Builder()
    {
    }

    public Builder interval(Interval i)
    {
      this.interval = i;
      return this;
    }

    public Builder clusteringColumns(RowSignature sig)
    {
      this.clusteringColumns = sig;
      return this;
    }

    public Builder nonClusteringDimensions(DimensionSchema... dims)
    {
      this.dimensionsSpec = DimensionsSpec.builder().setDimensions(Arrays.asList(dims)).build();
      return this;
    }

    public Builder metrics(AggregatorFactory... m)
    {
      this.metrics = m;
      return this;
    }

    public Builder rollup(boolean r)
    {
      this.rollup = r;
      return this;
    }

    /**
     * Add one cluster group with its positional clustering tuple + per-group rows (non-clustering columns only).
     */
    public Builder addGroup(List<?> clusteringValues, List<InputRow> rows)
    {
      groups.add(new GroupDescriptor(clusteringValues, rows));
      return this;
    }

    public ClusteredQueryableIndexTestFixture build()
    {
      if (interval == null) {
        throw new IllegalStateException("interval is required");
      }
      if (clusteringColumns == null || clusteringColumns.size() == 0) {
        throw new IllegalStateException("clusteringColumns is required");
      }
      if (groups.isEmpty()) {
        throw new IllegalStateException("at least one group is required");
      }
      for (int i = 0; i < groups.size(); i++) {
        if (groups.get(i).clusteringValues.size() != clusteringColumns.size()) {
          throw new IllegalStateException(
              "group " + i + " clusteringValues size [" + groups.get(i).clusteringValues.size()
              + "] does not match clusteringColumns size [" + clusteringColumns.size() + "]"
          );
        }
      }

      final Closer closer = Closer.create();

      // Build per-group QueryableIndex instances via IndexBuilder, one tmpDir per group.
      final File rootTmpDir = FileUtils.createTempDir("clusteredFixture");
      closer.register(() -> FileUtils.deleteDirectory(rootTmpDir));

      final List<QueryableIndex> perGroupIndexes = new ArrayList<>(groups.size());
      for (int i = 0; i < groups.size(); i++) {
        final File groupTmpDir = new File(rootTmpDir, "group" + i);
        try {
          FileUtils.mkdirp(groupTmpDir);
        }
        catch (IOException e) {
          throw new IllegalStateException("could not create " + groupTmpDir, e);
        }
        final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
            .withDimensionsSpec(dimensionsSpec)
            .withMetrics(metrics)
            .withRollup(rollup)
            .withMinTimestamp(interval.getStartMillis())
            .build();
        final QueryableIndex groupIndex = IndexBuilder.create()
            .useV10()
            .tmpDir(groupTmpDir)
            .schema(schema)
            .rows(groups.get(i).rows)
            .buildMMappedIndex(interval);
        closer.register(groupIndex);
        perGroupIndexes.add(groupIndex);
      }

      // Build summary + specs via the existing test helper.
      final List<List<?>> tuples = new ArrayList<>(groups.size());
      for (GroupDescriptor g : groups) {
        tuples.add(g.clusteringValues);
      }
      final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
          clusteringColumns,
          tuples
      );

      // The summary's "columnNames" is the full set of base-table columns including clustering ones; we mirror the
      // physical-side convention: clustering names first, then __time, then non-clustering dims, then metrics.
      final List<String> summaryColumnNames = new ArrayList<>();
      summaryColumnNames.addAll(clusteringColumns.getColumnNames());
      summaryColumnNames.add(ColumnHolder.TIME_COLUMN_NAME);
      for (DimensionSchema d : dimensionsSpec.getDimensions()) {
        summaryColumnNames.add(d.getName());
      }
      for (AggregatorFactory m : metrics) {
        summaryColumnNames.add(m.getName());
      }

      // Default ordering: clustering columns ascending, then __time ascending. Tests can construct their own
      // summary downstream if they need different ordering, but the common case is covered.
      final List<OrderBy> ordering = new ArrayList<>();
      for (String c : clusteringColumns.getColumnNames()) {
        ordering.add(OrderBy.ascending(c));
      }
      ordering.add(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME));

      final ClusteredValueGroupsBaseTableSchema summary = new ClusteredValueGroupsBaseTableSchema(
          VirtualColumns.EMPTY,
          summaryColumnNames,
          metrics,
          ordering,
          clusteringColumns,
          null,
          built.dictionaries(),
          built.specs()
      );

      // Outer SimpleQueryableIndex subclass: top-level columns empty (clustered convention), getMetadata returns
      // the summary-derived metadata, and getClusterGroupQueryableIndex dispatches to the IndexBuilder-built indexes.
      final List<TableClusterGroupSpec> specs = summary.getClusterGroups();
      final Metadata segmentMetadata = summary.asMetadata(null);
      final QueryableIndex segmentIndex = new SimpleQueryableIndex(
          interval,
          new ListIndexed<>(List.of()),
          new RoaringBitmapFactory(),
          Map.of(),
          null,
          segmentMetadata,
          Map.of(),
          summary,
          buildEmptyClusterGroupColumns(specs.size())
      )
      {
        @Override
        public Metadata getMetadata()
        {
          return segmentMetadata;
        }

        @Override
        public int getNumRows()
        {
          int total = 0;
          for (QueryableIndex idx : perGroupIndexes) {
            total += idx.getNumRows();
          }
          return total;
        }

        @Override
        public QueryableIndex getClusterGroupQueryableIndex(TableClusterGroupSpec groupSpec)
        {
          final int idx = specs.indexOf(groupSpec);
          if (idx < 0) {
            throw new IllegalArgumentException("Cluster group spec is not part of this fixture");
          }
          return perGroupIndexes.get(idx);
        }
      };
      closer.register(segmentIndex);

      return new ClusteredQueryableIndexTestFixture(segmentIndex, summary, closer);
    }

    private static List<Map<String, Supplier<BaseColumnHolder>>> buildEmptyClusterGroupColumns(int n)
    {
      final List<Map<String, Supplier<BaseColumnHolder>>> out = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        out.add(Collections.emptyMap());
      }
      return out;
    }
  }

  private record GroupDescriptor(List<?> clusteringValues, List<InputRow> rows) {}
}
